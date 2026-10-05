package dtq

import (
	"container/heap"
	"time"
)

// replay 从 WAL 重建内存状态。
//
//   - recSnapshot 之后的内容以快照为准（快照之前的记录全部丢弃）；
//   - 崩溃时在途（leased）的任务保持 leased，按落盘的 leaseExpire
//     判定：启动时已过期则立即走超时流程（重投或死信）；
//   - 正常关闭写出的 leaseRelease 让在途任务立即回到 ready；
//   - 第一个 CRC/长度不合法的帧就是崩溃截断点，其后全部截断，
//     截断字节数通过 RecoveryInfo.TruncatedBytes 报告；
//   - 文件头版本不认识时在 openWAL 阶段就显式拒绝，不动任何数据。
func (q *Queue) replay() error {
	var records [][]byte
	validEnd, scanErr := q.wal.scan(func(payload []byte) error {
		if len(payload) > 0 && payload[0] == recSnapshot {
			// 只保留最后一个快照及其后的记录。
			records = records[:0]
		}
		records = append(records, append([]byte(nil), payload...))
		return nil
	})

	fi, _ := q.wal.f.Stat()
	size := fi.Size()
	if scanErr != nil {
		if cut := size - validEnd; cut > 0 {
			if err := q.wal.truncateTail(validEnd); err != nil {
				return err
			}
			q.recovery.TruncatedBytes = cut
		}
	}

	for _, p := range records {
		if len(p) == 0 {
			return errCorruptWAL
		}
		if p[0] == recSnapshot {
			s, ok := decodeSnapshot(p)
			if !ok {
				return errCorruptWAL
			}
			q.loadSnapshotLocked(s)
			q.recovery.RecordsReplayed++
			continue
		}
		if err := q.applyRecordLocked(p); err != nil {
			return err
		}
		q.recovery.RecordsReplayed++
	}

	// 恢复时把“崩溃前就已超时”的在途任务立即处理掉。
	now := q.nowNano()
	for _, t := range q.tasks {
		if t.status == statusLeased && t.due <= now {
			// 崩溃恢复中的在途任务：代次归零、重新按 ready 投递。
			// 这样既保证未确认任务不丢，也不会因为“恢复”这个动作
			// 凭空消耗一次投递尝试而把任务直接判进死信。
			t.status = statusReady
			t.due = 0
			t.leaseExpire = 0
			t.delivery = 0
			t.attempts = 0
			if t.heapIdx >= 0 {
				heap.Fix(&q.pq, t.heapIdx)
			}
		}
	}
	q.processExpirationsLocked(now)

	for _, t := range q.tasks {
		switch t.status {
		case statusReady:
			q.recovery.Ready++
		case statusLeased:
			q.recovery.InFlight++
		case statusDelayed:
			q.recovery.Delayed++
		}
	}
	q.recovery.DeadLetters = len(q.dead)
	q.walSize = validEnd
	return nil
}

func (q *Queue) loadSnapshotLocked(s snapshotData) {
	q.tasks = map[string]*taskState{}
	q.pq = nil
	q.dead = nil
	for _, t := range s.tasks {
		t.heapIdx = -1
		q.tasks[t.id] = t
		if t.status != statusDone && t.status != statusDead {
			heap.Push(&q.pq, t)
		}
	}
	q.dead = append(q.dead, s.dead...)
}

func (q *Queue) applyRecordLocked(p []byte) error {
	switch p[0] {
	case recEnqueue:
		r, ok := decodeEnqueue(p)
		if !ok {
			return errCorruptWAL
		}
		if _, exists := q.tasks[r.id]; exists {
			return nil
		}
		t := &taskState{
			id: r.id, payload: append([]byte(nil), r.payload...),
			enqueuedAt: r.enqueuedAt, status: statusReady, heapIdx: -1,
		}
		q.tasks[t.id] = t
		heap.Push(&q.pq, t)

	case recLeased:
		r, ok := decodeLeased(p)
		if !ok {
			return errCorruptWAL
		}
		t := q.tasks[r.id]
		if t == nil {
			// leased 引用了不存在的任务，说明日志损坏。
			return errCorruptWAL
		}
		t.status = statusLeased
		t.delivery = r.delivery
		t.attempts = r.attempts
		t.leaseExpire = r.leaseExpire
		t.timeoutMS = r.timeoutMS
		t.due = r.leaseExpire
		if t.heapIdx < 0 {
			heap.Push(&q.pq, t)
		} else {
			heap.Fix(&q.pq, t.heapIdx)
		}

	case recAcked:
		r, ok := decodeAcked(p)
		if !ok {
			return errCorruptWAL
		}
		// 只要任务在该代次被确认过就永久消失：
		// 即使后面还有同任务的旧记录，已确认的任务也不能再冒出来。
		if t, exists := q.tasks[r.id]; exists {
			if t.delivery > r.delivery {
				return errCorruptWAL
			}
			q.removeFromHeapLocked(t)
			delete(q.tasks, r.id)
		}

	case recNacked:
		r, ok := decodeNacked(p)
		if !ok {
			return errCorruptWAL
		}
		t := q.tasks[r.id]
		if t == nil || t.delivery != r.delivery {
			return nil // 任务已终结，或迟到的旧代次回执，忽略
		}
		if !r.retryable {
			// 正常情况下紧跟一条 dead 记录；若该帧恰好在写入
			// 间隙被截断丢失，仍须遵守“不可重试不反复重试”的语义。
			q.removeFromHeapLocked(t)
			delete(q.tasks, r.id)
			q.dead = append(q.dead, DeadLetter{
				Task:     t.snapshotTask(),
				Attempts: r.attempts,
				Reason:   r.reason,
				DeadAt:   time.Unix(0, r.at),
			})
			return nil
		}
		t.status = statusDelayed
		t.due = r.at + q.backoffNano(r.attempts)
		if t.heapIdx >= 0 {
			heap.Fix(&q.pq, t.heapIdx)
		}

	case recDead:
		r, ok := decodeDead(p)
		if !ok {
			return errCorruptWAL
		}
		t := q.tasks[r.id]
		var payload []byte
		var enqueuedAt int64
		if t != nil {
			payload = append([]byte(nil), t.payload...)
			enqueuedAt = t.enqueuedAt
			q.removeFromHeapLocked(t)
			delete(q.tasks, r.id)
		} else {
			// 有效日志里任务一定能从快照/此前的 enqueue 找到；
			// 找不到说明序列异常，按损坏处理。
			return errCorruptWAL
		}
		q.dead = append(q.dead, DeadLetter{
			Task: Task{
				ID:         r.id,
				Payload:    payload,
				EnqueuedAt: time.Unix(0, enqueuedAt),
				Attempts:   r.attempts,
			},
			Attempts: r.attempts,
			Reason:   r.reason,
			DeadAt:   time.Unix(0, r.deadAt),
		})

	case recLeaseRelease:
		r, ok := decodeLeaseRelease(p)
		if !ok {
			return errCorruptWAL
		}
		t := q.tasks[r.id]
		// 仅当它还停留在被释放的这一代次时才放回，
		// 避免覆盖更新一代的租约。
		if t != nil && t.status == statusLeased && t.delivery == r.delivery {
			t.status = statusReady
			t.due = 0
			t.leaseExpire = 0
			if t.heapIdx >= 0 {
				heap.Fix(&q.pq, t.heapIdx)
			}
		}

	default:
		// 已知版本内出现未知记录类型：可能是同版本的新字段，
		// 为安全起见按损坏处理并停止（不截断，由人工排查）。
		return errCorruptWAL
	}
	return nil
}

// compact 把当前所有存活状态写成一帧快照，原子替换 WAL。
// 已确认任务就此清除；替换是 “写临时文件 + fsync + rename”，
// 崩溃在任何一步发生都至少保留旧文件或新文件中的一个完整版本。
func (q *Queue) compact() error {
	q.mu.Lock()
	q.compacting = true
	s := snapshotData{at: q.nowNano()}
	for _, t := range q.tasks {
		if t.status == statusReady || t.status == statusLeased || t.status == statusDelayed {
			s.tasks = append(s.tasks, t)
		}
	}
	s.dead = append(s.dead, q.dead...)
	rec := encodeSnapshot(s)
	full := append(fileHeader(), frame(rec)...)
	q.mu.Unlock()

	if err := q.wal.rewrite(full); err != nil {
		q.mu.Lock()
		q.compacting = false
		q.mu.Unlock()
		return err
	}
	q.mu.Lock()
	q.walSize = int64(len(full))
	q.compacting = false
	q.mu.Unlock()
	return nil
}
