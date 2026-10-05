package dtq

import (
	"container/heap"
	"time"
)

// loop 是唯一执行“租约发放 / 超时扫描 / 退避到期 / 压缩”的 goroutine。
func (q *Queue) loop() {
	defer close(q.doneCh)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return
		}
		now := q.nowNano()
		q.processExpirationsLocked(now)
		q.assignLocked(now)

		wait := time.Hour
		if len(q.pq) > 0 && q.pq[0].status != statusReady {
			d := time.Duration(q.pq[0].due - now)
			if d < 0 {
				d = 0
			}
			wait = d + time.Millisecond
		}
		needCompact := !q.compacting && q.walSize >= q.cfg.CompactionThreshold
		q.mu.Unlock()

		if needCompact {
			if err := q.compact(); err == nil {
				q.signal()
			}
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)

		select {
		case <-q.wake:
		case <-timer.C:
		case <-q.closeCh:
			return
		}
	}
}

// processExpirationsLocked 处理租约超时与退避到期。
func (q *Queue) processExpirationsLocked(now int64) {
	for len(q.pq) > 0 {
		head := q.pq[0]
		if head.status == statusReady || head.due > now {
			return
		}
		t := heap.Pop(&q.pq).(*taskState)
		switch t.status {
		case statusLeased:
			// 消费者一直没有回执，这算一次投递尝试。
			if t.attempts >= q.cfg.MaxAttempts {
				if err := q.recordDeadLocked(t, "lease timed out: consumer did not acknowledge", now); err != nil {
					// 死信落盘失败：保留在堆里下次再试，绝不静默丢任务。
					t.status = statusLeased
					t.due = now + int64(q.cfg.LeaseTimeout)
					heap.Push(&q.pq, t)
					return
				}
				continue
			}
			t.status = statusDelayed
			t.due = now + q.backoffNano(t.attempts)
			// 超时重投不写 WAL：leaseExpire 早已落盘，恢复时依据
			// 同一个到期时刻会得到完全相同的结果。
		case statusDelayed:
			t.status = statusReady
			t.due = 0
		default:
			continue
		}
		heap.Push(&q.pq, t)
	}
}

// assignLocked 把可投递任务分配给等待的消费者。每次发放都先把
// 新一代 leased 记录 fsync 落盘，成功后才把租约交给消费者；
// 因此任一时刻同一任务最多只有一个有效代次被持有。
func (q *Queue) assignLocked(now int64) {
	for len(q.waiters) > 0 && len(q.pq) > 0 {
		idx := -1
		for i, t := range q.pq {
			if t.status == statusReady {
				idx = i
				break
			}
		}
		if idx < 0 {
			return
		}
		t := heap.Remove(&q.pq, idx).(*taskState)

		t.delivery++
		t.attempts = int(t.delivery)
		t.status = statusLeased
		t.timeoutMS = q.cfg.LeaseTimeout.Milliseconds()
		t.leaseExpire = now + int64(q.cfg.LeaseTimeout)
		t.due = t.leaseExpire

		rec := encodeLeased(leasedRec{
			id: t.id, delivery: t.delivery, attempts: t.attempts,
			leaseExpire: t.leaseExpire, timeoutMS: t.timeoutMS,
		})
		if err := q.wal.appendRecord(rec); err != nil {
			// 发放失败：任务放回 ready，本次不消耗投递次数。
			t.delivery--
			t.attempts = int(t.delivery)
			t.status = statusReady
			t.due = 0
			t.leaseExpire = 0
			heap.Push(&q.pq, t)
			return
		}
		q.advanceWalSizeLocked(int64(len(frame(rec))))
		heap.Push(&q.pq, t)

		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w.ch <- Lease{
			TaskID:   t.id,
			Delivery: t.delivery,
			Task:     t.snapshotTask(),
		}
	}
}

// recordDeadLocked 写死信记录并从活跃集合移除。
func (q *Queue) recordDeadLocked(t *taskState, reason string, now int64) error {
	rec := encodeDead(deadRec{
		id: t.id, attempts: t.attempts, reason: reason,
		enqueuedAt: t.enqueuedAt, deadAt: now,
	})
	if err := q.wal.appendRecord(rec); err != nil {
		return err
	}
	q.advanceWalSizeLocked(int64(len(frame(rec))))
	dl := DeadLetter{Task: t.snapshotTask(), Reason: reason, DeadAt: time.Unix(0, now)}
	dl.Attempts = t.attempts
	q.dead = append(q.dead, dl)
	t.status = statusDead
	q.removeFromHeapLocked(t)
	delete(q.tasks, t.id)
	return nil
}

// Close 优雅关闭：停止调度器，把所有在途任务写成 leaseRelease
// （fsync），使下次启动时它们直接回到可投递状态。关闭后
// Enqueue/Dequeue/Ack/Nack 返回 ErrClosed。多次调用安全。
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		<-q.doneCh
		return nil
	}
	q.closed = true

	var firstErr error
	for _, t := range q.tasks {
		if t.status != statusLeased {
			continue
		}
		rec := encodeLeaseRelease(leaseReleaseRec{id: t.id, delivery: t.delivery})
		if err := q.wal.appendRecord(rec); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		q.advanceWalSizeLocked(int64(len(frame(rec))))
		t.status = statusReady
		t.due = 0
		t.leaseExpire = 0
	}
	q.waiters = nil
	q.mu.Unlock()

	close(q.closeCh)
	<-q.doneCh
	if err := q.wal.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
