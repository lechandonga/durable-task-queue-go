package dtq

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Config 是队列配置。零值字段会在 Open 时填充默认值。
type Config struct {
	// Dir 是队列数据目录，不存在会自动创建。
	Dir string
	// MaxAttempts 是单个任务的最大投递次数；用尽后进入死信。默认 5。
	MaxAttempts int
	// LeaseTimeout 是投递后等待消费者确认的时间；超时自动重投。默认 5s。
	LeaseTimeout time.Duration
	// InitialBackoff 是第一次失败后的退避时间。默认 100ms。
	InitialBackoff time.Duration
	// MaxBackoff 是退避上限。默认 30s。
	MaxBackoff time.Duration
	// BackoffFactor 是退避乘数。默认 2。
	BackoffFactor float64
	// CompactionThreshold 是 WAL 超过该大小后触发一次压缩。默认 1MiB。
	CompactionThreshold int64
	// Now 用于注入时钟（测试用）；为 nil 时使用 time.Now。
	Now func() time.Time
}

func (c *Config) withDefaults() {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.LeaseTimeout <= 0 {
		c.LeaseTimeout = 5 * time.Second
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = 100 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.BackoffFactor <= 1 {
		c.BackoffFactor = 2
	}
	if c.CompactionThreshold <= 0 {
		c.CompactionThreshold = 1 << 20
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Stats 是队列计数快照。
type Stats struct {
	Ready    int
	InFlight int
	Delayed  int
	Dead     int
}

// RecoveryInfo 描述本次启动时从磁盘恢复出的状态。
type RecoveryInfo struct {
	// RecordsReplayed 是重放的逻辑记录数（快照内联记录计入）。
	RecordsReplayed int
	// TruncatedBytes > 0 表示文件末尾发现崩溃留下的残缺帧并已截断。
	TruncatedBytes int64
	Ready          int
	InFlight       int
	Delayed        int
	DeadLetters    int
}

// Queue 是并发安全的本地持久化任务队列。
type Queue struct {
	cfg Config
	wal *walFile

	mu     sync.Mutex
	tasks  map[string]*taskState
	pq     dueHeap
	dead   []DeadLetter
	closed bool

	waiters []*leaseRequest

	compacting bool

	wake    chan struct{}
	closeCh chan struct{}
	doneCh  chan struct{}

	recovery RecoveryInfo
	walSize  int64
}

type leaseRequest struct {
	ch chan Lease
}

// Open 打开（或新建）队列并立即完成崩溃恢复。
func Open(cfg Config) (*Queue, error) {
	cfg.withDefaults()
	w, err := openWAL(cfg.Dir)
	if err != nil {
		return nil, err
	}
	q := &Queue{
		cfg:     cfg,
		wal:     w,
		tasks:   map[string]*taskState{},
		wake:    make(chan struct{}, 1),
		closeCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	if err := q.replay(); err != nil {
		w.close()
		return nil, err
	}
	go q.loop()
	return q, nil
}

// LastRecovery 返回本次启动的恢复报告。
func (q *Queue) LastRecovery() RecoveryInfo {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.recovery
}

func (q *Queue) nowNano() int64 { return q.cfg.Now().UnixNano() }

func (q *Queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// Enqueue 投递新任务，返回任务 ID。落盘 fsync 成功后才返回。
func (q *Queue) Enqueue(ctx context.Context, payload []byte) (string, error) {
	id := newID()
	now := q.nowNano()
	payloadCopy := append([]byte(nil), payload...)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return "", ErrClosed
	}
	rec := encodeEnqueue(enqueueRec{
		id: id, payload: payloadCopy, enqueuedAt: now,
	})
	if err := q.wal.appendRecord(rec); err != nil {
		return "", err
	}
	t := &taskState{
		id: id, payload: payloadCopy, enqueuedAt: now,
		status: statusReady, heapIdx: -1,
	}
	q.tasks[id] = t
	heap.Push(&q.pq, t)
	q.advanceWalSizeLocked(int64(len(frame(rec))))
	return id, nil
}

// Dequeue 领取任务。无任务可投时阻塞等待，可被 ctx 取消。
func (q *Queue) Dequeue(ctx context.Context) (Lease, error) {
	req := &leaseRequest{ch: make(chan Lease, 1)}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return Lease{}, ErrClosed
	}
	q.waiters = append(q.waiters, req)
	q.mu.Unlock()
	q.signal()

	select {
	case l := <-req.ch:
		return l, nil
	case <-ctx.Done():
		q.mu.Lock()
		for i, w := range q.waiters {
			if w == req {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		q.mu.Unlock()
		return Lease{}, ctx.Err()
	case <-q.closeCh:
		return Lease{}, ErrClosed
	}
}

// Ack 确认任务完成。租约不存在（已超时、已确认、代次不匹配）返回
// ErrUnknownLease；这是正常的竞争结果，不是数据损坏。
func (q *Queue) Ack(l Lease) error { return q.complete(l, nil) }

// Nack 上报处理失败。Retryable=false 直接进死信；Retryable=true 但
// 投递次数已用尽同样进死信，其余按指数退避重新投递。
func (q *Queue) Nack(l Lease, f Failure) error { return q.complete(l, &f) }

func (q *Queue) complete(l Lease, f *Failure) error {
	now := q.nowNano()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	t, ok := q.tasks[l.TaskID]
	// 唯一确认：必须同时匹配任务、租约代次和“持有中”状态。
	// 超时重投会令 delivery 加 1，迟到确认必然走到这里失败。
	if !ok || t.status != statusLeased || t.delivery != l.Delivery {
		return ErrUnknownLease
	}

	switch {
	case f == nil:
		rec := encodeAcked(ackedRec{id: t.id, delivery: t.delivery, at: now})
		if err := q.wal.appendRecord(rec); err != nil {
			return err
		}
		q.advanceWalSizeLocked(int64(len(frame(rec))))
		t.status = statusDone
		q.removeFromHeapLocked(t)
		delete(q.tasks, t.id)
	case !f.Retryable || t.attempts >= q.cfg.MaxAttempts:
		rec := encodeDead(deadRec{
			id: t.id, attempts: t.attempts, reason: f.Reason,
			enqueuedAt: t.enqueuedAt, deadAt: now,
		})
		if err := q.wal.appendRecord(rec); err != nil {
			return err
		}
		q.advanceWalSizeLocked(int64(len(frame(rec))))
		dl := DeadLetter{Task: t.snapshotTask(), Reason: f.Reason, DeadAt: time.Unix(0, now)}
		dl.Attempts = t.attempts
		q.dead = append(q.dead, dl)
		t.status = statusDead
		q.removeFromHeapLocked(t)
		delete(q.tasks, t.id)
	default:
		rec := encodeNacked(nackedRec{
			id: t.id, delivery: t.delivery, attempts: t.attempts,
			retryable: true, reason: f.Reason, at: now,
		})
		if err := q.wal.appendRecord(rec); err != nil {
			return err
		}
		q.advanceWalSizeLocked(int64(len(frame(rec))))
		t.status = statusDelayed
		t.due = now + q.backoffNano(t.attempts)
		heap.Fix(&q.pq, t.heapIdx)
	}
	q.signal()
	return nil
}

// DeadLetters 返回死信档案快照（含失败原因、重试/投递次数、入队与入死信时间）。
func (q *Queue) DeadLetters() []DeadLetter {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]DeadLetter, len(q.dead))
	for i, d := range q.dead {
		out[i] = d
		out[i].Payload = append([]byte(nil), d.Payload...)
	}
	return out
}

// Stats 返回队列计数快照。
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	var s Stats
	for _, t := range q.tasks {
		switch t.status {
		case statusReady:
			s.Ready++
		case statusLeased:
			s.InFlight++
		case statusDelayed:
			s.Delayed++
		}
	}
	s.Dead = len(q.dead)
	return s
}

func (q *Queue) backoffNano(used int) int64 {
	d := float64(q.cfg.InitialBackoff)
	for i := 1; i < used; i++ {
		d *= q.cfg.BackoffFactor
		if d >= float64(q.cfg.MaxBackoff) {
			return int64(q.cfg.MaxBackoff)
		}
	}
	if d > float64(q.cfg.MaxBackoff) {
		d = float64(q.cfg.MaxBackoff)
	}
	return int64(d)
}

func (q *Queue) removeFromHeapLocked(t *taskState) {
	if t.heapIdx >= 0 {
		heap.Remove(&q.pq, t.heapIdx)
	}
}

func (q *Queue) advanceWalSizeLocked(frameSize int64) {
	q.walSize += frameSize
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败时无法安全生成 ID，直接 panic（与 uuid 库行为一致）。
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	dst := make([]byte, 36)
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], b[10:])
	return string(dst)
}
