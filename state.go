package dtq

import "time"

// Task 是队列中的一个任务。
type Task struct {
	// ID 为任务唯一标识；由 Enqueue 自动生成，全生命周期不变。
	ID string
	// Payload 是业务负载，对队列不透明。
	Payload []byte
	// EnqueuedAt 为首次入队时间。
	EnqueuedAt time.Time
	// Attempts 为已经发生的投递次数（当前这次投递计入其中）。
	Attempts int
}

// Lease 是一次投递的凭证。消费者处理完后必须用完全相同的
// TaskID + Delivery 调用 Ack/Nack；超时后该凭证立即失效，
// 迟到的回执会得到 ErrUnknownLease。
type Lease struct {
	TaskID   string
	Delivery uint64
	Task     Task
}

// Failure 表示一次失败回执。
type Failure struct {
	// Retryable 为 false 时任务不再重试，直接进入死信。
	Retryable bool
	// Reason 会原样记录到死信中。
	Reason string
}

// DeadLetter 是进入死信的任务的完整档案。
type DeadLetter struct {
	Task
	// Attempts 为总共投递过的次数。
	Attempts int
	// Reason 为最后一次失败的原因；租约耗尽（消费者一直不回执）
	// 时为固定说明。
	Reason string
	// DeadAt 为进入死信的时间。
	DeadAt time.Time
}

// taskState 是内存中的任务状态，按到期时间挂在最小堆里。
type taskState struct {
	id          string
	payload     []byte
	enqueuedAt  int64 // unix nano
	status      byte
	delivery    uint64
	attempts    int
	leaseExpire int64 // unix nano，statusLeased 时有效
	timeoutMS   int64 // 本次租约的超时时间（毫秒）
	due         int64 // unix nano：ready 为 0，leased/delayed 为到期时刻
	heapIdx     int   // 在 dueHeap 中的位置，不在堆中时为 -1
}

const (
	statusReady   byte = 1 // 可投递（含正常关闭后放回的在途任务）
	statusLeased  byte = 2 // 被某个消费者持有
	statusDelayed byte = 3 // 重试退避中
	statusDead    byte = 4 // 已进入死信
	statusDone    byte = 5 // 已确认，压缩后清除
)

func (t *taskState) snapshotTask() Task {
	return Task{
		ID:         t.id,
		Payload:    append([]byte(nil), t.payload...),
		EnqueuedAt: time.Unix(0, t.enqueuedAt),
		Attempts:   t.attempts,
	}
}
