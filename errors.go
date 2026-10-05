package dtq

import "errors"

var (
	// ErrClosed 表示队列已关闭，不再接受投递或领取。
	ErrClosed = errors.New("dtq: queue is closed")
	// ErrUnknownLease 表示确认/失败回执对应的租约不存在：
	// 可能任务已超时重投、已被确认，或任务 ID / 投递代次不匹配。
	ErrUnknownLease = errors.New("dtq: unknown or expired lease")
	// ErrUnknownVersion 表示磁盘文件是当前代码无法识别的更高格式版本。
	ErrUnknownVersion = errors.New("dtq: unknown data format version")

	errCorruptWAL = errors.New("dtq: corrupt or truncated wal record")
)
