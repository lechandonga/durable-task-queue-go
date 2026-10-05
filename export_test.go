package dtq

import "time"

// crash 模拟进程被 kill -9：立刻停止调度循环并关闭文件句柄，
// 不写在途任务的 leaseRelease。
func (q *Queue) crash() {
	close(q.closeCh)
	<-q.doneCh
	_ = q.wal.close()
	q.closed = true
}

func (q *Queue) dataFilePath() string { return q.wal.path }

func (q *Queue) waitStats(deadline time.Duration, pred func(Stats) bool) bool {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if pred(q.Stats()) {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return pred(q.Stats())
}

func (q *Queue) forceCompact() error { return q.compact() }
