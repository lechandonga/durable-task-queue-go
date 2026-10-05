package dtq

import (
	"context"
	"testing"
	"time"
)

// TestCompaction：压缩把已确认任务物理清除、WAL 重写为单个快照帧；
// 活跃任务（含在途）与死信在压缩后及重启后状态不变、可继续流转。
func TestCompaction(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.LeaseTimeout = time.Hour
	})
	q := mustOpen(t, cfg)

	// 先制造一条死信。
	deadID, _ := q.Enqueue(context.Background(), []byte("dead-payload"))
	ld, err := q.Dequeue(context.Background())
	if err != nil || ld.TaskID != deadID {
		t.Fatalf("死信任务投递异常: %v", err)
	}
	if err := q.Nack(ld, Failure{Retryable: false, Reason: "compact-dead"}); err != nil {
		t.Fatal(err)
	}

	// 投递 20 个任务，前 19 个确认，最后一个保持在途。
	var keep string
	for i := 0; i < 20; i++ {
		id, _ := q.Enqueue(context.Background(), []byte("compact-payload"))
		if i == 19 {
			keep = id
		}
		l, err := q.Dequeue(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if l.TaskID == keep {
			continue // 最后一个保持在途，验证快照保留 leased 状态
		}
		if err := q.Ack(l); err != nil {
			t.Fatal(err)
		}
	}

	if err := q.forceCompact(); err != nil {
		t.Fatalf("压缩失败: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q2 := mustOpen(t, cfg)
	rec := q2.LastRecovery()
	t.Logf("压缩后重启恢复: 重放=%d(=1 快照帧 + 关闭释放) ready=%d inFlight=%d dead=%d",
		rec.RecordsReplayed, rec.Ready, rec.InFlight, rec.DeadLetters)
	if rec.RecordsReplayed != 2 {
		t.Fatalf("压缩+正常关闭后期望 2 条记录（快照+leaseRelease），实际 %d 条", rec.RecordsReplayed)
	}
	if rec.Ready != 1 || rec.InFlight != 0 || rec.Delayed != 0 || rec.DeadLetters != 1 {
		t.Fatalf("压缩后状态异常: 活跃任务应为 1，实际 ready=%d inFlight=%d delayed=%d dead=%d",
			rec.Ready, rec.InFlight, rec.Delayed, rec.DeadLetters)
	}
	if dls := q2.DeadLetters(); len(dls) != 1 || dls[0].Reason != "compact-dead" {
		t.Fatalf("压缩后死信丢失或异常: %+v", dls)
	}
	t.Log("断言通过: 压缩清除已确认记录；正常关闭后在途任务回到 ready，死信完整保留")
}
