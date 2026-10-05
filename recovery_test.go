package dtq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestGracefulCloseRecovery：正常关闭时在途任务回到可投递状态，
// 重启后立刻能被新消费者拿到；已确认的任务不会再冒出来。
func TestGracefulCloseRecovery(t *testing.T) {
	cfg := testConfig(t)
	q := mustOpen(t, cfg)

	id1, _ := q.Enqueue(context.Background(), []byte("in-flight-1"))
	id2, _ := q.Enqueue(context.Background(), []byte("in-flight-2"))
	id3, _ := q.Enqueue(context.Background(), []byte("acked"))
	id4, _ := q.Enqueue(context.Background(), []byte("pending"))

	// 按 FIFO 依次领取前三个，第三个确认，前两个保持在途；id4 待投递。
	var l Lease
	for i, want := range []string{id1, id2, id3} {
		var err error
		l, err = q.Dequeue(context.Background())
		if err != nil || l.TaskID != want {
			t.Fatalf("第 %d 次领取异常: got=%s want=%s err=%v", i+1, l.TaskID, want, err)
		}
	}
	if err := q.Ack(l); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("优雅关闭失败: %v", err)
	}

	q2 := mustOpen(t, cfg)
	rec := q2.LastRecovery()
	t.Logf("重启恢复报告: 重放=%d 截断=%d ready=%d inFlight=%d delayed=%d dead=%d",
		rec.RecordsReplayed, rec.TruncatedBytes, rec.Ready, rec.InFlight, rec.Delayed, rec.DeadLetters)
	if rec.Ready != 3 || rec.InFlight != 0 {
		t.Fatalf("业务断言失败: 优雅关闭后期望 ready=3/inFlight=0，实际 ready=%d inFlight=%d",
			rec.Ready, rec.InFlight)
	}

	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		l, err := q2.Dequeue(context.Background())
		if err != nil {
			t.Fatalf("重启后投递失败: %v", err)
		}
		got[l.TaskID] = true
		if err := q2.Ack(l); err != nil {
			t.Fatal(err)
		}
	}
	if !got[id1] || !got[id2] || !got[id4] || got[id3] {
		t.Fatalf("业务断言失败: 两个在途+待投递必须回来、已确认不能冒出来: %v", got)
	}
	t.Log("断言通过: 正常关闭后在途任务与待投递任务均可再投递，已确认任务消失")
}

// TestCrashRecovery：模拟 kill -9（在途任务没有 release 记录），
// 重启后：未确认任务按落盘的 leaseExpire 判定，已过期的立即重新可投；
// 未到期的仍保持在途，到期后也能再投；已确认任务永不出现。
func TestCrashRecovery(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.LeaseTimeout = 200 * time.Millisecond
		c.InitialBackoff = 5 * time.Millisecond
	})
	q := mustOpen(t, cfg)

	idInflight, _ := q.Enqueue(context.Background(), []byte("in-flight-1"))
	idInflight2, _ := q.Enqueue(context.Background(), []byte("in-flight-2"))
	idAcked, _ := q.Enqueue(context.Background(), []byte("acked"))
	idPending, _ := q.Enqueue(context.Background(), []byte("pending"))

	// 按 FIFO 领取前三个；确认第三个，前两个保持在途（将随崩溃过期）。
	var l1 Lease
	for _, want := range []string{idInflight, idInflight2, idAcked} {
		var err error
		l1, err = q.Dequeue(context.Background())
		if err != nil || l1.TaskID != want {
			t.Fatalf("领取异常 got=%s want=%s err=%v", l1.TaskID, want, err)
		}
	}
	if err := q.Ack(l1); err != nil {
		t.Fatal(err)
	}

	// 睡过租约超时点再“崩溃”，恢复时应立即判定超时。
	time.Sleep(250 * time.Millisecond)
	q.crash()

	q2 := mustOpen(t, cfg)
	rec := q2.LastRecovery()
	t.Logf("崩溃恢复报告: 重放=%d 截断=%d ready=%d inFlight=%d delayed=%d dead=%d",
		rec.RecordsReplayed, rec.TruncatedBytes, rec.Ready, rec.InFlight, rec.Delayed, rec.DeadLetters)
	if rec.TruncatedBytes != 0 {
		t.Fatalf("正常 fsync 的崩溃不应有截断，实际截断 %d 字节", rec.TruncatedBytes)
	}

	got := map[string]bool{}
	// idPending 立即可投；两个在途任务经过一次退避后也会回来。
	deadline := time.Now().Add(3 * time.Second)
	for len(got) < 3 && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		l, err := q2.Dequeue(ctx)
		cancel()
		if err != nil {
			continue
		}
		got[l.TaskID] = true
		_ = q2.Ack(l)
	}
	if !got[idPending] || !got[idInflight] || !got[idInflight2] {
		t.Fatalf("业务断言失败: 崩溃后待投递/超时在途任务都必须回来: %v", got)
	}

	// 已确认任务在任何时候都不能再冒出来。
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	for {
		l, err := q2.Dequeue(ctx)
		if err != nil {
			break
		}
		if l.TaskID == idAcked {
			t.Fatal("业务断言失败: 已确认任务在崩溃恢复后再次出现")
		}
	}
	t.Log("断言通过: kill -9 后未确认与待投递任务不丢、已确认任务不复活")
}

// TestTruncatedTailRecovery：在 WAL 末尾手工制造“写了一半”的残缺帧，
// 重启必须能识别、截断并报告，且此前完整数据完整可用。
func TestTruncatedTailRecovery(t *testing.T) {
	cfg := testConfig(t)
	q := mustOpen(t, cfg)
	idOK, _ := q.Enqueue(context.Background(), []byte("good"))
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(cfg.Dir, "queue.log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 追加一段长度声明为 1000 但只写了 7 字节的垃圾帧。
	garbage := []byte{0xe8, 0x03, 0x00, 0x00, 'x', 'y', 'z', 0, 0, 0, 0}
	if err := os.WriteFile(path, append(data, garbage...), 0o644); err != nil {
		t.Fatal(err)
	}

	q2 := mustOpen(t, cfg)
	rec := q2.LastRecovery()
	if rec.TruncatedBytes != int64(len(garbage)) {
		t.Fatalf("业务断言失败: 期望报告截断 %d 字节，实际 %d", len(garbage), rec.TruncatedBytes)
	}
	l, err := q2.Dequeue(context.Background())
	if err != nil {
		t.Fatalf("截断恢复后任务不可用: %v", err)
	}
	if l.TaskID != idOK {
		t.Fatalf("恢复出的任务不对: %s", l.TaskID)
	}
	t.Logf("断言通过: 残缺帧被识别并截断 %d 字节，此前 %d 条完整数据可用",
		rec.TruncatedBytes, rec.Ready)
}

// TestCrcCorruptionRecovery：帧内容被翻转导致 CRC 不匹配，
// 该帧及其后内容必须被隔离截断，绝不能读出脏数据。
func TestCrcCorruptionRecovery(t *testing.T) {
	cfg := testConfig(t)
	q := mustOpen(t, cfg)
	_, _ = q.Enqueue(context.Background(), []byte("before"))
	idAfter, _ := q.Enqueue(context.Background(), []byte("after"))
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(cfg.Dir, "queue.log")
	data, _ := os.ReadFile(path)
	// 找到第二条 enqueue 记录的 payload（跳过文件头与第一帧）。
	firstLen := int(uint32(data[headerSize]) | uint32(data[headerSize+1])<<8 |
		uint32(data[headerSize+2])<<16 | uint32(data[headerSize+3])<<24)
	secondFrame := headerSize + 4 + firstLen + 4
	if data[secondFrame+4] == 0 {
		data[secondFrame+4] = 1
	} else {
		data[secondFrame+4] = 0
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	q2 := mustOpen(t, cfg)
	rec := q2.LastRecovery()
	if rec.TruncatedBytes <= 0 {
		t.Fatal("业务断言失败: CRC 损坏必须被识别并截断")
	}
	// “after” 任务所在帧已损坏被截断，恢复出来的只有 “before”。
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	l, err := q2.Dequeue(ctx)
	if err != nil {
		t.Fatalf("损坏点之前的任务应可用: %v", err)
	}
	if err := q2.Ack(l); err != nil { // 防止租约超时后被重新投递
		t.Fatal(err)
	}
	if l.TaskID == idAfter {
		t.Fatal("业务断言失败: 损坏帧中的任务不应被读出")
	}
	if _, err := q2.Dequeue(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("损坏点之后不应再读出任务: %v", err)
	}
	t.Logf("断言通过: CRC 损坏帧被隔离截断（%d 字节），未读出脏数据", rec.TruncatedBytes)
}

// TestUnknownVersionRejected：文件头声明一个更高的未知版本时，
// Open 必须显式返回 ErrUnknownVersion，且原文件字节不被修改。
func TestUnknownVersionRejected(t *testing.T) {
	cfg := testConfig(t)
	q := mustOpen(t, cfg)
	_, _ = q.Enqueue(context.Background(), []byte("v1-data"))
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(cfg.Dir, "queue.log")
	before, _ := os.ReadFile(path)
	modified := make([]byte, len(before))
	copy(modified, before)
	modified[3] = '9'
	modified[4] = 9 // 版本号 9，当前实现不认识
	modified[5] = 0
	if err := os.WriteFile(path, modified, 0o644); err != nil {
		t.Fatal(err)
	}

	q2, err := Open(cfg)
	if !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("业务断言失败: 未知版本必须拒绝，实际 err=%v q=%v", err, q2)
	}
	after, rerr := os.ReadFile(path)
	if rerr != nil || string(after) != string(modified) {
		t.Fatal("业务断言失败: 拒绝未知版本时不得修改原有数据")
	}
	t.Log("断言通过: 未知格式版本被显式拒绝（ErrUnknownVersion），原文件未改动")
}

// TestDelayedRetryBackoff：验证退避节奏——Nack 后在退避窗口内拿不到，
// 窗口结束后才能再次投递。
func TestDelayedRetryBackoff(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.InitialBackoff = 120 * time.Millisecond
		c.MaxBackoff = time.Second
	})
	q := mustOpen(t, cfg)
	enqueueN(t, q, 1)

	l, _ := q.Dequeue(context.Background())
	if err := q.Nack(l, Failure{Retryable: true, Reason: "slow"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("退避窗口内不应投递: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	l2, err := q.Dequeue(ctx2)
	if err != nil {
		t.Fatalf("退避结束后应重新投递: %v", err)
	}
	if l2.Delivery != 2 {
		t.Fatalf("代次应为 2，实际 %d", l2.Delivery)
	}
	t.Log("断言通过: 可重试失败严格按退避节奏重新投递")
}
