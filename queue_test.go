package dtq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(t *testing.T, opts ...func(*Config)) Config {
	t.Helper()
	cfg := Config{
		Dir:                 t.TempDir(),
		MaxAttempts:         3,
		LeaseTimeout:        60 * time.Millisecond,
		InitialBackoff:      20 * time.Millisecond,
		MaxBackoff:          200 * time.Millisecond,
		BackoffFactor:       2,
		CompactionThreshold: 1 << 30,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func mustOpen(t *testing.T, cfg Config) *Queue {
	t.Helper()
	q, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func enqueueN(t *testing.T, q *Queue, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id, err := q.Enqueue(context.Background(), []byte(fmt.Sprintf("task-%d", i)))
		if err != nil {
			t.Fatalf("Enqueue(%d) 失败: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// TestConcurrentProducersConsumers：多生产者并发投递、多消费者并发领取，
// 断言每个任务恰好被一个消费者处理一次（无重复、无遗漏）。
func TestConcurrentProducersConsumers(t *testing.T) {
	q := mustOpen(t, testConfig(t))
	const producers, perP, consumers = 6, 40, 8
	const total = producers * perP

	var prodWG sync.WaitGroup
	for p := 0; p < producers; p++ {
		prodWG.Add(1)
		go func(p int) {
			defer prodWG.Done()
			for i := 0; i < perP; i++ {
				if _, err := q.Enqueue(context.Background(),
					[]byte(fmt.Sprintf("p%d-t%d", p, i))); err != nil {
					t.Errorf("Enqueue: %v", err)
				}
			}
		}(p)
	}

	var (
		mu       sync.Mutex
		seen     = map[string]int{}
		got      int32
		consWG   sync.WaitGroup
		stopOnce sync.Once
		stopCh   = make(chan struct{})
	)
	for c := 0; c < consumers; c++ {
		consWG.Add(1)
		go func() {
			defer consWG.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				<-stopCh
				cancel()
			}()
			for {
				l, err := q.Dequeue(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				seen[l.TaskID]++
				mu.Unlock()
				if err := q.Ack(l); err != nil {
					t.Errorf("Ack: %v", err)
				}
				if atomic.AddInt32(&got, 1) == int32(total) {
					stopOnce.Do(func() { close(stopCh) })
				}
			}
		}()
	}

	prodWG.Wait()
	select {
	case <-stopCh:
	case <-time.After(15 * time.Second):
	}
	consWG.Wait()

	if atomic.LoadInt32(&got) != total {
		t.Fatalf("业务断言失败: 期望处理 %d 条，实际 %d 条", total, atomic.LoadInt32(&got))
	}
	if len(seen) != total {
		t.Fatalf("业务断言失败: 期望唯一任务 %d 个，实际 %d 个", total, len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("业务断言失败: 任务 %s 被投递了 %d 次", id, n)
		}
	}
	t.Logf("断言通过: %d 个生产者投递 %d 条，%d 个消费者各处理一次，无重复无遗漏",
		producers, total, consumers)
}

// TestLeaseTimeoutRedelivery：消费者领取后不确认，租约超时后必须重新投递；
// 迟到确认失败；已确认任务不会再次出现。
func TestLeaseTimeoutRedelivery(t *testing.T) {
	q := mustOpen(t, testConfig(t))
	ids := enqueueN(t, q, 1)

	ctx := context.Background()
	l1, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if l1.TaskID != ids[0] || l1.Delivery != 1 {
		t.Fatalf("第一次投递异常: %+v", l1)
	}
	t.Log("第一次投递成功，消费者故意不 Ack，等待租约超时")

	dctx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(dctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("租约未到期就被再次投递: err=%v", err)
	}

	l2, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("超时后重新投递失败，任务卡死: %v", err)
	}
	if l2.TaskID != ids[0] || l2.Delivery != 2 {
		t.Fatalf("第二次投递异常: %+v", l2)
	}
	t.Log("断言通过: 租约超时后任务以第 2 代重新投递")

	if err := q.Ack(l1); !errors.Is(err, ErrUnknownLease) {
		t.Fatalf("迟到确认期望 ErrUnknownLease，实际: %v", err)
	}
	t.Log("断言通过: 超时后的迟到 Ack 被拒绝（ErrUnknownLease）")

	if err := q.Ack(l2); err != nil {
		t.Fatalf("新代次 Ack: %v", err)
	}
	if err := q.Ack(l1); !errors.Is(err, ErrUnknownLease) {
		t.Fatalf("确认后重复 Ack 期望 ErrUnknownLease，实际: %v", err)
	}
	dctx2, cancel2 := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel2()
	if _, err := q.Dequeue(dctx2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("已确认任务被再次投递: %v", err)
	}
	t.Log("断言通过: 已确认任务不会被再次执行，重复 Ack 幂等失败")
}

var spin int32

// TestAckAndTimeoutRace：在租约超时边界附近并发 Ack，结果必须唯一——
// 每个任务恰好成功确认一次；超时重投允许发生，但不会有任务被成功处理两次。
func TestAckAndTimeoutRace(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.LeaseTimeout = 15 * time.Millisecond
		c.InitialBackoff = time.Millisecond
		c.MaxBackoff = 5 * time.Millisecond
		c.MaxAttempts = 50
	})
	q := mustOpen(t, cfg)
	const n = 200
	enqueueN(t, q, n)

	var successAcks, deliveries int64
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if atomic.LoadInt64(&successAcks) == n {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				l, err := q.Dequeue(ctx)
				cancel()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						continue
					}
					return
				}
				atomic.AddInt64(&deliveries, 1)
				time.Sleep(time.Duration(atomic.AddInt32(&spin, 1)%30) * time.Millisecond)
				if err := q.Ack(l); err == nil {
					atomic.AddInt64(&successAcks, 1)
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("竞争测试超时，可能有任务卡死")
	}

	if successAcks != n {
		t.Fatalf("业务断言失败: 成功确认数期望 %d，实际 %d（总投递 %d）",
			n, successAcks, deliveries)
	}
	if dls := q.DeadLetters(); len(dls) != 0 {
		t.Fatalf("业务断言失败: 竞争场景不应产生死信，实际 %d 条", len(dls))
	}
	t.Logf("断言通过: %d 个任务在确认/超时竞争下均恰好成功确认一次（总投递 %d，含超时重投）",
		n, deliveries)
}

// TestRetryThenDeadLetter：可重试失败按退避重投，次数用尽进死信；
// 不可重试失败立即进死信；死信可查到原因、投递次数、入队时间。
func TestRetryThenDeadLetter(t *testing.T) {
	q := mustOpen(t, testConfig(t))

	idA, err := q.Enqueue(context.Background(), []byte("payload-A"))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for attempt := 1; attempt <= 3; attempt++ {
		l, err := q.Dequeue(context.Background())
		if err != nil {
			t.Fatalf("A 第 %d 次投递: %v", attempt, err)
		}
		if l.TaskID != idA || l.Task.Attempts != attempt {
			t.Fatalf("A 投递代次异常: %+v, 期望 attempt=%d", l, attempt)
		}
		if err := q.Nack(l, Failure{Retryable: true, Reason: fmt.Sprintf("transient-%d", attempt)}); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		t.Logf("A 第 %d 次投递以可重试失败回执，等待退避重投", attempt)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(q.DeadLetters()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	dls := q.DeadLetters()
	if len(dls) != 1 || dls[0].ID != idA {
		t.Fatalf("死信异常: %+v", dls)
	}
	dl := dls[0]
	if dl.Attempts != 3 {
		t.Fatalf("死信投递次数期望 3，实际 %d", dl.Attempts)
	}
	if dl.Reason != "transient-3" || !bytes.Equal(dl.Payload, []byte("payload-A")) {
		t.Fatalf("死信原因/负载异常: %+v", dl)
	}
	if dl.EnqueuedAt.IsZero() || dl.DeadAt.Before(start) || dl.DeadAt.IsZero() {
		t.Fatalf("死信时间字段异常: 入队=%v 入死信=%v", dl.EnqueuedAt, dl.DeadAt)
	}
	t.Logf("断言通过: A 重试 3 次后入死信 reason=%q attempts=%d 入队=%v 入死信=%v",
		dl.Reason, dl.Attempts,
		dl.EnqueuedAt.Format(time.RFC3339Nano), dl.DeadAt.Format(time.RFC3339Nano))

	idB, _ := q.Enqueue(context.Background(), []byte("payload-B"))
	lb, err := q.Dequeue(context.Background())
	if err != nil || lb.TaskID != idB {
		t.Fatalf("B 投递异常: %v", err)
	}
	if err := q.Nack(lb, Failure{Retryable: false, Reason: "poison message"}); err != nil {
		t.Fatalf("Nack B: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	if s := q.Stats(); s.Ready+s.InFlight+s.Delayed != 0 {
		t.Fatalf("不可重试任务仍在流转: %+v", s)
	}
	dls = q.DeadLetters()
	if len(dls) != 2 || dls[1].Reason != "poison message" || dls[1].Attempts != 1 {
		t.Fatalf("B 的死信异常: %+v", dls)
	}
	t.Log("断言通过: B 不可重试，第 1 次投递即入死信，未再投递")
}

// TestLeaseTimeoutExhaustsToDeadLetter：消费者始终不确认时，
// 超时重投也要计入次数，用尽后入死信（原因标注租约超时）。
func TestLeaseTimeoutExhaustsToDeadLetter(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.MaxAttempts = 2
		c.LeaseTimeout = 30 * time.Millisecond
		c.InitialBackoff = 5 * time.Millisecond
		c.MaxBackoff = 5 * time.Millisecond
	})
	q := mustOpen(t, cfg)
	enqueueN(t, q, 1)

	got := make(map[string]uint64)
	deadline := time.Now().Add(3 * time.Second)
	for len(q.DeadLetters()) == 0 && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		l, err := q.Dequeue(ctx)
		cancel()
		if err != nil {
			continue
		}
		got[l.TaskID] = l.Delivery
		time.Sleep(45 * time.Millisecond) // 故意错过 Ack，触发超时
		if err := q.Ack(l); err == nil {
			t.Fatal("超时后的 Ack 不应成功")
		}
	}
	dls := q.DeadLetters()
	if len(dls) != 1 {
		t.Fatalf("期望 1 条死信，实际 %d", len(dls))
	}
	if dls[0].Attempts != 2 {
		t.Fatalf("死信投递次数期望 2，实际 %d", dls[0].Attempts)
	}
	if dls[0].Reason == "" {
		t.Fatal("死信必须记录超时原因")
	}
	t.Logf("断言通过: 超时重投计入次数，2 次后入死信 reason=%q", dls[0].Reason)
}
