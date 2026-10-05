package dtq_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	dtq "github.com/lechandonga/durable-task-queue-go"
)

// ExampleQueue 演示生产者投递、消费者处理、失败重试与死信查询的完整流程。
func ExampleQueue() {
	q, err := dtq.Open(dtq.Config{
		Dir:            mustTempDir(),
		MaxAttempts:    3,
		LeaseTimeout:   50 * time.Millisecond,
		InitialBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	ctx := context.Background()
	if _, err := q.Enqueue(ctx, []byte(`{"order":"A-1"}`)); err != nil {
		log.Fatal(err)
	}

	// 第一次处理失败：上报可重试失败，队列按退避重新投递。
	l, err := q.Dequeue(ctx)
	if err != nil {
		log.Fatal(err)
	}
	_ = q.Nack(l, dtq.Failure{Retryable: true, Reason: "temporary network error"})

	// 退避后拿到同一任务的新一代投递，这次处理成功并确认。
	l2, err := q.Dequeue(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := q.Ack(l2); err != nil {
		log.Fatal(err)
	}

	// 不可重试的任务会直接进入死信，可用 DeadLetters 审计。
	_, _ = q.Enqueue(ctx, []byte("poison"))
	l3, err := q.Dequeue(ctx)
	if err != nil {
		log.Fatal(err)
	}
	_ = q.Nack(l3, dtq.Failure{Retryable: false, Reason: "invalid payload"})

	for _, d := range q.DeadLetters() {
		fmt.Printf("dead: attempts=%d reason=%s\n", d.Attempts, d.Reason)
	}

	// Output:
	// dead: attempts=1 reason=invalid payload
}

func mustTempDir() string {
	d, err := os.MkdirTemp("", "dtq-example-*")
	if err != nil {
		log.Fatal(err)
	}
	return d
}
