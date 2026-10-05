# durable-task-queue-go

仅依赖 Go 标准库和本地磁盘的并发持久化任务队列：支持多生产者 / 多消费者、
租约超时自动重投、失败分类重试与死信、进程崩溃恢复。无外部服务、无中间件。

## 快速开始

```go
package main

import (
    "context"
    "log"
    "time"

    dtq "github.com/lechandonga/durable-task-queue-go"
)

func main() {
    q, err := dtq.Open(dtq.Config{
        Dir:            "./queue-data",
        MaxAttempts:    5,
        LeaseTimeout:   30 * time.Second,
        InitialBackoff: time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close()

    // 生产者
    _, _ = q.Enqueue(context.Background(), []byte(`{"job":"render"}`))

    // 消费者（可开任意多个 goroutine / 进程内并发）
    for {
        lease, err := q.Dequeue(context.Background())
        if err != nil {
            return
        }
        if err := handle(lease.Task.Payload); err != nil {
            _ = q.Nack(lease, dtq.Failure{Retryable: true, Reason: err.Error()})
            continue
        }
        _ = q.Ack(lease)
    }
}
```

完整可运行示例见 `example_test.go`（`ExampleQueue`）。

## 投递语义

- **至少一次（at-least-once）**：任务在被 `Ack` 之前，其每次发放都先以
  `leased` 记录 `fsync` 落盘，之后才交给消费者，因此崩溃不会丢失已发放任务。
- **同一时刻最多一个持有者**：每次发放都会递增投递代次 `Delivery` 并写新
  `leased` 记录。`Ack/Nack` 必须同时携带**任务 ID 与代次**，只有当前代次
  的回执有效。
- **租约超时重投**：`Dequeue` 拿到的任务若在 `LeaseTimeout` 内没有有效
  回执，会自动进入退避并重新发放，避免“消费者宕机 → 任务永久卡死”。
- **确认与超时竞争的唯一结果**：
  - 若超时先发生，旧代次租约立即失效，迟到的 `Ack` 得到
    `dtq.ErrUnknownLease`，任务只会被新一代持有者处理；
  - 若 `Ack` 先落盘，任务永久消失，之后不会再被任何人拿到，重复 `Ack`
    同样返回 `ErrUnknownLease`。
  - 两种情况的结果都唯一，不会出现“已确认又执行”或“超时后永远没人拿到”。

## 重试与死信规则

- `Nack(lease, Failure{Retryable: true, ...})`：按指数退避重新投递，
  退避为 `InitialBackoff * BackoffFactor^(attempts-1)`，上限 `MaxBackoff`。
- `Nack(lease, Failure{Retryable: false, ...})`：**不重试**，直接进死信。
- 可重试失败次数用尽（投递次数达到 `MaxAttempts`）→ 进死信。
- 消费者始终不回执导致的**租约超时也计入投递次数**，用尽后同样进死信，
  死信原因为 `"lease timed out: consumer did not acknowledge"`。
- 死信不会静默丢弃：`DeadLetters()` 返回每条死信的任务 ID、负载、
  失败原因、总投递次数、入队时间与进入死信的时间。

投递次数语义：每次实际发放（`Dequeue` 成功返回）计 1 次，随 `Lease.Task.Attempts`
暴露给消费者；崩溃恢复本身不额外消耗次数（在途任务按未过期重新发放处理）。

## 持久化与版本兼容

所有状态变化写入数据目录下的单个追加日志 `queue.log`，每条记录独立 `fsync`。

文件布局（小端序）：

```
文件头:  "DTQ1" + version:u16 + flags:u16              共 8 字节
帧:      len:u32 + payload:len字节 + crc32(ieee):u32
```

记录类型（payload 首字节，编号只增不复用）：

| 类型 | 含义 |
|---|---|
| 1 `enqueue` | 新任务（id、payload、入队时间） |
| 2 `leased` | 一次发放（id、代次、次数、租约到期时刻） |
| 3 `acked` | 成功确认（id、被确认的代次） |
| 4 `nacked` | 失败回执（是否可重试、原因、时间） |
| 5 `dead` | 进入死信（次数、原因、时间） |
| 6 `snapshot` | 压缩快照（全部存活任务 + 死信） |
| 7 `leaseRelease` | 正常关闭时把在途任务放回可投递 |

兼容策略：

- **版本只升不降**：文件头中的 `version` 是唯一格式版本号。当前为 `1`。
- **未知版本显式拒绝**：打开更高版本文件时 `Open` 返回
  `dtq.ErrUnknownVersion`，不截断、不覆盖、不做任何写入，原文件保持原样。
- **前向兼容预留**：同一版本内若出现无法识别的记录类型，按数据损坏处理并
  停止启动，避免误读。
- **崩溃截断**：只追加 + 定长帧 + CRC 意味着崩溃只会在文件末尾留下不完整
  的帧。恢复时从第一个长度或 CRC 不合法的帧起截断其后所有字节，
  截断字节数通过 `LastRecovery().TruncatedBytes` 报告；此前完整记录
  （已 fsync）全部有效，绝不读出脏数据。
- **压缩**：WAL 超过 `CompactionThreshold` 后把全部存活状态写成一个
  `snapshot` 帧，经“临时文件 + fsync + rename + 目录 fsync”原子替换；
  恢复时只重放最后一个快照及其后的记录。已确认任务由此被物理清除。

恢复行为：

- 正常关闭（`Close`）会给每个在途任务追加 `leaseRelease`，重启后它们直接
  回到可投递队列；
- 异常退出（kill -9 / 断电）时，未确认任务按落盘的租约到期时刻判定：
  未到期的继续保持在途，已到期的立即重新参与投递；待投递任务原样保留；
  已确认任务不会复活。
- 启动恢复报告见 `LastRecovery()`（重放记录数、截断字节数、ready/in-flight/
  delayed/死信计数）。

## 本地验证

```bash
# 全量测试（竞态检测），约 10 秒
CGO_ENABLED=1 go test -race -count=1 ./...

# 稳定性：重复运行
CGO_ENABLED=1 go test -race -count=10 ./...

# 仅看带业务说明的详细日志
CGO_ENABLED=1 go test -race -v ./...
```

> `-race` 需要 cgo；若环境报 `race requires cgo`，请设置
> `CGO_ENABLED=1`（并装有 C 工具链）。

测试与业务场景的对应关系：

| 用例 | 覆盖的业务 |
|---|---|
| `TestConcurrentProducersConsumers` | 6 生产者 × 40 条 + 8 消费者：无重复、无遗漏、每任务恰处理一次 |
| `TestLeaseTimeoutRedelivery` | 超时自动重投、迟到 Ack 被拒、已确认不再出现 |
| `TestAckAndTimeoutRace` | 200 任务在超时边界附近并发确认：结果唯一，无重复执行、无卡死 |
| `TestRetryThenDeadLetter` | 指数退避重投、次数用尽入死信、不可重试立即入死信，死信字段可查 |
| `TestLeaseTimeoutExhaustsToDeadLetter` | 超时次数计入投递，用尽后以超时原因入死信 |
| `TestDelayedRetryBackoff` | 退避窗口内不可见、窗口结束后才重投 |
| `TestGracefulCloseRecovery` | 正常关闭：在途/待投递回归可投，已确认不复活 |
| `TestCrashRecovery` | 模拟 kill -9：未确认与待投递不丢、已确认不复活 |
| `TestTruncatedTailRecovery` | 末尾残缺帧被识别、截断并报告，队列仍可用 |
| `TestCrcCorruptionRecovery` | CRC 损坏帧被隔离，不读出脏数据 |
| `TestUnknownVersionRejected` | 未知版本显式拒绝，且原文件字节不被修改 |
| `TestCompaction` | 快照压缩清除已确认记录，存活任务与死信完整保留 |

### 手工复现并发 / 超时竞争

```go
q, _ := dtq.Open(dtq.Config{
    Dir: "...", LeaseTimeout: 15 * time.Millisecond,
    InitialBackoff: time.Millisecond, MaxAttempts: 50,
})
// 1) 投 200 个任务
// 2) 开 12 个消费者 goroutine，Dequeue 后随机 sleep 0~30ms 再 Ack
// 3) 观察：每个任务恰好 1 次成功 Ack；Ack 返回 ErrUnknownLease 的
//    都是超时后重投的旧代次，属于预期竞争结果
```

### 手工复现崩溃恢复

```go
q, _ := dtq.Open(dtq.Config{Dir: dataDir, LeaseTimeout: 200 * time.Millisecond})
// 投递三个任务：Dequeue 后分别「不处理 / 不处理 / Ack」
// 直接 os.Exit(1)（或 kill -9），不要调用 q.Close

q2, _ := dtq.Open(dtq.Config{Dir: dataDir})
fmt.Println(q2.LastRecovery()) // 重启后前两个任务会被再次投递，第三个永不出现
```

## 设计边界

- 单进程内并发安全；不提供网络接口，也不支持多进程同时打开同一目录
  （多进程需要外部文件锁，当前未实现）。
- 状态保存在内存（map + 时间堆），适合单机后台任务量级；超大积压场景
  可调大 `CompactionThreshold` 或分片到多个队列目录。
