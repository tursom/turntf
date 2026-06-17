package store

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	// groupCommitMaxOps 是触发强制刷盘的最大批处理操作数。
	// 当累积的 relaxed 写入操作数达到此阈值时，立即提交所有挂起的批次。
	groupCommitMaxOps = 128

	// groupCommitMaxDelay 是 relaxed 写入的最大等待延迟。
	// 在首次 relaxed 写入后启动计时器，到期后强制提交挂起批次，避免写入无限期延迟。
	groupCommitMaxDelay = 5 * time.Millisecond
)

// pebbleWriteCoordinator 是 Pebble 写入协调器，实现组提交（group commit）策略。
//
// 设计意图：Pebble 的每次 Commit(pebble.Sync) 都会触发 fsync，在高吞吐场景下代价高昂。
// 该协调器将非关键写入（relaxed，无需即时持久化）批量累积，达到 ops 或时间阈值后统一刷盘；
// 关键写入（forceSync）则立即执行同步提交，确保数据安全。
//
// 在后端架构中的角色：
// - 事件日志（非消息事件）需要同步刷盘以保证一致性
// - 消息投影写入使用 relaxed 模式，由协调器合并为更大的批量提交以提升吞吐
type pebbleWriteCoordinator struct {
	// db 是底层的 Pebble 数据库实例
	db *pebble.DB

	// requests 是写入请求的通道，run goroutine 从中消费并处理
	requests chan pebbleWriteRequest
	// closeCh 是关闭请求通道，用于优雅终止 run goroutine
	closeCh chan chan error
	// done 在 run goroutine 退出时关闭，用于同步等待
	done chan struct{}

	// stateMu 保护 closed、asyncErr、stats 的并发访问
	stateMu sync.Mutex
	// closed 标记协调器是否已关闭
	closed bool
	// asyncErr 记录异步处理过程中发生的错误，供后续调用检查
	asyncErr error
	// stats 记录协调器的运行统计信息
	stats pebbleWriteCoordinatorStats
}

// pebbleWriteRequest 是提交到协调器的单个写入请求。
type pebbleWriteRequest struct {
	// batch 是待提交的 Pebble 批次操作
	batch *pebble.Batch
	// forceSync 为 true 时将绕过组提交逻辑直接执行同步刷盘
	forceSync bool
	// response 是用于返回处理结果的通道，发送 nil 表示成功
	response chan error
}

// pendingPebbleBatch 是协调器中挂起的待提交批次。
type pendingPebbleBatch struct {
	// batch 是已通过 ApplyNoSyncWait 提交但尚未 SyncWait 的批次
	batch *pebble.Batch
	// ops 是批次中包含的操作数，用于累计判断是否达到组提交阈值
	ops int
}

// pebbleWriteCoordinatorStats 记录协调器的运行统计信息。
type pebbleWriteCoordinatorStats struct {
	// RelaxedBatches 是已处理的 non-forceSync 批次总数
	RelaxedBatches uint64
	// ForceSyncBatches 是已处理的 forceSync 批次总数
	ForceSyncBatches uint64
	// FlushesBySize 是因操作数达到阈值而触发的刷盘次数
	FlushesBySize uint64
	// FlushesByDelay 是因超时而触发的刷盘次数
	FlushesByDelay uint64
	// FlushesByForce 是因 forceSync 请求而触发的刷盘次数
	FlushesByForce uint64
}

// newPebbleWriteCoordinator 创建写入协调器并启动后台处理 goroutine。
// 如果 db 为 nil，返回 nil（表示不使用协调器，直接写入）。
func newPebbleWriteCoordinator(db *pebble.DB) *pebbleWriteCoordinator {
	if db == nil {
		return nil
	}
	c := &pebbleWriteCoordinator{
		db:       db,
		requests: make(chan pebbleWriteRequest),
		closeCh:  make(chan chan error, 1),
		done:     make(chan struct{}),
	}
	go c.run()
	return c
}

// Apply 提交一个 Pebble 批次到写入协调器。
//
// 参数:
//   - batch: 待提交的 Pebble 批次（不为 nil）
//   - forceSync: 是否强制同步刷盘
//
// 行为:
//   - forceSync=true: 立即同步提交，绕过组提交（先刷空挂起批次）
//   - forceSync=false: 通过 ApplyNoSyncWait 非阻塞提交，等待组提交
//
// 并发安全: 是，支持多 goroutine 并发调用。
// 性能特征: forceSync 延迟低但 fsync 开销高；relaxed 延迟稍高但总体吞吐更高。
func (c *pebbleWriteCoordinator) Apply(batch *pebble.Batch, forceSync bool) error {
	if batch == nil {
		return fmt.Errorf("%w: pebble batch cannot be nil", ErrInvalidInput)
	}
	if c == nil {
		defer batch.Close()
		if forceSync {
			return batch.Commit(pebble.Sync)
		}
		return batch.Commit(pebble.NoSync)
	}
	if err := c.stateError(); err != nil {
		_ = batch.Close()
		return err
	}

	response := make(chan error, 1)
	req := pebbleWriteRequest{
		batch:     batch,
		forceSync: forceSync,
		response:  response,
	}

	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		_ = batch.Close()
		return errors.New("pebble write coordinator is closed")
	}
	c.stateMu.Unlock()

	c.requests <- req
	if err := <-response; err != nil {
		return err
	}
	return c.stateError()
}

// Flush 将所有挂起的批次强制刷盘。
// 实现方式：提交一个空的 forceSync 批次，利用其先刷空挂起批次再提交自身的特性。
func (c *pebbleWriteCoordinator) Flush() error {
	if c == nil {
		return nil
	}
	batch := c.db.NewBatch()
	return c.Apply(batch, true)
}

// Close 优雅关闭写入协调器。
// 先刷空所有挂起批次，然后停止后台 goroutine。
// 如果已经关闭，返回已有的 asyncErr（如果有）。
func (c *pebbleWriteCoordinator) Close() error {
	if c == nil {
		return nil
	}

	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		<-c.done
		return c.stateError()
	}
	c.closed = true
	c.stateMu.Unlock()

	response := make(chan error, 1)
	c.closeCh <- response
	err := <-response
	<-c.done
	if err != nil {
		return err
	}
	return c.stateError()
}

// statsSnapshot 返回协调器统计信息的当前快照。
func (c *pebbleWriteCoordinator) statsSnapshot() pebbleWriteCoordinatorStats {
	if c == nil {
		return pebbleWriteCoordinatorStats{}
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.stats
}

// run 是写入协调器的主循环，在独立的 goroutine 中运行。
//
// 处理逻辑:
//  1. forceSync 请求: 先刷空挂起批次，再同步提交当前批次
//  2. relaxed 请求: 通过 ApplyNoSyncWait 非阻塞写入，累积到 pending 列表
//     - 当 pendingOps >= groupCommitMaxOps 时立即刷盘（"size"）
//     - 否则启动延迟计时器，到期自动刷盘（"delay"）
//  3. closeCh: 刷空挂起批次后退出
//  4. timerC: 延迟到期后刷盘
//
// 错误处理: flushPending 或同步提交失败时调用 setAsyncErr 记录，
// 之后所有后续请求立即返回该错误，不再处理新写入。
func (c *pebbleWriteCoordinator) run() {
	defer close(c.done)

	var (
		pending    []pendingPebbleBatch
		pendingOps int
		timer      *time.Timer
		timerC     <-chan time.Time
	)

	// stopTimer 安全停止延迟计时器并清理其通道状态。
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}

	// setAsyncErr 记录异步错误，仅保留第一个错误。
	// 一旦发生异步错误，后续所有请求将立即失败。
	setAsyncErr := func(err error) {
		if err == nil {
			return
		}
		c.stateMu.Lock()
		if c.asyncErr == nil {
			c.asyncErr = err
		}
		c.stateMu.Unlock()
	}

	// flushPending 提交所有挂起的 relaxed 批次。
	// 对每个批次依次调用 SyncWait（等待写入完成）和 Close（释放资源）。
	// reason 参数用于统计计数（"size"/"delay"/"force"）。
	flushPending := func(reason string) error {
		if len(pending) == 0 {
			stopTimer()
			return nil
		}

		current := pending
		pending = nil
		pendingOps = 0
		stopTimer()

		var flushErr error
		for _, item := range current {
			if err := item.batch.SyncWait(); err != nil && flushErr == nil {
				flushErr = err
			}
			if err := item.batch.Close(); err != nil && flushErr == nil {
				flushErr = err
			}
		}
		if flushErr == nil {
			c.stateMu.Lock()
			switch reason {
			case "size":
				c.stats.FlushesBySize++
			case "delay":
				c.stats.FlushesByDelay++
			case "force":
				c.stats.FlushesByForce++
			}
			c.stateMu.Unlock()
		}
		return flushErr
	}

	for {
		select {
		case req := <-c.requests:
			if req.batch == nil {
				req.response <- fmt.Errorf("%w: pebble batch cannot be nil", ErrInvalidInput)
				continue
			}
			if err := c.stateError(); err != nil {
				_ = req.batch.Close()
				req.response <- err
				continue
			}

			if req.forceSync {
				err := flushPending("force")
				if err == nil {
					err = req.batch.Commit(pebble.Sync)
				}
				if closeErr := req.batch.Close(); err == nil && closeErr != nil {
					err = closeErr
				}
				if err != nil {
					setAsyncErr(err)
				} else {
					c.stateMu.Lock()
					c.stats.ForceSyncBatches++
					c.stateMu.Unlock()
				}
				req.response <- err
				continue
			}

			if err := c.db.ApplyNoSyncWait(req.batch, pebble.Sync); err != nil {
				_ = req.batch.Close()
				req.response <- err
				continue
			}
			ops := int(req.batch.Count())
			if ops <= 0 {
				ops = 1
			}
			pending = append(pending, pendingPebbleBatch{batch: req.batch, ops: ops})
			pendingOps += ops
			c.stateMu.Lock()
			c.stats.RelaxedBatches++
			c.stateMu.Unlock()

			var err error
			if pendingOps >= groupCommitMaxOps {
				err = flushPending("size")
				if err != nil {
					setAsyncErr(err)
				}
			} else if timer == nil {
				timer = time.NewTimer(groupCommitMaxDelay)
				timerC = timer.C
			}
			req.response <- err

		case <-timerC:
			if err := flushPending("delay"); err != nil {
				setAsyncErr(err)
			}

		case response := <-c.closeCh:
			err := flushPending("force")
			if err != nil {
				setAsyncErr(err)
			}
			response <- err
			return
		}
	}
}

// stateError 返回协调器的异步错误（如果有），用于快速失败后续请求。
func (c *pebbleWriteCoordinator) stateError() error {
	if c == nil {
		return nil
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.asyncErr
}
