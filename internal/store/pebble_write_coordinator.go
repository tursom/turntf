package store

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	groupCommitMaxOps   = 64
	groupCommitMaxDelay = 2 * time.Millisecond
)

type pebbleWriteCoordinator struct {
	db *pebble.DB

	requests chan pebbleWriteRequest
	closeCh  chan chan error
	done     chan struct{}

	stateMu  sync.Mutex
	closed   bool
	asyncErr error
	stats    pebbleWriteCoordinatorStats
}

type pebbleWriteRequest struct {
	batch     *pebble.Batch
	forceSync bool
	response  chan error
}

type pendingPebbleBatch struct {
	batch *pebble.Batch
	ops   int
}

type pebbleWriteCoordinatorStats struct {
	RelaxedBatches   uint64
	ForceSyncBatches uint64
	FlushesBySize    uint64
	FlushesByDelay   uint64
	FlushesByForce   uint64
}

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

func (c *pebbleWriteCoordinator) Flush() error {
	if c == nil {
		return nil
	}
	batch := c.db.NewBatch()
	return c.Apply(batch, true)
}

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

func (c *pebbleWriteCoordinator) statsSnapshot() pebbleWriteCoordinatorStats {
	if c == nil {
		return pebbleWriteCoordinatorStats{}
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.stats
}

func (c *pebbleWriteCoordinator) run() {
	defer close(c.done)

	var (
		pending    []pendingPebbleBatch
		pendingOps int
		timer      *time.Timer
		timerC     <-chan time.Time
	)

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

func (c *pebbleWriteCoordinator) stateError() error {
	if c == nil {
		return nil
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.asyncErr
}
