package batcher_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gobatcher "github.com/plasne/go-batcher"
	"github.com/stretchr/testify/assert"
)

func TestEnqueue(t *testing.T) {

	t.Run("enqueue is allowed before startup", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			done()
		})
		operation := gobatcher.NewOperation(watcher, 0, struct{}{})
		err := batcher.Enqueue(operation)
		assert.NoError(t, err, "expect enqueue to be fine even if not started")
	})

	t.Run("enqueue must include an operation", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		err := batcher.Start()
		assert.NoError(t, err, "expecting no errors on startup")
		err = batcher.Enqueue(nil)
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Equal(t, gobatcher.NoOperationError{}, err, "expect a no-operation error")
	})

	t.Run("operations require a watcher", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		err := batcher.Start()
		assert.NoError(t, err, "expecting no errors on startup")
		operation := gobatcher.NewOperation(nil, 0, struct{}{})
		err = batcher.Enqueue(operation)
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Equal(t, gobatcher.NoWatcherError{}, err, "expect a no-watcher error")
	})

	t.Run("operations cannot exceed max capacity (provisioned)", func(t *testing.T) {
		res := gobatcher.NewProvisionedResource(1000)
		batcher := gobatcher.NewBatcher().
			WithRateLimiter(res)
		err := batcher.Start()
		assert.NoError(t, err, "expecting no errors on startup")
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			done()
		})
		operation := gobatcher.NewOperation(watcher, 2000, struct{}{})
		err = batcher.Enqueue(operation)
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Equal(t, gobatcher.TooExpensiveError{}, err, "expect a too-expensive-error error")
	})

	t.Run("operations cannot exceed max capacity (shared)", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithReservedCapacity(2000)
		batcher := gobatcher.NewBatcher().
			WithRateLimiter(res)
		err := batcher.Start()
		assert.NoError(t, err, "expecting no errors on startup")
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			done()
		})
		good := gobatcher.NewOperation(watcher, 11000, struct{}{})
		err = batcher.Enqueue(good)
		assert.NoError(t, err)
		bad := gobatcher.NewOperation(watcher, 13000, struct{}{})
		err = batcher.Enqueue(bad)
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Equal(t, gobatcher.TooExpensiveError{}, err, "expect a too-expensive-error error")
	})

	t.Run("operations cannot be attempted more than x times", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond)
		err := batcher.Start()
		assert.NoError(t, err, "expecting no errors on startup")
		attempts := 0
		func() {
			var op *gobatcher.Operation
			enqueue := func() {
				attempts++
				if eerr := batcher.Enqueue(op); eerr != nil {
					if eerr != nil {
						_ = eerr.Error() // improves code coverage
					}
					assert.Equal(t, gobatcher.TooManyAttemptsError{}, eerr, "expect the error to be too-many-attempts")
					return
				}
				if attempts > 8 {
					assert.FailNow(t, "the max-attempts governor didn't work, we have tried too many times")
				}
			}
			watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
				enqueue()
				done()
			}).WithMaxAttempts(3)
			op = gobatcher.NewOperation(watcher, 100, struct{}{})
			enqueue()
			time.Sleep(100 * time.Millisecond)
		}()
		assert.Equal(t, 4, attempts, "expect enqueue will be accepted 3 times, but fail on the 4th")
	})

	multipleEnqueueTests := []bool{false, true}
	for _, batching := range multipleEnqueueTests {
		testName := fmt.Sprintf("operations can be enqueued multiple times at once (batch:%v)", batching)
		t.Run(testName, func(t *testing.T) {
			batcher := gobatcher.NewBatcher().
				WithFlushInterval(1 * time.Millisecond)
			var updateCountersMutex sync.Mutex
			var attempts uint32
			func() {
				count := 0
				watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
					func() {
						updateCountersMutex.Lock()
						defer updateCountersMutex.Unlock()
						for _, entry := range batch {
							count++
							if entry.Attempt() > attempts {
								attempts = entry.Attempt()
							}
						}
					}()
					done()
					if count > 3 {
						return
					}
				}).WithMaxAttempts(1)
				// NOTE: enqueue before start to ensure nothing is processed when enqueueing
				var err error
				var op = gobatcher.NewOperation(watcher, 100, struct{}{}).WithBatching(batching)
				err = batcher.Enqueue(op)
				assert.NoError(t, err, "expecting no error on enqueue")
				err = batcher.Enqueue(op)
				assert.NoError(t, err, "expecting no error on enqueue")
				err = batcher.Enqueue(op)
				assert.NoError(t, err, "expecting no error on enqueue")
				err = batcher.Enqueue(op)
				assert.NoError(t, err, "expecting no error on enqueue")
				err = batcher.Start()
				assert.NoError(t, err, "expecting no errors on startup")
				time.Sleep(100 * time.Millisecond)
			}()
			assert.Equal(t, uint32(4), attempts, "expecting 4 attempts were made (even though max is 1) because enqueue happened before processing")
		})
	}

	t.Run("enqueue will block caller if buffer full (default)", func(t *testing.T) {
		batcher := gobatcher.NewBatcherWithBuffer(1)
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			done()
		})
		var err error
		op1 := gobatcher.NewOperation(watcher, 0, struct{}{})
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "expecting no error on enqueue")
		done := make(chan bool, 1)
		go func() {
			op2 := gobatcher.NewOperation(watcher, 0, struct{}{})
			err = batcher.Enqueue(op2)
			assert.NoError(t, err, "expecting no error on enqueue")
			done <- true
		}()
		timeout := false
		select {
		case <-done:
			assert.Fail(t, "did not expect the enqueue to complete because there was no buffer")
		case <-time.After(500 * time.Millisecond):
			timeout = true
		}
		assert.True(t, timeout, "expecting the second enqueue to timeout (was blocking)")
	})

	t.Run("enqueue will throw error if buffer is full (config)", func(t *testing.T) {
		batcher := gobatcher.NewBatcherWithBuffer(1).
			WithErrorOnFullBuffer()
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			done()
		})
		var err error
		op1 := gobatcher.NewOperation(watcher, 0, struct{}{})
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "expecting no error on enqueue")
		op2 := gobatcher.NewOperation(watcher, 0, struct{}{})
		err = batcher.Enqueue(op2)
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Equal(t, gobatcher.BufferFullError{}, err, "expecting the buffer to be full")
	})

}

func TestOperationsInBuffer(t *testing.T) {

	t.Run("enqueuing operations increases num in buffer", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			done()
		})
		op := gobatcher.NewOperation(watcher, 100, struct{}{})
		err := batcher.Enqueue(op)
		assert.NoError(t, err, "expecting no error on enqueue")
		cap := batcher.OperationsInBuffer()
		assert.Equal(t, uint32(1), cap, "expecting the number of operations to match the number enqueued")
	})

	multipleDoneTests := []bool{false, true}
	for _, batching := range multipleDoneTests {
		testName := fmt.Sprintf("marking operations as done reduces num in buffer (batch:%v)", batching)
		t.Run(testName, func(t *testing.T) {
			batcher := gobatcher.NewBatcher()
			wg := sync.WaitGroup{}
			wg.Add(4)
			watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
				for i := 0; i < len(batch); i++ {
					wg.Done()
				}
				done()
			})
			for i := 0; i < 4; i++ {
				op := gobatcher.NewOperation(watcher, 100, struct{}{}).WithBatching(batching)
				err := batcher.Enqueue(op)
				assert.NoError(t, err, "expecting no error on enqueue")
			}
			before := batcher.OperationsInBuffer()
			err := batcher.Start()
			assert.NoError(t, err, "expecting no error on enqueue")
			wg.Wait()
			after := batcher.OperationsInBuffer()
			assert.Equal(t, uint32(4), before, "expecting the buffer to include all records before processing")
			assert.Equal(t, uint32(0), after, "expecting the buffer to be empty after processing")
		})
	}

}

func TestNeedsCapacity(t *testing.T) {

	t.Run("cost updates the target", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			done()
		})
		op := gobatcher.NewOperation(watcher, 100, struct{}{})
		err := batcher.Enqueue(op)
		assert.NoError(t, err, "expecting no error on enqueue")
		cap := batcher.NeedsCapacity()
		assert.Equal(t, uint32(100), cap, "expecting the capacity to match the operation cost")
	})

	multipleDoneTests := []bool{false, true}
	for _, batching := range multipleDoneTests {
		testName := fmt.Sprintf("marking operations as done reduces target (batch:%v)", batching)
		t.Run(testName, func(t *testing.T) {
			batcher := gobatcher.NewBatcher()
			wg := sync.WaitGroup{}
			wg.Add(4)
			watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
				done()
				for i := 0; i < len(batch); i++ {
					wg.Done()
				}
			})
			for i := 0; i < 4; i++ {
				op := gobatcher.NewOperation(watcher, 100, struct{}{}).WithBatching(batching)
				err := batcher.Enqueue(op)
				assert.NoError(t, err, "expecting no error on enqueue")
			}
			before := batcher.NeedsCapacity()
			err := batcher.Start()
			assert.NoError(t, err, "expecting no error on enqueue")
			wg.Wait()
			time.Sleep(100 * time.Millisecond)
			after := batcher.NeedsCapacity()
			assert.Equal(t, uint32(400), before, "expecting the cost to be the sum of all operations")
			assert.Equal(t, uint32(0), after, "expecting the cost be 0 after processing")
		})
	}

	t.Run("ensure operation costs result in requests", func(t *testing.T) {
		res := gobatcher.NewProvisionedResource(10000)
		batcher := gobatcher.NewBatcher().
			WithRateLimiter(res).
			WithFlushInterval(1 * time.Millisecond)
		var max int
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "request":
				if val > max {
					max = val
				}
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			// do not mark as done()
		})
		var err error
		op1 := gobatcher.NewOperation(watcher, 800, struct{}{})
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "expecting no error on enqueue")
		op2 := gobatcher.NewOperation(watcher, 300, struct{}{})
		err = batcher.Enqueue(op2)
		assert.NoError(t, err, "expecting no error on enqueue")
		err = batcher.Start()
		assert.NoError(t, err, "expecting no error on start")
		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, 1100, max, "expecting the request to be the sum of the operations")
	})

	t.Run("ensure operation costs result in target", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
		batcher := gobatcher.NewBatcher().
			WithRateLimiter(res).
			WithFlushInterval(1 * time.Millisecond)
		var max int
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "target":
				if val > max {
					max = val
				}
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			// do not mark as done()
		})
		var err error
		op1 := gobatcher.NewOperation(watcher, 800, struct{}{})
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "expecting no error on enqueue")
		op2 := gobatcher.NewOperation(watcher, 300, struct{}{})
		err = batcher.Enqueue(op2)
		assert.NoError(t, err, "expecting no error on enqueue")
		err = batcher.Start()
		assert.NoError(t, err, "expecting no error on start")
		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, 1100, max, "expecting the request to be the sum of the operations")
	})

}

type pauseDurations struct {
	id     string
	input  time.Duration
	output int64
}

func TestBatcherPause(t *testing.T) {

	durations := []pauseDurations{
		{id: "500 ms (default)", input: time.Duration(0), output: 500},
		{id: "750 ms", input: 750 * time.Millisecond, output: 750},
	}
	for _, duration := range durations {
		testName := fmt.Sprintf("ensure pause lasts for %v", duration.id)
		t.Run(testName, func(t *testing.T) {
			batcher := gobatcher.NewBatcher().
				WithPauseTime(duration.input)
			err := batcher.Start()
			assert.NoError(t, err, "not expecting a start error")
			wg := sync.WaitGroup{}
			wg.Add(2)
			var paused, resumed time.Time
			batcher.AddListener(func(event string, val int, msg *string) {
				switch event {
				case "pause":
					paused = time.Now()
					wg.Done()
				case "resume":
					resumed = time.Now()
					wg.Done()
				}
			})
			batcher.Pause()
			done := make(chan struct{})
			go func() {
				defer close(done)
				wg.Wait()
			}()
			select {
			case <-done:
				// saw a pause and resume
			case <-time.After(1 * time.Second):
				assert.Fail(t, "expected to be resumed before now")
			}
			len := resumed.Sub(paused)
			assert.GreaterOrEqual(t, len.Milliseconds(), duration.output, "expecting the pause to be at least %v ms", duration.output)
		})
	}

	t.Run("ensure multiple pauses do not increase the time", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		err := batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		wg := sync.WaitGroup{}
		wg.Add(2)
		var paused, resumed time.Time
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "pause":
				paused = time.Now()
				wg.Done()
			case "resume":
				resumed = time.Now()
				wg.Done()
			}
		})
		batcher.Pause()
		time.Sleep(100 * time.Millisecond)
		batcher.Pause()
		done := make(chan struct{})
		go func() {
			defer close(done)
			wg.Wait()
		}()
		select {
		case <-done:
			// saw a pause and resume
		case <-time.After(1 * time.Second):
			assert.Fail(t, "expected to be resumed before now")
		}
		len := resumed.Sub(paused)
		assert.GreaterOrEqual(t, len.Milliseconds(), int64(500), "expecting the pause to be at least 500 ms")
		assert.Less(t, len.Milliseconds(), int64(600), "expecting the pause to be under 600 ms")
	})

	t.Run("ensure negative duration uses 500 ms (default)", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithPauseTime(-100 * time.Millisecond)
		err := batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		wg := sync.WaitGroup{}
		wg.Add(2)
		var paused, resumed time.Time
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "pause":
				paused = time.Now()
				wg.Done()
			case "resume":
				resumed = time.Now()
				wg.Done()
			}
		})
		batcher.Pause()
		done := make(chan struct{})
		go func() {
			defer close(done)
			wg.Wait()
		}()
		select {
		case <-done:
			// saw a pause and resume
		case <-time.After(1 * time.Second):
			assert.Fail(t, "expected to be resumed before now")
		}
		len := resumed.Sub(paused)
		assert.GreaterOrEqual(t, len.Milliseconds(), int64(500), "expecting the pause to be at least 500 ms")
		assert.Less(t, len.Milliseconds(), int64(600), "expecting the pause to be under 600 ms")
	})

	t.Run("ensure no processing happens during a pause", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		err := batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		wg := sync.WaitGroup{}
		wg.Add(2)
		resumed := false
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "resume":
				resumed = true
				wg.Done()
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			assert.True(t, resumed, "all batches should be raised after resume")
			done()
			wg.Done()
		})
		batcher.Pause()
		op := gobatcher.NewOperation(watcher, 100, struct{}{})
		err = batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		done := make(chan struct{})
		go func() {
			defer close(done)
			wg.Wait()
		}()
		select {
		case <-done:
			// saw a pause and resume
		case <-time.After(1 * time.Second):
			assert.Fail(t, "expected to be completed before now")
		}
		assert.True(t, resumed, "expecting the pause to have resumed")
	})

}

func TestBatcherStart(t *testing.T) {

	t.Run("start without using new fails", func(t *testing.T) {
		batcher := gobatcher.Batcher{}
		err := batcher.Start()
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Error(t, gobatcher.BufferNotAllocated{}, err, "expecting the startup to fail since the buffer was never allocated")
	})

	t.Run("start is callable only once", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		var err1, err2 error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			err1 = batcher.Start()
			wg.Done()
		}()
		go func() {
			err2 = batcher.Start()
			wg.Done()
		}()
		wg.Wait()
		if e, ok := err1.(gobatcher.BatcherImproperOrderError); ok && err2 == nil {
			// valid response
			_ = e.Error() // improves code coverage
		} else if e, ok := err2.(gobatcher.BatcherImproperOrderError); ok && err1 == nil {
			// valid response
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected one of the two calls to fail (err1: %v) (err2: %v)", err1, err2)
		}
	})

	t.Run("ensure that mixed operations are batched or not as appropriate", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		var err error
		wg := sync.WaitGroup{}
		wg.Add(2)
		var op1, op2, op3 *gobatcher.Operation
		var count uint32 = 0
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			atomic.AddUint32(&count, uint32(len(batch)))
			switch len(batch) {
			case 1:
				assert.Equal(t, op2, batch[0], "expect that the batch has op2")
			case 2:
				assert.Equal(t, op1, batch[0], "expect that the batch has op1 and op3")
				assert.Equal(t, op3, batch[1], "expect that the batch has op1 and op3")
			}
			done()
			wg.Done()
		})
		op1 = gobatcher.NewOperation(watcher, 100, struct{}{}).AllowBatch()
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "not expecting an enqueue error")
		op2 = gobatcher.NewOperation(watcher, 100, struct{}{})
		err = batcher.Enqueue(op2)
		assert.NoError(t, err, "not expecting an enqueue error")
		op3 = gobatcher.NewOperation(watcher, 100, struct{}{}).AllowBatch()
		err = batcher.Enqueue(op3)
		assert.NoError(t, err, "not expecting an enqueue error")
		err = batcher.Start()
		assert.NoError(t, err, "not expecting an startup error")
		wg.Wait()
		assert.Equal(t, uint32(3), count, "expect 3 operations to be completed")
	})

	t.Run("ensure full batches are flushed", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond)
		var count uint32 = 0
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			atomic.AddUint32(&count, 1)
			assert.Equal(t, 3, len(batch), "expect batches to have 3 operations each")
			done()
		}).WithMaxBatchSize(3)
		for i := 0; i < 9; i++ {
			op := gobatcher.NewOperation(watcher, 100, struct{}{}).AllowBatch()
			err := batcher.Enqueue(op)
			assert.NoError(t, err, "not expecting an enqueue error")
		}
		err := batcher.Start()
		assert.NoError(t, err, "not expecting an startup error")
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, uint32(3), count, "expect 3 batches")
	})

}

func TestBatcherStop(t *testing.T) {

	t.Run("stop emits shutdown", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		done := make(chan bool)
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "shutdown":
				close(done)
			}
		})
		err := batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		batcher.Stop()
		select {
		case <-done:
			// success
		case <-time.After(1 * time.Second):
			// timeout
			assert.Fail(t, "expected shutdown but didn't see one even after 1 second")
		}
	})

	t.Run("stop before start does not shutdown", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		done := make(chan bool)
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "shutdown":
				close(done)
			}
		})
		batcher.Stop()
		select {
		case <-done:
			// success
			assert.Fail(t, "expected no shutdown")
		case <-time.After(1 * time.Second):
			// timeout; no shutdown as expected
		}
	})

	t.Run("multiple stops shutdown only once", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		count := 0
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "shutdown":
				count += 1
			}
		})
		err := batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		go func() {
			batcher.Stop()
		}()
		go func() {
			batcher.Stop()
		}()
		time.Sleep(1 * time.Second)
		assert.Equal(t, 1, count, "expecting only a single shutdown")
	})

}

type flushIntervalTest struct {
	id       string
	interval time.Duration
	enqueue  int
	wait     time.Duration
	expect   uint32
}

type capacityIntervalTest struct {
	id       string
	interval time.Duration
	wait     time.Duration
	expect   uint32
}

func TestTimers(t *testing.T) {

	flushIntervalTests := []flushIntervalTest{
		{id: "-200ms (default to 100)", interval: -200 * time.Millisecond, enqueue: 4, wait: 250 * time.Millisecond, expect: 2},
		{id: "100ms (default)", interval: 0 * time.Millisecond, enqueue: 4, wait: 250 * time.Millisecond, expect: 2},
		{id: "300ms", interval: 300 * time.Millisecond, enqueue: 4, wait: 650 * time.Millisecond, expect: 2},
	}
	for _, d := range flushIntervalTests {
		testName := fmt.Sprintf("ensure operations are flushed in %v", d.id)
		t.Run(testName, func(t *testing.T) {
			res := gobatcher.NewProvisionedResource(100)
			batcher := gobatcher.NewBatcher().
				WithRateLimiter(res).
				WithFlushInterval(d.interval)
			var count uint32 = 0
			watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
				atomic.AddUint32(&count, uint32(len(batch)))
				done()
			})
			for i := 0; i < d.enqueue; i++ {
				op := gobatcher.NewOperation(watcher, 100, struct{}{})
				err := batcher.Enqueue(op)
				assert.NoError(t, err, "not expecting an enqueue error")
			}
			err := batcher.Start()
			assert.NoError(t, err, "not expecting a start error")
			time.Sleep(d.wait)
			assert.Equal(t, d.expect, count, "expecting %v operations to be completed given the %v interval and capacity for only a single operation", d.interval, d.expect)
		})
	}

	capacityIntervalTests := []capacityIntervalTest{
		{id: "-200ms (default to 100ms)", interval: 0 * time.Millisecond, wait: 250 * time.Millisecond, expect: 2},
		{id: "100ms (default)", interval: 0 * time.Millisecond, wait: 250 * time.Millisecond, expect: 2},
		{id: "300ms", interval: 300 * time.Millisecond, wait: 650 * time.Millisecond, expect: 2},
	}
	for _, d := range capacityIntervalTests {
		testName := fmt.Sprintf("ensure capacity requests are raised every %v", d.id)
		t.Run(testName, func(t *testing.T) {
			res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000)
			batcher := gobatcher.NewBatcher().
				WithRateLimiter(res).
				WithCapacityInterval(d.interval)
			var count uint32 = 0
			batcher.AddListener(func(event string, val int, msg *string) {
				switch event {
				case "request":
					count += 1
				}
			})
			watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
				done()
			})
			op := gobatcher.NewOperation(watcher, 800, struct{}{})
			err := batcher.Enqueue(op)
			assert.NoError(t, err, "not expecting an enqueue error")
			err = batcher.Start()
			assert.NoError(t, err, "not expecting a start error")
			time.Sleep(d.wait)
			assert.Equal(t, d.expect, count, "expecting %v capacity requests given the %v interval and capacity for only a single operation", d.interval, d.expect)
		})
	}

	t.Run("ensure abandoned operations are still marked done (watcher)", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond)
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			// don't mark as done
		}).WithMaxOperationTime(10 * time.Millisecond)
		op := gobatcher.NewOperation(watcher, 100, struct{}{})
		err := batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		before := batcher.NeedsCapacity()
		err = batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(20 * time.Millisecond)
		after := batcher.NeedsCapacity()
		assert.Equal(t, uint32(100), before, "expecting 100 capacity request before starting")
		assert.Equal(t, uint32(0), after, "expecting 0 capacity request after max-operation-time")
	})

	t.Run("ensure abandoned operations are still marked done (batcher)", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond).
			WithMaxOperationTime(10 * time.Millisecond)
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			// don't mark as done
		})
		op := gobatcher.NewOperation(watcher, 100, struct{}{})
		err := batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		before := batcher.NeedsCapacity()
		err = batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(20 * time.Millisecond)
		after := batcher.NeedsCapacity()
		assert.Equal(t, uint32(100), before, "expecting 100 capacity request before starting")
		assert.Equal(t, uint32(0), after, "expecting 0 capacity request after max-operation-time")
	})

	t.Run("ensure abandoned operations are not marked done before 1 min", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond)
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			// don't mark as done
		})
		op := gobatcher.NewOperation(watcher, 100, struct{}{})
		err := batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		before := batcher.NeedsCapacity()
		err = batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(200 * time.Millisecond)
		after := batcher.NeedsCapacity()
		assert.Equal(t, uint32(100), before, "expecting 100 capacity request before starting")
		assert.Equal(t, uint32(100), after, "expecting 100 capacity request after 200 milliseconds")
	})

}

func TestAudit(t *testing.T) {

	t.Run("demonstrate an audit-pass", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond).
			WithAuditInterval(1 * time.Millisecond).
			WithMaxOperationTime(1 * time.Millisecond)
		var passed, failed bool
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "audit-pass":
				passed = true
			case "audit-fail":
				failed = true
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			// NOTE: using a max-op-time of 1ms removes the targets whether done is called or not
			done()
		})
		op := gobatcher.NewOperation(watcher, 100, struct{}{})
		err := batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		err = batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(10 * time.Millisecond)
		assert.True(t, passed, "expecting audit-pass because done() was called before max-operation-time (1m default)")
		assert.False(t, failed, "expecting no audit-fail messages")
	})

	t.Run("demonstrate an audit-fail", func(t *testing.T) {
		// NOTE: this sets a batcher max-op-time to 1ms and a watcher max-op-time to 1m allowing for the target to be around longer than it thinks it should be
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond).
			WithAuditInterval(1 * time.Millisecond).
			WithMaxOperationTime(1 * time.Millisecond)
		failed := false
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "audit-fail":
				failed = true
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			// don't mark as done
		}).WithMaxOperationTime(1 * time.Minute)
		op := gobatcher.NewOperation(watcher, 100, struct{}{})
		err := batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		err = batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(10 * time.Millisecond)
		assert.True(t, failed, "expecting an audit failure because done() was not called and max-operation-time was exceeded")
	})

	t.Run("demonstrate an audit-skip", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond).
			WithAuditInterval(1 * time.Millisecond)
		var skipped bool
		batcher.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "audit-skip":
				skipped = true
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			// don't mark as done
		})
		var err error
		op := gobatcher.NewOperation(watcher, 100, struct{}{})
		err = batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		err = batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(10 * time.Millisecond)
		assert.True(t, skipped, "expect that something in the buffer but max-operation-time is still valid, will cause skips")
	})

}
