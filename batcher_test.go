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
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {})
		operation := gobatcher.NewOperation(watcher, 0, struct{}{}, false)
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
		operation := gobatcher.NewOperation(nil, 0, struct{}{}, false)
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
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {})
		operation := gobatcher.NewOperation(watcher, 2000, struct{}{}, false)
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
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {})
		good := gobatcher.NewOperation(watcher, 11000, struct{}{}, false)
		err = batcher.Enqueue(good)
		assert.NoError(t, err)
		bad := gobatcher.NewOperation(watcher, 13000, struct{}{}, false)
		err = batcher.Enqueue(bad)
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Equal(t, gobatcher.TooExpensiveError{}, err, "expect a too-expensive-error error")
	})

	t.Run("operations cannot be attempted more than x times", func(t *testing.T) {
		// NOTE: this test works by recursively enqueuing the same operation over and over again until it fails
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond)
		err := batcher.Start()
		assert.NoError(t, err, "expecting no errors on startup")
		var mu sync.Mutex
		attempts := 0
		func() {
			var op gobatcher.IOperation
			enqueue := func() {
				mu.Lock()
				defer mu.Unlock()
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
			watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
				enqueue()
			}).WithMaxAttempts(3)
			op = gobatcher.NewOperation(watcher, 100, struct{}{}, false)
			enqueue()
			time.Sleep(100 * time.Millisecond)
		}()
		mu.Lock()
		defer mu.Unlock()
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
				watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
					updateCountersMutex.Lock()
					defer updateCountersMutex.Unlock()
					for _, entry := range batch {
						count++
						if entry.Attempt() > attempts {
							attempts = entry.Attempt()
						}
					}
					if count > 3 {
						return
					}
				}).WithMaxAttempts(1)
				// NOTE: enqueue before start to ensure nothing is processed when enqueueing
				var err error
				var op = gobatcher.NewOperation(watcher, 100, struct{}{}, batching)
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
			updateCountersMutex.Lock()
			defer updateCountersMutex.Unlock()
			assert.Equal(t, uint32(4), attempts, "expecting 4 attempts were made (even though max is 1) because enqueue happened before processing")
		})
	}

	t.Run("enqueue will block caller if buffer full (default)", func(t *testing.T) {
		batcher := gobatcher.NewBatcherWithBuffer(1)
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {})
		var err error
		op1 := gobatcher.NewOperation(watcher, 0, struct{}{}, false)
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "expecting no error on enqueue")
		done := make(chan bool, 1)
		go func() {
			op2 := gobatcher.NewOperation(watcher, 0, struct{}{}, false)
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
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {})
		var err error
		op1 := gobatcher.NewOperation(watcher, 0, struct{}{}, false)
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "expecting no error on enqueue")
		op2 := gobatcher.NewOperation(watcher, 0, struct{}{}, false)
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
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {})
		op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
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
			watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
				for i := 0; i < len(batch); i++ {
					wg.Done()
				}
			})
			for i := 0; i < 4; i++ {
				op := gobatcher.NewOperation(watcher, 100, struct{}{}, batching)
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
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {})
		op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
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
			watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
				for i := 0; i < len(batch); i++ {
					wg.Done()
				}
			})
			for i := 0; i < 4; i++ {
				op := gobatcher.NewOperation(watcher, 100, struct{}{}, batching)
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
		var mu sync.Mutex
		var max int
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case "request":
				mu.Lock()
				defer mu.Unlock()
				if val > max {
					max = val
				}
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			time.Sleep(400 * time.Millisecond)
		})
		var err error
		op1 := gobatcher.NewOperation(watcher, 800, struct{}{}, false)
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "expecting no error on enqueue")
		op2 := gobatcher.NewOperation(watcher, 300, struct{}{}, false)
		err = batcher.Enqueue(op2)
		assert.NoError(t, err, "expecting no error on enqueue")
		err = batcher.Start()
		assert.NoError(t, err, "expecting no error on start")
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 1100, max, "expecting the request to be the sum of the operations")
	})

	t.Run("ensure operation costs result in target", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
		batcher := gobatcher.NewBatcher().
			WithRateLimiter(res).
			WithFlushInterval(1 * time.Millisecond)
		var mu sync.Mutex
		var max int
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.TargetEvent:
				mu.Lock()
				defer mu.Unlock()
				if val > max {
					max = val
				}
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			time.Sleep(400 * time.Millisecond)
		})
		var err error
		op1 := gobatcher.NewOperation(watcher, 800, struct{}{}, false)
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "expecting no error on enqueue")
		op2 := gobatcher.NewOperation(watcher, 300, struct{}{}, false)
		err = batcher.Enqueue(op2)
		assert.NoError(t, err, "expecting no error on enqueue")
		err = batcher.Start()
		assert.NoError(t, err, "expecting no error on start")
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
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
			batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case gobatcher.PauseEvent:
					paused = time.Now()
					wg.Done()
				case gobatcher.ResumeEvent:
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
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.PauseEvent:
				paused = time.Now()
				wg.Done()
			case gobatcher.ResumeEvent:
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
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.PauseEvent:
				paused = time.Now()
				wg.Done()
			case gobatcher.ResumeEvent:
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
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.ResumeEvent:
				resumed = true
				wg.Done()
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			assert.True(t, resumed, "all batches should be raised after resume")
			wg.Done()
		})
		batcher.Pause()
		op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
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
		var op1, op2, op3 gobatcher.IOperation
		var count uint32 = 0
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			atomic.AddUint32(&count, uint32(len(batch)))
			switch len(batch) {
			case 1:
				assert.Equal(t, op2, batch[0], "expect that the batch has op2")
			case 2:
				assert.Equal(t, op1, batch[0], "expect that the batch has op1 and op3")
				assert.Equal(t, op3, batch[1], "expect that the batch has op1 and op3")
			}
			wg.Done()
		})
		op1 = gobatcher.NewOperation(watcher, 100, struct{}{}, true)
		err = batcher.Enqueue(op1)
		assert.NoError(t, err, "not expecting an enqueue error")
		op2 = gobatcher.NewOperation(watcher, 100, struct{}{}, false)
		err = batcher.Enqueue(op2)
		assert.NoError(t, err, "not expecting an enqueue error")
		op3 = gobatcher.NewOperation(watcher, 100, struct{}{}, true)
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
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			atomic.AddUint32(&count, 1)
			assert.Equal(t, 3, len(batch), "expect batches to have 3 operations each")
		}).WithMaxBatchSize(3)
		for i := 0; i < 9; i++ {
			op := gobatcher.NewOperation(watcher, 100, struct{}{}, true)
			err := batcher.Enqueue(op)
			assert.NoError(t, err, "not expecting an enqueue error")
		}
		err := batcher.Start()
		assert.NoError(t, err, "not expecting an startup error")
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, uint32(3), atomic.LoadUint32(&count), "expect 3 batches")
	})

}

func TestBatcherStop(t *testing.T) {

	t.Run("stop emits shutdown", func(t *testing.T) {
		batcher := gobatcher.NewBatcher()
		done := make(chan bool)
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.ShutdownEvent:
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
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.ShutdownEvent:
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
		var count uint32
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.ShutdownEvent:
				atomic.AddUint32(&count, 1)
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
		assert.Equal(t, uint32(1), atomic.LoadUint32(&count), "expecting only a single shutdown")
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
			watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
				atomic.AddUint32(&count, uint32(len(batch)))
			})
			for i := 0; i < d.enqueue; i++ {
				op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
				err := batcher.Enqueue(op)
				assert.NoError(t, err, "not expecting an enqueue error")
			}
			err := batcher.Start()
			assert.NoError(t, err, "not expecting a start error")
			time.Sleep(d.wait)
			assert.Equal(t, d.expect, atomic.LoadUint32(&count), "expecting %v operations to be completed given the %v interval and capacity for only a single operation", d.interval, d.expect)
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
			batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case gobatcher.RequestEvent:
					atomic.AddUint32(&count, 1)
				}
			})
			watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {})
			op := gobatcher.NewOperation(watcher, 800, struct{}{}, false)
			err := batcher.Enqueue(op)
			assert.NoError(t, err, "not expecting an enqueue error")
			err = batcher.Start()
			assert.NoError(t, err, "not expecting a start error")
			time.Sleep(d.wait)
			assert.Equal(t, d.expect, atomic.LoadUint32(&count), "expecting %v capacity requests given the %v interval and capacity for only a single operation", d.interval, d.expect)
		})
	}

	t.Run("ensure long-running operations are still marked done (watcher)", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond)
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			// NOTE: simulate a long-running operation
			time.Sleep(100 * time.Millisecond)
		}).WithMaxOperationTime(10 * time.Millisecond)
		op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
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

	t.Run("ensure long-running operations are still marked done (batcher)", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond).
			WithMaxOperationTime(10 * time.Millisecond)
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			// NOTE: simulate a long-running operation
			time.Sleep(100 * time.Millisecond)
		})
		op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
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

	t.Run("ensure abandoned operations are not marked done before 1m default", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond)
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			// NOTE: simulate a long-running operation
			time.Sleep(400 * time.Millisecond)
		})
		op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
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
		var passed, failed uint32
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AuditPassEvent:
				atomic.AddUint32(&passed, 1)
			case gobatcher.AuditFailEvent:
				atomic.AddUint32(&failed, 1)
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {})
		op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
		err := batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		err = batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(10 * time.Millisecond)
		assert.Greater(t, atomic.LoadUint32(&passed), uint32(0), "expecting audit-pass because done() was called before max-operation-time (1m default)")
		assert.Equal(t, uint32(0), atomic.LoadUint32(&failed), "expecting no audit-fail messages")
	})

	t.Run("demonstrate an audit-fail", func(t *testing.T) {
		// NOTE: this sets a batcher max-op-time to 1ms and a watcher max-op-time to 1m allowing for the target to be around longer than it thinks it should be
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond).
			WithAuditInterval(1 * time.Millisecond).
			WithMaxOperationTime(1 * time.Millisecond)
		var failed uint32
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AuditFailEvent:
				atomic.AddUint32(&failed, 1)
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			time.Sleep(20 * time.Millisecond)
		}).WithMaxOperationTime(1 * time.Minute)
		op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
		err := batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		err = batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(10 * time.Millisecond)
		assert.Greater(t, atomic.LoadUint32(&failed), uint32(0), "expecting an audit failure because done() was not called and max-operation-time was exceeded")
	})

	t.Run("demonstrate an audit-skip", func(t *testing.T) {
		batcher := gobatcher.NewBatcher().
			WithFlushInterval(1 * time.Millisecond).
			WithAuditInterval(1 * time.Millisecond)
		var skipped uint32
		batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AuditSkipEvent:
				atomic.AddUint32(&skipped, 1)
			}
		})
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
			time.Sleep(20 * time.Millisecond)
		})
		var err error
		op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
		err = batcher.Enqueue(op)
		assert.NoError(t, err, "not expecting an enqueue error")
		err = batcher.Start()
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(10 * time.Millisecond)
		assert.Greater(t, atomic.LoadUint32(&skipped), uint32(0), "expect that something in the buffer but max-operation-time is still valid, will cause skips")
	})

}

func TestManualFlush(t *testing.T) {
	var err error
	batcher := gobatcher.NewBatcher().WithFlushInterval(10 * time.Minute)
	err = batcher.Start()
	assert.NoError(t, err, "not expecting a start error")
	completed := make(chan bool, 1)
	watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
		completed <- true
	})
	op := gobatcher.NewOperation(watcher, 100, struct{}{}, false)
	err = batcher.Enqueue(op)
	assert.NoError(t, err, "not expecting an enqueue error")
	batcher.Flush()
	select {
	case <-completed:
	case <-time.After(1 * time.Second):
		assert.Fail(t, "expected the manual flush to have completed the batch before the timeout")
	}
}
