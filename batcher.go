package batcher

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Watcher struct {
	operations   []*Operation
	maxAttempts  uint32
	maxBatchSize uint32
	onReady      func(ops []*Operation, done func())
}

func NewWatcher(onReady func(ops []*Operation, done func())) *Watcher {
	return &Watcher{
		onReady: onReady,
	}
}

func (w *Watcher) WithMaxAttempts(val uint32) *Watcher {
	w.maxAttempts = val
	return w
}

func (w *Watcher) WithMaxBatchSize(val uint32) *Watcher {
	w.maxBatchSize = val
	return w
}

func (w *Watcher) len() uint32 {
	return uint32(len(w.operations))
}

func (w *Watcher) full() bool {
	return w.maxBatchSize > 0 && w.len() >= w.maxBatchSize
}

func (w *Watcher) clear() {
	w.operations = nil
}

type Operation struct {
	cost    uint32
	attempt uint32
	batch   bool
	watcher *Watcher
	payload interface{}
}

func NewOperation(watcher *Watcher, cost uint32, payload interface{}) *Operation {
	return &Operation{
		watcher: watcher,
		cost:    cost,
		payload: payload,
	}
}

func (o *Operation) AllowBatch() *Operation {
	o.batch = true
	return o
}

func (o *Operation) Payload() interface{} {
	return o.payload
}

func (o *Operation) Attempt() uint32 {
	return atomic.LoadUint32(&o.attempt)
}

func (o *Operation) makeAttempt() {
	atomic.AddUint32(&o.attempt, 1)
}

type Batcher struct {
	eventer
	sharedResource   SharedResource
	maxBufferSize    uint32
	flushInterval    time.Duration
	capacityInterval time.Duration
	maxOperationTime time.Duration
	buffer           chan *Operation
	stop             chan bool
	pause            chan bool
	shutdown         sync.WaitGroup
	targetMutex      sync.RWMutex
	target           uint32
}

func NewBatcher() *Batcher {
	return &Batcher{}
}

func (r *Batcher) WithSharedResource(res SharedResource) *Batcher {
	r.sharedResource = res
	return r
}

func (r *Batcher) WithMaxBufferSize(val uint32) *Batcher {
	r.maxBufferSize = val
	r.buffer = make(chan *Operation, r.maxBufferSize)
	return r
}

func (r *Batcher) WithFlushInterval(val time.Duration) *Batcher {
	r.flushInterval = val
	return r
}

func (r *Batcher) WithCapacityInterval(val time.Duration) *Batcher {
	r.capacityInterval = val
	return r
}

func (r *Batcher) WithMaxOperationTime(val time.Duration) *Batcher {
	r.maxOperationTime = val
	return r
}

func (r *Batcher) applyDefaults() {
	if r.maxBufferSize == 0 {
		r.maxBufferSize = 10000
	}
	if r.flushInterval == 0 {
		r.flushInterval = 100 * time.Millisecond
	}
	if r.capacityInterval == 0 {
		r.capacityInterval = 100 * time.Millisecond
	}
	if r.maxOperationTime == 0 {
		r.maxOperationTime = 1 * time.Minute
	}
}

type NoWatcherError struct {
	Operation *Operation
}

func (e NoWatcherError) Error() string {
	return "the operation must have a watcher assigned."
}

type TooManyAttemptsError struct {
	Operation *Operation
}

func (e TooManyAttemptsError) Error() string {
	return fmt.Sprintf("the operation was already tried %v times, which is the maximum number of attempts.", e.Operation.Attempt())
}

type TooExpensiveError struct {
	Operation *Operation
}

func (e TooExpensiveError) Error() string {
	return fmt.Sprintf("the operation costs %v which is more expensive than the maximum capacity.", e.Operation.cost)
}

func (r *Batcher) Enqueue(op *Operation) error {

	// ensure there is a watcher associated with the call
	if op.watcher == nil {
		return NoWatcherError{Operation: op}
	}

	// ensure the cost doesn't exceed max capacity
	if r.sharedResource != nil && op.cost > r.sharedResource.MaxCapacity() {
		return TooExpensiveError{Operation: op}
	}

	// ensure there are not too many attempts
	if op.watcher.maxAttempts > 0 && op.Attempt() >= op.watcher.maxAttempts {
		return TooManyAttemptsError{Operation: op}
	}

	// increment the target
	r.incTarget(int(op.cost))

	// put into the buffer
	r.buffer <- op

	return nil
}

func (r *Batcher) Pause() {
	select {
	case r.pause <- true:
		// successfully set the pause
	default:
		// pause was already set
	}
}

func (r *Batcher) OperatorsInBuffer() uint32 {
	return uint32(len(r.buffer))
}

func (r *Batcher) NeedsCapacity() uint32 {
	return atomic.LoadUint32(&r.target)
}

func (r *Batcher) getTarget() uint32 {
	r.targetMutex.RLock()
	defer r.targetMutex.RUnlock()
	return r.target
}

func (r *Batcher) setTarget(val uint32) {
	r.targetMutex.Lock()
	defer r.targetMutex.Unlock()
	r.target = val
}

func (r *Batcher) incTarget(val int) {
	r.targetMutex.Lock()
	defer r.targetMutex.Unlock()
	if val < 0 && r.target >= uint32(-val) {
		r.target += uint32(val)
	} else if val < 0 {
		r.target = 0
	} else if val > 0 {
		r.target += uint32(val)
	} // else is val=0, do nothing
}

func (r *Batcher) Start() *Batcher {
	r.applyDefaults()

	// setup
	r.pause = make(chan bool, 1)

	// TODO update the documentation

	// start the timers
	capacityTimer := time.NewTicker(r.capacityInterval)
	flushTimer := time.NewTicker(r.flushInterval)

	// define the func for flushing a batch
	call := func(watcher *Watcher, operations []*Operation) {
		if watcher.onReady != nil && len(operations) > 0 {
			go func() {

				// increment an attempt
				for _, op := range operations {
					op.makeAttempt()
				}

				// NOTE: done() is called by the user or happens after maxOperationTime
				waitForDone := make(chan struct{})
				watcher.onReady(operations, func() {
					close(waitForDone)
				})
				select {
				case <-waitForDone:
				case <-time.After(r.maxOperationTime):
				}

				// decrement target
				var total int = 0
				for _, op := range operations {
					total += int(op.cost)
				}
				r.incTarget(-total)

			}()
		}
	}
	flush := func(watcher *Watcher) {
		call(watcher, watcher.operations)
		watcher.clear()
	}

	// prepare for shutdown
	r.shutdown = sync.WaitGroup{}
	r.shutdown.Add(1)
	r.stop = make(chan bool, 1)

	// process
	go func() {
		lastFlushWithRecords := time.Now()

		// shutdown
		defer func() {
			capacityTimer.Stop()
			flushTimer.Stop()
			close(r.buffer)
			r.emit("shutdown", 0, nil)
			r.shutdown.Done()
		}()

		// loop
		var count int64 = 0
		for {
			select {

			case <-r.stop:
				// no more writes; abort
				return

			case <-r.pause:
				// pause; typically this is requested because there is too much pressure on the datastore
				// TODO: make this configurable
				r.emit("pause", 500, nil)
				time.Sleep(500 * time.Millisecond)

			case <-capacityTimer.C:
				// ask for capacity
				if r.sharedResource != nil {
					request := r.NeedsCapacity()
					r.sharedResource.GiveMe(request)
				}

			case <-flushTimer.C:
				// flush a percentage of the capacity (by default 10%)

				// determine the capacity
				enforceCapacity := r.sharedResource != nil
				var capacity uint32
				if enforceCapacity {
					capacity += uint32(float64(r.sharedResource.Capacity()) / 1000.0 * float64(r.flushInterval.Milliseconds()))
				}

				// if there are operations in the buffer, go up to the capacity
				batches := make(map[*Watcher]bool)
				var consumed uint32 = 0
			Fill:
				for {
					if enforceCapacity && consumed >= capacity {
						break Fill
					}
					select {
					case op := <-r.buffer:
						lastFlushWithRecords = time.Now()

						// process immediately or add to a batch
						if op == nil {
							// op can be nil when the buffer is closed
						} else if op.batch {
							consumed += op.cost
							if _, ok := batches[op.watcher]; !ok {
								batches[op.watcher] = true
							}
							op.watcher.operations = append(op.watcher.operations, op)
							if op.watcher.full() {
								flush(op.watcher)
							}
						} else {
							consumed += op.cost
							call(op.watcher, []*Operation{op})
						}

					default:
						// there is nothing in the buffer
						break Fill
					}
				}

				// flush all batches that were seen
				for batch := range batches {
					flush(batch)
				}
				count++

				// implement a failsafe against requesting a target without having any operations
				if time.Since(lastFlushWithRecords) > r.maxOperationTime && r.NeedsCapacity() > 0 {
					r.setTarget(0)
				}

			}
		}

	}()

	return r
}

func (r *Batcher) Stop() {
	r.stop <- true
	r.shutdown.Wait()
}
