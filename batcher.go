package batcher

// TODO fix problem where partitions are getting wiped when inc/dec

import (
	"sync"
	"time"
)

const (
	batcherPhaseUninitialized = iota
	batcherPhaseStarted
	batcherPhasePaused
	batcherPhaseStopped
)

type IBatcher interface {
	ieventer
	WithRateLimiter(rl IRateLimiter) IBatcher
	WithFlushInterval(val time.Duration) IBatcher
	WithCapacityInterval(val time.Duration) IBatcher
	WithAuditInterval(val time.Duration) IBatcher
	WithMaxOperationTime(val time.Duration) IBatcher
	WithPauseTime(val time.Duration) IBatcher
	WithErrorOnFullBuffer() IBatcher
	WithEmitBatch() IBatcher
	Enqueue(op IOperation) error
	Pause()
	Flush()
	OperationsInBuffer() uint32
	NeedsCapacity() uint32
	Start() (err error)
	Stop()
}

type Batcher struct {
	eventer

	// configuration items that should not change after Start()
	ratelimiter       IRateLimiter
	flushInterval     time.Duration
	capacityInterval  time.Duration
	auditInterval     time.Duration
	maxOperationTime  time.Duration
	pauseTime         time.Duration
	errorOnFullBuffer bool
	emitBatch         bool

	// used for internal operations
	buffer chan IOperation
	pause  chan bool
	flush  chan bool

	// manage the phase
	phaseMutex sync.Mutex
	phase      int
	shutdown   sync.WaitGroup
	stop       chan bool

	// target needs to be threadsafe and changes frequently
	targetMutex sync.RWMutex
	target      uint32
}

// This method creates a new Batcher. Generally you should have 1 Batcher per datastore. Commonly after calling NewBatcher() you will chain
// some WithXXXX methods, for instance... `NewBatcher().WithRateLimiter(limiter)`.
func NewBatcher() IBatcher {
	return NewBatcherWithBuffer(10000)
}

func NewBatcherWithBuffer(maxBufferSize uint32) IBatcher {
	r := &Batcher{}
	r.buffer = make(chan IOperation, maxBufferSize)
	r.pause = make(chan bool, 1)
	r.flush = make(chan bool, 1)
	return r
}

// Use AzureSharedResource or ProvisionedResource as a rate limiter with Batcher to throttle the requests made against a datastore. This is
// optional; the default behavior does not rate limit.
func (r *Batcher) WithRateLimiter(rl IRateLimiter) IBatcher {
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseUninitialized {
		panic(InitializationOnlyError{})
	}
	r.ratelimiter = rl
	return r
}

// The FlushInterval determines how often the processing loop attempts to flush buffered Operations. The default is `100ms`. If a rate limiter
// is being used, the interval determines the capacity that each flush has to work with. For instance, with the default 100ms and 10,000
// available capacity, there would be 10 flushes per second, each dispatching one or more batches of Operations that aim for 1,000 total
// capacity. If no rate limiter is used, each flush will attempt to empty the buffer.
func (r *Batcher) WithFlushInterval(val time.Duration) IBatcher {
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseUninitialized {
		panic(InitializationOnlyError{})
	}
	r.flushInterval = val
	return r
}

// The CapacityInterval determines how often the processing loop asks the rate limiter for capacity by calling GiveMe(). The default is
// `100ms`. The Batcher asks for capacity equal to every Operation's cost that has not been marked done. In other words, when you Enqueue()
// an Operation it increments a target based on cost. When you call done() on a batch (or the MaxOperationTime is exceeded), the target is
// decremented by the cost of all Operations in the batch. If there is no rate limiter attached, this interval does nothing.
func (r *Batcher) WithCapacityInterval(val time.Duration) IBatcher {
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseUninitialized {
		panic(InitializationOnlyError{})
	}
	r.capacityInterval = val
	return r
}

// The AuditInterval determines how often the target capacity is audited to ensure it still seems legitimate. The default is `10s`. The
// target capacity is the amount of capacity the Batcher thinks it needs to process all outstanding Operations. Only atomic operatios are
// performed on the target and there are other failsafes such as MaxOperationTime, however, since it is critical that the target capacity
// be correct, this is one final failsafe to ensure the Batcher isn't asking for the wrong capacity. Generally you should leave this set
// at the default.
func (r *Batcher) WithAuditInterval(val time.Duration) IBatcher {
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseUninitialized {
		panic(InitializationOnlyError{})
	}
	r.auditInterval = val
	return r
}

// The MaxOperationTime determines how long Batcher waits until marking a batch done after releasing it to the Watcher. The default is `1m`.
// You should always call the done() func when your batch has completed processing instead of relying on MaxOperationTime. The MaxOperationTime
// on Batcher will be superceded by MaxOperationTime on Watcher if provided.
func (r *Batcher) WithMaxOperationTime(val time.Duration) IBatcher {
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseUninitialized {
		panic(InitializationOnlyError{})
	}
	r.maxOperationTime = val
	return r
}

// The PauseTime determines how long Batcher suspends the processing loop once Pause() is called. The default is `500ms`. Typically, Pause()
// is called because errors are being received from the datastore such as TooManyRequests or Timeout. Pausing hopefully allows the datastore
// to catch up without making the problem worse.
func (r *Batcher) WithPauseTime(val time.Duration) IBatcher {
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseUninitialized {
		panic(InitializationOnlyError{})
	}
	r.pauseTime = val
	return r
}

// Setting this option changes Enqueue() such that it throws an error if the buffer is full. Normal behavior is for the Enqueue() func to
// block until it is able to add to the buffer.
func (r *Batcher) WithErrorOnFullBuffer() IBatcher {
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseUninitialized {
		panic(InitializationOnlyError{})
	}
	r.errorOnFullBuffer = true
	return r
}

// DO NOT SET THIS IN PRODUCTION. For unit tests, it may be beneficial to raise an event for each batch of operations.
func (r *Batcher) WithEmitBatch() IBatcher {
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseUninitialized {
		panic(InitializationOnlyError{})
	}
	r.emitBatch = true
	return r
}

func (r *Batcher) applyDefaults() {
	if r.flushInterval <= 0 {
		r.flushInterval = 100 * time.Millisecond
	}
	if r.capacityInterval <= 0 {
		r.capacityInterval = 100 * time.Millisecond
	}
	if r.auditInterval <= 0 {
		r.auditInterval = 10 * time.Second
	}
	if r.maxOperationTime <= 0 {
		r.maxOperationTime = 1 * time.Minute
	}
	if r.pauseTime <= 0 {
		r.pauseTime = 500 * time.Millisecond
	}
}

// Call this method to add an Operation into the buffer.
func (r *Batcher) Enqueue(op IOperation) error {

	// ensure an operation was provided
	if op == nil {
		return NoOperationError{}
	}

	// ensure there is a watcher associated with the call
	watcher := op.Watcher()
	if op.Watcher() == nil {
		return NoWatcherError{}
	}

	// ensure the cost doesn't exceed max capacity
	if r.ratelimiter != nil && op.Cost() > r.ratelimiter.MaxCapacity() {
		return TooExpensiveError{}
	}

	// ensure there are not too many attempts
	maxAttempts := watcher.MaxAttempts()
	if maxAttempts > 0 && op.Attempt() >= maxAttempts {
		return TooManyAttemptsError{}
	}

	// increment the target
	r.incTarget(int(op.Cost()))

	// put into the buffer
	if r.errorOnFullBuffer {
		select {
		case r.buffer <- op:
			// successfully queued
		default:
			return BufferFullError{}
		}
	} else {
		r.buffer <- op
	}

	return nil
}

// Call this method when your datastore is throwing transient errors. This pauses the processing loop to ensure that you are not flooding
// the datastore with additional data it cannot process making the situation worse.
func (r *Batcher) Pause() {

	// ensure pausing only happens when it is running
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseStarted {
		// simply ignore an invalid pause
		return
	}

	// pause
	select {
	case r.pause <- true:
		// successfully set the pause
	default:
		// pause was already set
	}

	// switch to paused phase
	r.phase = batcherPhasePaused

}

func (r *Batcher) resume() {
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase == batcherPhasePaused {
		r.phase = batcherPhaseStarted
	}
}

// Call this method to manually flush as if the flushInterval were triggered.
func (r *Batcher) Flush() {

	// flush
	select {
	case r.flush <- true:
		// successfully set the flush
	default:
		// flush was already set
	}

}

// This tells you how many operations are still in the buffer. This does not include operations that have been sent back to the Watcher as part
// of a batch for processing.
func (r *Batcher) OperationsInBuffer() uint32 {
	return uint32(len(r.buffer))
}

// This tells you how much capacity the Batcher believes it needs to process everything outstanding. Outstanding operations include those in
// the buffer and operations and any that have been sent as a batch but not marked done yet.
func (r *Batcher) NeedsCapacity() uint32 {
	return r.getTarget()
}

func (r *Batcher) getTarget() uint32 {
	r.targetMutex.RLock()
	defer r.targetMutex.RUnlock()
	return r.target
}

func (r *Batcher) trySetTargetToZero() bool {
	r.targetMutex.Lock()
	defer r.targetMutex.Unlock()
	if r.target > 0 {
		r.target = 0
		return true
	} else {
		return false
	}
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

// Call this method to start the processing loop. The processing loop requests capacity at the CapacityInterval, organizes operations into
// batches at the FlushInterval, and audits the capacity target at the AuditInterval.
func (r *Batcher) Start() (err error) {

	// only allow one phase at a time
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase != batcherPhaseUninitialized {
		err = ImproperOrderError{}
		return
	}

	// ensure buffer was provisioned
	if r.buffer == nil {
		err = BufferNotAllocated{}
		return
	}

	// apply defaults
	r.applyDefaults()

	// start the timers
	capacityTimer := time.NewTicker(r.capacityInterval)
	flushTimer := time.NewTicker(r.flushInterval)
	auditTimer := time.NewTicker(r.auditInterval)

	// define the func for flushing a batch
	var lastFlushWithRecords time.Time
	flush := func(watcher IWatcher, batch []IOperation) {
		if len(batch) > 0 {
			if r.emitBatch {
				r.emit(BatchEvent, len(batch), "", batch)
			}
			lastFlushWithRecords = time.Now()
			go func() {

				// increment an attempt
				for _, op := range batch {
					op.MakeAttempt()
				}

				// process the batch
				waitForDone := make(chan struct{})
				go func() {
					defer close(waitForDone)
					watcher.ProcessBatch(batch)
				}()

				// the batch is "done" when the ProcessBatch func() finishes or the maxOperationTime is exceeded
				maxOperationTime := r.maxOperationTime
				if watcher.MaxOperationTime() > 0 {
					maxOperationTime = watcher.MaxOperationTime()
				}
				select {
				case <-waitForDone:
				case <-time.After(maxOperationTime):
				}

				// decrement target
				var total int = 0
				for _, op := range batch {
					total += int(op.Cost())
				}
				r.incTarget(-total)

			}()
		}
	}

	// prepare for shutdown
	r.shutdown.Add(1)
	r.stop = make(chan bool)

	// process
	go func() {

		// shutdown
		defer func() {
			capacityTimer.Stop()
			flushTimer.Stop()
			auditTimer.Stop()
			close(r.buffer)
			r.emit(ShutdownEvent, 0, "", nil)
			r.shutdown.Done()
		}()

		// loop
		for {
			select {

			case <-r.stop:
				// no more writes; abort
				return

			case <-r.pause:
				// pause; typically this is requested because there is too much pressure on the datastore
				r.emit(PauseEvent, int(r.pauseTime.Milliseconds()), "", nil)
				time.Sleep(r.pauseTime)
				r.resume()
				r.emit(ResumeEvent, 0, "", nil)

			case <-auditTimer.C:
				// ensure that if the buffer is empty and everything should have been flushed, that target is set to 0
				if len(r.buffer) < 1 && time.Since(lastFlushWithRecords) > r.maxOperationTime {
					if r.trySetTargetToZero() {
						r.emit(AuditFailEvent, 0, "an audit revealed that the target should be zero but was not.", nil)
					} else {
						r.emit(AuditPassEvent, 0, "", nil)
					}
				} else {
					r.emit(AuditSkipEvent, 0, "", nil)
				}

			case <-capacityTimer.C:
				// ask for capacity
				if r.ratelimiter != nil {
					request := r.NeedsCapacity()
					r.emit(RequestEvent, int(request), "", nil)
					r.ratelimiter.GiveMe(request)
				}

			case <-flushTimer.C:
				r.Flush()

			case <-r.flush:
				// flush a percentage of the capacity (by default 10%)

				// determine the capacity
				enforceCapacity := r.ratelimiter != nil
				var capacity uint32
				if enforceCapacity {
					capacity += uint32(float64(r.ratelimiter.Capacity()) / 1000.0 * float64(r.flushInterval.Milliseconds()))
				}

				// if there are operations in the buffer, go up to the capacity
				batches := make(map[IWatcher][]IOperation)
				var consumed uint32 = 0
			Fill:
				for {
					// NOTE: by requiring consumed to be higher than capacity we ensure the process always dispatches at least 1 operation
					if enforceCapacity && (capacity == 0 || consumed > capacity) {
						break Fill
					}
					select {
					case op := <-r.buffer:

						// process immediately or add to a batch
						watcher := op.Watcher()
						if op == nil {
							// op can be nil when the buffer is closed
						} else if op.IsBatchable() {
							consumed += op.Cost()
							batch := batches[watcher]
							batch = append(batch, op)
							max := watcher.MaxBatchSize()
							if max > 0 && len(batch) >= int(max) {
								flush(watcher, batch)
								batches[watcher] = nil
							} else {
								batches[watcher] = batch
							}
						} else {
							consumed += op.Cost()
							flush(watcher, []IOperation{op})
						}

					default:
						// there is nothing in the buffer
						break Fill
					}
				}

				// flush all batches that were seen
				for watcher, batch := range batches {
					flush(watcher, batch)
				}

			}
		}

	}()

	// end starting
	r.phase = batcherPhaseStarted

	return
}

// Call this method to stop the processing loop. You may not restart after stopping.
func (r *Batcher) Stop() {

	// only allow one phase at a time
	r.phaseMutex.Lock()
	defer r.phaseMutex.Unlock()
	if r.phase == batcherPhaseStopped {
		// NOTE: there should be no need for callers to handle errors at Stop(), we will just ignore them
		return
	}

	// signal the stop
	if r.stop != nil {
		close(r.stop)
	}
	r.shutdown.Wait()

	// update the phase
	r.phase = batcherPhaseStopped

}
