package batcher

import "errors"

const (
	AuditMsgFailureOnTargetAndInflight = "an audit revealed that the target and inflight should both be zero but neither was."
	AuditMsgFailureOnTarget            = "an audit revealed that the target should be zero but was not."
	AuditMsgFailureOnInflight          = "an audit revealed that inflight should be zero but was not."
)

var (
	UndefinedLeaseManagerError   = errors.New("a lease manager must be assigned.")
	UndefinedSharedCapacityError = errors.New("you must define a SharedCapacity.")
	NoWatcherError               = errors.New("the operation must have a watcher assigned.")
	TooManyAttemptsError         = errors.New("the operation exceeded the maximum number of attempts.")
	TooExpensiveError            = errors.New("the operation costs more than the maximum capacity.")
	BufferFullError              = errors.New("the buffer is full, try to enqueue again later.")
	BufferNotAllocated           = errors.New("the buffer was never allocated, make sure to create a Batcher by calling NewBatcher().")
	ImproperOrderError           = errors.New("methods can only be called in this order Start() > Stop().")
	NoOperationError             = errors.New("no operation was provided.")
)

type PartitionsOutOfRangeError struct {
	MaxCapacity    uint32
	Factor         uint32
	PartitionCount int
}

func (e PartitionsOutOfRangeError) Error() string {
	return "you must have between 1 and 500 partitions."
}
