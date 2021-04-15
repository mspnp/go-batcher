package batcher

import "errors"

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
	InitializationOnlyError      = errors.New("this property can only be set before Start() is called.")
)

type PartitionsOutOfRangeError struct {
	MaxCapacity    uint32
	Factor         uint32
	PartitionCount int
}

func (e PartitionsOutOfRangeError) Error() string {
	return "you must have between 1 and 500 partitions."
}
