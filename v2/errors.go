package batcher

type UndefinedLeaseManagerError struct{}

func (e UndefinedLeaseManagerError) Error() string {
	return "a lease manager must be assigned."
}

type NoWatcherError struct{}

func (e NoWatcherError) Error() string {
	return "the operation must have a watcher assigned."
}

type TooManyAttemptsError struct{}

func (e TooManyAttemptsError) Error() string {
	return "the operation exceeded the maximum number of attempts."
}

type TooExpensiveError struct{}

func (e TooExpensiveError) Error() string {
	return "the operation costs more than the maximum capacity."
}

type BufferFullError struct{}

func (e BufferFullError) Error() string {
	return "the buffer is full, try to enqueue again later."
}

type BufferNotAllocated struct{}

func (e BufferNotAllocated) Error() string {
	return "the buffer was never allocated, make sure to create a Batcher by calling NewBatcher()."
}

type ImproperOrderError struct{}

func (e ImproperOrderError) Error() string {
	return "methods can only be called in this order Start() > Stop()."
}

type InitializationOnlyError struct{}

func (e InitializationOnlyError) Error() string {
	return "this property can only be set before Start() is called."
}

type NoOperationError struct{}

func (e NoOperationError) Error() string {
	return "no operation was provided."
}
