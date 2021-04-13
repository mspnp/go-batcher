package batcher

type repeater struct {
	parent ieventer
}

func (r *repeater) emit(event string, val int, msg string, metadata interface{}) {
	r.parent.emit(event, val, msg, metadata)
}
