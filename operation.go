package batcher

import "sync/atomic"

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

func (o *Operation) WithBatching(val bool) *Operation {
	o.batch = val
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
