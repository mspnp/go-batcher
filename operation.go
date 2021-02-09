package batcher

import "sync/atomic"

type IOperation interface {
	Payload() interface{}
	Attempt() uint32
	Cost() uint32
	Watcher() IWatcher
	IsBatchable() bool
	MakeAttempt()
}

type Operation struct {
	cost      uint32
	attempt   uint32
	batchable bool
	watcher   IWatcher
	payload   interface{}
}

func NewOperation(watcher IWatcher, cost uint32, payload interface{}, batchable bool) IOperation {
	return &Operation{
		watcher:   watcher,
		cost:      cost,
		payload:   payload,
		batchable: batchable,
	}
}

func (o *Operation) Payload() interface{} {
	return o.payload
}

func (o *Operation) Attempt() uint32 {
	return atomic.LoadUint32(&o.attempt)
}

func (o *Operation) MakeAttempt() {
	atomic.AddUint32(&o.attempt, 1)
}

func (o *Operation) Cost() uint32 {
	return o.cost
}

func (o *Operation) Watcher() IWatcher {
	return o.watcher
}

func (o *Operation) IsBatchable() bool {
	return o.batchable
}
