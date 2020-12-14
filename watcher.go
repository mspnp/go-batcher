package batcher

import "time"

type Watcher struct {
	operations       []*Operation
	maxAttempts      uint32
	maxBatchSize     uint32
	maxOperationTime time.Duration
	onReady          func(ops []*Operation, done func())
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

func (w *Watcher) WithMaxOperationTime(val time.Duration) *Watcher {
	w.maxOperationTime = val
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
