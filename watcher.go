package batcher

import "time"

type WatcherProcessFunc func(ops []IOperation, done func())

type IWatcher interface {
	WithMaxAttempts(val uint32) IWatcher
	WithMaxBatchSize(val uint32) IWatcher
	WithMaxOperationTime(val time.Duration) IWatcher
	MaxAttempts() uint32
	MaxBatchSize() uint32
	MaxOperationTime() time.Duration
	ProcessBatch(ops []IOperation, done func())
}

type Watcher struct {
	maxAttempts      uint32
	maxBatchSize     uint32
	maxOperationTime time.Duration
	onReady          WatcherProcessFunc
}

func NewWatcher(onReady func(ops []IOperation, done func())) IWatcher {
	return &Watcher{
		onReady: onReady,
	}
}

func (w *Watcher) WithMaxAttempts(val uint32) IWatcher {
	w.maxAttempts = val
	return w
}

func (w *Watcher) WithMaxBatchSize(val uint32) IWatcher {
	w.maxBatchSize = val
	return w
}

func (w *Watcher) WithMaxOperationTime(val time.Duration) IWatcher {
	w.maxOperationTime = val
	return w
}

func (w *Watcher) MaxAttempts() uint32 {
	return w.maxAttempts
}

func (w *Watcher) MaxBatchSize() uint32 {
	return w.maxBatchSize
}

func (w *Watcher) MaxOperationTime() time.Duration {
	return w.maxOperationTime
}

func (w *Watcher) ProcessBatch(ops []IOperation, done func()) {
	w.onReady(ops, done)
}
