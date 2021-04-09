package batcher

import (
	"errors"
	"sync"
)

// TODO add comments

type IBuffer interface {
	Size() uint32
	Max() uint32
	Top() IOperation
	Skip() IOperation
	Remove() IOperation
	Enqueue(IOperation, bool) error
	Clear()
}

type buffer struct {
	lock    *sync.Mutex
	notFull *sync.Cond
	size    uint32
	max     uint32
	head    *links
	tail    *links
	cursor  *links
}

type links struct {
	prv *links
	op  IOperation
	nxt *links
}

func NewBuffer(max uint32) IBuffer {
	lock := &sync.Mutex{}
	return &buffer{
		lock:    lock,
		notFull: sync.NewCond(lock),
		max:     max,
	}
}

func (b *buffer) Size() uint32 {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.size
}

func (b *buffer) Max() uint32 {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.max
}

func (b *buffer) Top() IOperation {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.cursor = b.head
	if b.cursor == nil {
		return nil
	}
	return b.cursor.op
}

func (b *buffer) Skip() IOperation {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.cursor == nil {
		return nil
	}
	b.cursor = b.cursor.nxt
	if b.cursor == nil {
		return nil
	}
	return b.cursor.op
}

func (b *buffer) Remove() IOperation {
	b.lock.Lock()
	defer b.lock.Unlock()

	switch {
	case b.cursor == nil:
		return nil
	case b.cursor.prv != nil && b.cursor.nxt != nil:
		// cursor is at neither a head nor a tail
		b.cursor.prv.nxt = b.cursor.nxt
		b.cursor.nxt.prv = b.cursor.prv
		b.cursor = b.cursor.nxt
	case b.cursor.prv != nil:
		// cursor is the tail
		b.cursor.prv.nxt = nil
		b.tail = b.cursor.prv
		b.cursor = nil
	case b.cursor.nxt != nil:
		// cursor is the head
		b.cursor.nxt.prv = nil
		b.head = b.cursor.nxt
		b.cursor = b.cursor.nxt
	default:
		// cursor is both head and tail
		b.head = nil
		b.tail = nil
		b.cursor = nil
	}

	b.size--
	if b.cursor == nil {
		return nil
	}
	return b.cursor.op
}

func (b *buffer) Enqueue(op IOperation, errorOnFull bool) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	for b.size >= b.max {
		if errorOnFull {
			return BufferFullError{}
		}
		b.notFull.Wait()
	}

	switch {
	case b.head == nil:
		link := &links{op: op}
		b.head = link
		b.tail = link
	case b.tail == nil:
		panic(errors.New("a buffer tail was not found"))
	default:
		link := &links{prv: b.tail, op: op}
		b.tail.nxt = link
		b.tail = link
	}

	b.size++

	return nil
}

func (b *buffer) Clear() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.head = nil
	b.tail = nil
	b.cursor = nil
	b.size = 0
}
