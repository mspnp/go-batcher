package batcher

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBuffer_New(t *testing.T) {
	buffer := newBuffer(10)
	assert.Equal(t, uint32(0), buffer.size())
	assert.Equal(t, uint32(10), buffer.max())
}

func TestBuffer_Enqueue(t *testing.T) {
	buffer := newBuffer(1)
	watcher := NewWatcher(func(batch []Operation) {})
	op := NewOperation(watcher, 0, struct{}{}, false)
	err := buffer.enqueue(op, false)
	assert.Nil(t, err, "expecting no error on enqueue")
}

func TestBuffer_ErrorOnFull(t *testing.T) {
	var err error
	buffer := newBuffer(1)
	watcher := NewWatcher(func(batch []Operation) {})
	op1 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op1, true)
	assert.Nil(t, err, "expecting no error on enqueue")
	op2 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op2, true)
	assert.Equal(t, BufferFullError, err, "expecting the buffer is full because it has a size of 1")
}

func TestBuffer_BlockOnFull(t *testing.T) {
	buffer := newBuffer(1)
	watcher := NewWatcher(func(batch []Operation) {})
	done := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		wg.Wait()
		close(done)
	}()
	for i := 0; i < 2; i++ {
		go func() {
			op := NewOperation(watcher, 0, struct{}{}, false)
			err := buffer.enqueue(op, false)
			assert.Nil(t, err, "expecting no error on enqueue")
			wg.Done()
		}()
	}
	select {
	case <-done:
		assert.Fail(t, "expecting a timeout because the enqueue is blocking")
	case <-time.After(10 * time.Millisecond):
		// expect this timeout
	}
}

func TestBuffer_BlockOnFullAndThenEnqueue(t *testing.T) {
	buffer := newBuffer(1)
	watcher := NewWatcher(func(batch []Operation) {})

	done := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(5)
	go func() {
		wg.Wait()
		close(done)
	}()

	for i := 0; i < 5; i++ {
		go func(ii int) {
			op := NewOperation(watcher, 0, struct{}{}, false)
			err := buffer.enqueue(op, false)
			assert.Nil(t, err, "expecting no error on enqueue")
		}(i)
	}

	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(5 * time.Millisecond)
			assert.NotNil(t, buffer.top())
			assert.Nil(t, buffer.remove())
			wg.Done()
		}
	}()

	select {
	case <-done:
		// expect done
	case <-time.After(200 * time.Millisecond):
		assert.Fail(t, "expecting the enqueue to finish because of buffer.Remove()")
	}

	assert.Equal(t, uint32(0), buffer.size())
}

func TestBuffer_SizeIsCorrect(t *testing.T) {
	var err error
	buffer := newBuffer(10)
	assert.Equal(t, uint32(0), buffer.size())
	watcher := NewWatcher(func(batch []Operation) {})

	op1 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op1, false)
	assert.Nil(t, err, "expecting no error on enqueue")
	assert.Equal(t, uint32(1), buffer.size())

	op2 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op2, false)
	assert.Nil(t, err, "expecting no error on enqueue")
	assert.Equal(t, uint32(2), buffer.size())

	buffer.top()

	buffer.remove()
	assert.Nil(t, err, "expecting no error on remove")
	assert.Equal(t, uint32(1), buffer.size())

	buffer.remove()
	assert.Nil(t, err, "expecting no error on remove")
	assert.Equal(t, uint32(0), buffer.size())
}

func TestBuffer_Skip(t *testing.T) {
	var err error
	buffer := newBuffer(10)
	assert.Nil(t, buffer.skip())
	watcher := NewWatcher(func(batch []Operation) {})

	op1 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op1, false)
	assert.Nil(t, err, "expecting no error on enqueue")

	op2 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op2, false)
	assert.Nil(t, err, "expecting no error on enqueue")

	assert.Equal(t, op1, buffer.top())
	assert.Equal(t, op2, buffer.skip())
	assert.Nil(t, buffer.skip())
	assert.Equal(t, uint32(2), buffer.size())
}

func TestBuffer_Remove(t *testing.T) {
	var err error
	buffer := newBuffer(10)
	assert.Nil(t, buffer.remove())
	watcher := NewWatcher(func(batch []Operation) {})

	op1 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op1, false)
	assert.Nil(t, err, "expecting no error on enqueue")

	op2 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op2, false)
	assert.Nil(t, err, "expecting no error on enqueue")

	assert.Equal(t, op1, buffer.top())
	assert.Equal(t, uint32(2), buffer.size())
	assert.Equal(t, op2, buffer.remove())
	assert.Equal(t, uint32(1), buffer.size())
	assert.Nil(t, buffer.remove())
	assert.Equal(t, uint32(0), buffer.size())
}

func TestBuffer_RemoveFromMiddle(t *testing.T) {
	var err error
	buffer := newBuffer(10)
	watcher := NewWatcher(func(batch []Operation) {})

	op1 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op1, false)
	assert.Nil(t, err, "expecting no error on enqueue")

	op2 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op2, false)
	assert.Nil(t, err, "expecting no error on enqueue")

	op3 := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op3, false)
	assert.Nil(t, err, "expecting no error on enqueue")

	assert.Equal(t, op1, buffer.top())
	assert.Equal(t, op2, buffer.skip())
	assert.Equal(t, op3, buffer.remove())
	assert.Nil(t, buffer.skip())
	assert.Equal(t, op1, buffer.top())
	assert.Equal(t, op3, buffer.skip())
	assert.Nil(t, buffer.skip())
}

func TestBuffer_TopIsEmpty(t *testing.T) {
	buffer := newBuffer(10)
	assert.Nil(t, buffer.top(), "expecting no head")
}

func TestBuffer_Shutdown(t *testing.T) {
	var err error
	buffer := newBuffer(10)
	watcher := NewWatcher(func(batch []Operation) {})
	op := NewOperation(watcher, 0, struct{}{}, false)
	err = buffer.enqueue(op, false)
	assert.Nil(t, err, "expecting no error on enqueue")
	buffer.shutdown()
	assert.Equal(t, uint32(0), buffer.size())
	assert.Nil(t, buffer.top())
	err = buffer.enqueue(op, false)
	assert.Equal(t, BufferIsShutdown, err, "expecting an error when enqueue after shutdown")
}
