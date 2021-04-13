package batcher_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gobatcher "github.com/plasne/go-batcher"
	"github.com/stretchr/testify/assert"
)

func TestSetCapacity(t *testing.T) {
	var err error
	res := gobatcher.NewProvisionedResource(1000)
	var wg sync.WaitGroup
	var events uint32
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			switch atomic.LoadUint32(&events) {
			case 0:
				assert.Equal(t, 1000, val)
			case 1:
				assert.Equal(t, 2000, val)
			}
			atomic.AddUint32(&events, 1)
			wg.Done()
		}
	})
	batcher := gobatcher.NewBatcher().
		WithRateLimiter(res).
		WithFlushInterval(10 * time.Minute)

	wg.Add(1)
	err = res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	err = batcher.Start()
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(1000), res.Capacity())

	wg.Add(1)
	res.SetCapacity(2000)
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity())
	assert.Equal(t, uint32(2), atomic.LoadUint32(&events))
}
