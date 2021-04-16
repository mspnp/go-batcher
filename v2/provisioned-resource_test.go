package batcher_test

import (
	"context"
	"testing"
	"time"

	gobatcher "github.com/plasne/go-batcher/v2"
	"github.com/stretchr/testify/assert"
)

func TestSetReservedCapacityForProvisioned(t *testing.T) {
	var err error
	res := gobatcher.NewProvisionedResource(1000)
	batcher := gobatcher.NewBatcher().
		WithRateLimiter(res).
		WithFlushInterval(10 * time.Minute)

	err = res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	err = batcher.Start()
	assert.NoError(t, err, "not expecting a start error")
	assert.Equal(t, uint32(1000), res.Capacity())

	res.SetReservedCapacity(2000)
	assert.Equal(t, uint32(2000), res.Capacity())
}
