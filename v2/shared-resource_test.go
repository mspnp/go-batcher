package batcher_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gobatcher "github.com/plasne/go-batcher/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockLeaseManager struct {
	mock.Mock
}

func (mgr *mockLeaseManager) RaiseEventsTo(sr gobatcher.Eventer) {
	mgr.Called(sr)
}

func (mgr *mockLeaseManager) Provision(ctx context.Context) (err error) {
	args := mgr.Called(ctx)
	return args.Error(0)
}

func (mgr *mockLeaseManager) CreatePartitions(ctx context.Context, count int) {
	mgr.Called(ctx, count)
}

func (mgr *mockLeaseManager) LeasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration) {
	args := mgr.Called(ctx, id, index)
	return args.Get(0).(time.Duration)
}

func TestSharedResource_Start_CorrectNumberOfPartitions(t *testing.T) {
	testCases := map[string]struct {
		sharedCapacity uint32
		factor         uint32
		partitions     int
		waits          int
	}{
		"factor defaults to 1":  {sharedCapacity: 10, partitions: 10, waits: 1},
		"max of 500 partitions": {sharedCapacity: 10000, factor: 1, partitions: 500, waits: 2},
		"partial rounds up":     {sharedCapacity: 10050, factor: 1000, partitions: 11, waits: 1},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mgr := &mockLeaseManager{}
			mgr.On("RaiseEventsTo", mock.Anything)
			mgr.On("Provision", mock.Anything).Return(nil)
			mgr.On("CreatePartitions", mock.Anything, testCase.partitions)
			res := gobatcher.NewSharedResource().
				WithSharedCapacity(testCase.sharedCapacity, mgr).
				WithFactor(testCase.factor)
			var wg sync.WaitGroup
			res.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case gobatcher.ProvisionDoneEvent:
					wg.Done()
				case gobatcher.ErrorEvent:
					assert.Equal(t, 10000, val) // only raised on "max of 500 partitions"
					wg.Done()
				}
			})
			wg.Add(testCase.waits)
			err := res.Start(ctx)
			assert.NoError(t, err, "not expecting a start error")
			wg.Wait()
			mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
			mgr.AssertNumberOfCalls(t, "Provision", 1)
			mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
		})
	}
}

func TestSharedResource_MaxCapacity_EqualToSharedPlusReserved(t *testing.T) {
	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	res := gobatcher.NewSharedResource().
		WithReservedCapacity(2000).
		WithSharedCapacity(10000, mgr).
		WithFactor(1000)
	max := res.MaxCapacity()
	assert.Equal(t, uint32(12000), max)
	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
}

func TestSharedResource_MaxCapacity_CapsAt500Partitions(t *testing.T) {
	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	res := gobatcher.NewSharedResource().
		WithReservedCapacity(2000).
		WithSharedCapacity(10000, mgr).
		WithFactor(1)
	max := res.MaxCapacity()
	assert.Equal(t, uint32(2500), max)
	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
}

func TestSharedResource_Capacity_EqualToReservedWhenThereIsNoRequest(t *testing.T) {
	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	res := gobatcher.NewSharedResource().
		WithReservedCapacity(2000).
		WithSharedCapacity(10000, mgr)
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity equal to reserved only because there was no GiveMe()")
	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
}

func TestSharedResource_Capacity_EqualToReservedPlusShared(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(15 * time.Second)

	res := gobatcher.NewSharedResource().
		WithReservedCapacity(2000).
		WithSharedCapacity(10000, mgr).
		WithFactor(1000).
		WithMaxInterval(1)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		case gobatcher.TargetEvent:
			assert.Equal(t, 500, val, "expecting 500 additional capacity is needed")
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity to equal reserved since there is no GiveMe() yet")

	wg.Add(2)
	res.GiveMe(2500)
	wg.Wait()
	assert.Equal(t, uint32(3000), res.Capacity(), "expecting capacity to equal reserved plus allocated; in this case, it should only have allocated 1 partition")

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
	mgr.AssertNumberOfCalls(t, "LeasePartition", 1)
}

func TestSharedResource_GiveMe_GrantsCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(15 * time.Second)

	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(1000).
		WithMaxInterval(1)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		case gobatcher.TargetEvent:
			assert.Equal(t, 4000, val, "expecting 4000 additional capacity is needed")
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(0), res.Capacity(), "expecting the capacity to be zero before GiveMe()")

	wg.Add(5) // 4 CapacityEvents + 1 TargetEvent
	res.GiveMe(4000)
	wg.Wait()
	assert.Equal(t, uint32(4000), res.Capacity(), "expecting the 4 partitions of capacity")

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
	mgr.AssertNumberOfCalls(t, "LeasePartition", 4)
}

func TestSharedResource_GiveMe_DoesNotGrantIfReserveIsEqual(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)

	res := gobatcher.NewSharedResource().
		WithReservedCapacity(2000).
		WithSharedCapacity(10000, mgr).
		WithFactor(1000).
		WithMaxInterval(1)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		case gobatcher.TargetEvent:
			assert.Equal(t, 0, val, "expecting no additional capacity is actually needed to fulfill the request")
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity to equal reserved since there is no GiveMe() yet")

	wg.Add(1)
	res.GiveMe(2000)
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity to equal reserved; no need to allocate capacity")

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
}

func TestSharedResource_GiveMe_DoesNotGrantIfReserveIsHigher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)

	res := gobatcher.NewSharedResource().
		WithReservedCapacity(2000).
		WithSharedCapacity(10000, mgr).
		WithFactor(1000).
		WithMaxInterval(1)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		case gobatcher.TargetEvent:
			assert.Equal(t, 0, val, "expecting no additional capacity is actually needed to fulfill the request")
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity to equal reserved since there is no GiveMe() yet")

	wg.Add(1)
	res.GiveMe(1800)
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity to equal reserved")

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
}

func TestSharedResource_GiveMe_GrantsAccordingToFactor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 13)
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(15 * time.Second)

	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(777).
		WithMaxInterval(1)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		case gobatcher.TargetEvent:
			assert.Equal(t, 1800, val, "expecting 1800 additional capacity is needed")
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(0), res.Capacity(), "expecting the capacity to be zero before GiveMe()")

	wg.Add(4) // 3 CapacityEvents + 1 TargetEvent
	res.GiveMe(1800)
	wg.Wait()
	assert.Equal(t, uint32(2331), res.Capacity(), "expecting the capacity to reflect 3 partitions")

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
	mgr.AssertNumberOfCalls(t, "LeasePartition", 3)
}

func TestSharedResource_Start_NoProvisionWithoutSharedCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := gobatcher.NewSharedResource().
		WithReservedCapacity(10000)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		}
	})
	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
}

func TestSharedResource_Start_CalledOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := gobatcher.NewSharedResource().
		WithReservedCapacity(10000)
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		err1 = res.Start(ctx)
		wg.Done()
	}()
	go func() {
		err2 = res.Start(ctx)
		wg.Done()
	}()
	wg.Wait()
	if err1 != nil {
		assert.Equal(t, gobatcher.ImproperOrderError, err1)
	} else if err2 != nil {
		assert.Equal(t, gobatcher.ImproperOrderError, err2)
	} else {
		t.Errorf("expected one of the two calls to fail (err1: %v) (err2: %v)", err1, err2)
	}
}

func TestSharedResource_Start_InitializationAfterStartCausesPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := gobatcher.NewSharedResource().
		WithReservedCapacity(1000)

	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	assert.PanicsWithError(t, gobatcher.InitializationOnlyError.Error(), func() { res.WithSharedCapacity(1000, nil) })
	assert.PanicsWithError(t, gobatcher.InitializationOnlyError.Error(), func() { res.WithReservedCapacity(1000) })
	assert.PanicsWithError(t, gobatcher.InitializationOnlyError.Error(), func() { res.WithFactor(10) })
	assert.PanicsWithError(t, gobatcher.InitializationOnlyError.Error(), func() { res.WithMaxInterval(10) })
}

func TestSharedResource_Start_AnnouncesStartingCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 1)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(1000, mgr).
		WithFactor(1000).
		WithReservedCapacity(2000)
	var count, value uint32
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			atomic.AddUint32(&count, 1)
			atomic.AddUint32(&value, uint32(val))
		}
	})
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, uint32(1), atomic.LoadUint32(&count), "expecting 1 capacity event")
	assert.Equal(t, uint32(2000), atomic.LoadUint32(&value), "expecting only the reserved capacity")

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
}

func TestSharedResource_Loop_CanLeaseAndReleasePartitions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(100 * time.Millisecond)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(1000).
		WithMaxInterval(1)

	var wg sync.WaitGroup
	var allocated, released uint32
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.AllocatedEvent:
			atomic.AddUint32(&allocated, 1)
			wg.Done()
		case gobatcher.ReleasedEvent:
			atomic.AddUint32(&released, 1)
			wg.Done()
		}
	})

	wg.Add(2)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	res.GiveMe(1800)
	wg.Wait()

	wg.Add(2)
	res.GiveMe(0)
	wg.Wait()

	assert.Equal(t, uint32(2), atomic.LoadUint32(&allocated), "expecting 2 allocations to meet capacity requirement")
	assert.Equal(t, uint32(2), atomic.LoadUint32(&released), "expecting 2 releases because more than 15 seconds have passed")

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
	mgr.AssertNumberOfCalls(t, "LeasePartition", 2)
}

func TestSharedResource_Loop_ZeroDurationLeasesDoNotAllocateOrRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(0 * time.Millisecond)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(1000)

	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.AllocatedEvent:
			assert.FailNow(t, "allocation was not expected")
		case gobatcher.ReleasedEvent:
			assert.FailNow(t, "released was not expected")
		}
	})

	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	res.GiveMe(2000)
	time.Sleep(1 * time.Second)

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
	mgr.AssertCalled(t, "LeasePartition", mock.Anything, mock.Anything, mock.Anything) // at least once
}

func TestSharedResource_Start_ProvisionReturnsErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	provErr := errors.New("provision error")
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(provErr)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(1000)

	var wg sync.WaitGroup
	var allocated, released uint32
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ProvisionDoneEvent:
			atomic.AddUint32(&allocated, 1)
			wg.Done()
		case gobatcher.ReleasedEvent:
			atomic.AddUint32(&released, 1)
			wg.Done()
		}
	})

	err := res.Start(ctx)
	assert.Equal(t, provErr, err)

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
}

func TestSharedResource_Start_OnlyAllocatesToMaxCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(15 * time.Second)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(1000).
		WithMaxInterval(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	res.GiveMe(80000)
	time.Sleep(100 * time.Millisecond)
	cap := res.Capacity()
	assert.Equal(t, uint32(10000), cap, "expecting capacity equal to max")

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
	mgr.AssertNumberOfCalls(t, "LeasePartition", 10)
}

func TestSharedResource_Loop_NoEventsRaisedAfterRemoveListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(15 * time.Second)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(1000).
		WithMaxInterval(1)

	var wg sync.WaitGroup
	var count uint32
	id := res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		atomic.AddUint32(&count, 1)
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	var start uint32
	atomic.AddUint32(&start, atomic.LoadUint32(&count))
	res.RemoveListener(id)

	res.GiveMe(10000)
	time.Sleep(100 * time.Millisecond)
	assert.Greater(t, atomic.LoadUint32(&start), uint32(0), "expecting there to be some initial events")
	assert.Equal(t, atomic.LoadUint32(&start), atomic.LoadUint32(&count), "expecting no events after removing the listener")

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
	mgr.AssertNumberOfCalls(t, "LeasePartition", 10)
}

func TestSharedResource_SetSharedCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)
	mgr.On("CreatePartitions", mock.Anything, 20)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(1000)
	var wg sync.WaitGroup
	var done uint32
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ProvisionDoneEvent:
			atomic.AddUint32(&done, 1)
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(1), done)
	assert.Equal(t, uint32(10000), res.MaxCapacity())

	atomic.StoreUint32(&done, 0)

	wg.Add(1)
	res.SetSharedCapacity(20000)
	wg.Wait()
	assert.Equal(t, uint32(1), done)
	assert.Equal(t, uint32(20000), res.MaxCapacity())

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 2)
}

func TestSharedResource_SetReservedCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 10)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(1000)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(10000), res.MaxCapacity())
	assert.Equal(t, uint32(0), res.Capacity())

	wg.Add(1)
	res.SetReservedCapacity(2000)
	wg.Wait()
	assert.Equal(t, uint32(12000), res.MaxCapacity())
	assert.Equal(t, uint32(2000), res.Capacity())

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
}

func TestSharedResource_SetSharedCapacity_WithoutLeaseManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := gobatcher.NewSharedResource().
		WithReservedCapacity(2000)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		}
	})
	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity())
	serr := res.SetSharedCapacity(2000)
	assert.Equal(t, gobatcher.SharedCapacityNotProvisioned, serr)
}

func waitUntil(f func() bool, timeout time.Duration) {
	total := 0
	for {
		time.Sleep(10 * time.Millisecond)
		total += 10
		if f() {
			return
		}
		if time.Duration(total)*time.Millisecond >= timeout {
			return
		}
	}
}

func TestSharedResource_SetSharedCapacity_KeepsExistingPartitionLeases(t *testing.T) {
	testCases := map[string]struct {
		changeTo int
		exp      uint32
	}{
		"increasing": {changeTo: 12, exp: 5000},
		"decreasing": {changeTo: 3, exp: 3000},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mgr := &mockLeaseManager{}
			mgr.On("RaiseEventsTo", mock.Anything)
			mgr.On("Provision", mock.Anything).Return(nil)
			mgr.On("CreatePartitions", mock.Anything, 10).Once()
			mgr.On("CreatePartitions", mock.Anything, testCase.changeTo)
			mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(15 * time.Second)
			res := gobatcher.NewSharedResource().
				WithSharedCapacity(10000, mgr).
				WithFactor(1000).
				WithMaxInterval(1)
			var wg sync.WaitGroup
			res.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case gobatcher.ProvisionDoneEvent:
					wg.Done()
				}
			})

			wg.Add(1)
			err := res.Start(ctx)
			assert.NoError(t, err, "not expecting a start error")
			wg.Wait()
			assert.Equal(t, uint32(0), res.Capacity())

			res.GiveMe(5000)
			waitUntil(func() bool {
				return res.Capacity() == 5000
			}, 100*time.Millisecond)
			assert.Equal(t, uint32(5000), res.Capacity())

			wg.Add(1)
			res.SetSharedCapacity(uint32(testCase.changeTo * 1000))
			wg.Wait()
			waitUntil(func() bool {
				return res.Capacity() == uint32(testCase.changeTo*10000)
			}, 100*time.Millisecond)
			assert.Equal(t, testCase.exp, res.Capacity())

			mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
			mgr.AssertNumberOfCalls(t, "Provision", 1)
			mgr.AssertNumberOfCalls(t, "CreatePartitions", 2)
			mgr.AssertCalled(t, "LeasePartition", mock.Anything, mock.Anything, mock.Anything) // at least once
		})
	}
}

func TestSharedResource_Loop_ExpiringLeasesThatAreNoLongerTrackedDoesNotCausePanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything).Once()
	mgr.On("Provision", mock.Anything).Return(nil).Once()
	mgr.On("CreatePartitions", mock.Anything, 10).Once()
	mgr.On("CreatePartitions", mock.Anything, 0).Once()
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(100 * time.Millisecond).Times(5)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(10000, mgr).
		WithFactor(1000).
		WithMaxInterval(1)
	var wg sync.WaitGroup

	provisionListener := res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ProvisionDoneEvent:
			wg.Done()
		}
	})
	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(0), res.Capacity())
	res.RemoveListener(provisionListener)

	capacityListener := res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		}
	})
	wg.Add(5)
	res.GiveMe(5000)
	wg.Wait()
	assert.Equal(t, uint32(5000), res.Capacity())
	res.RemoveListener(capacityListener)

	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ReleasedEvent:
			wg.Done()
		}
	})
	wg.Add(5)
	res.SetSharedCapacity(0)
	wg.Wait()
	assert.Equal(t, uint32(0), res.Capacity())
}

func TestSharedResource_Start_WithZeroSharedCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 0).Once()
	mgr.On("CreatePartitions", mock.Anything, 1)
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(15 * time.Second)
	res := gobatcher.NewSharedResource().
		WithSharedCapacity(0, mgr).
		WithFactor(1000)

	var expectedCapacity int
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			assert.Equal(t, expectedCapacity, val)
			wg.Done()
		case gobatcher.ProvisionDoneEvent:
			wg.Done()
		}
	})

	wg.Add(2)
	expectedCapacity = 0
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(0), res.MaxCapacity())

	wg.Add(2)
	expectedCapacity = 0
	res.SetSharedCapacity(1000)
	wg.Wait()
	assert.Equal(t, uint32(1000), res.MaxCapacity())

	wg.Add(1)
	expectedCapacity = 1000
	res.GiveMe(9999)
	wg.Wait()
	assert.Equal(t, uint32(1000), res.Capacity())
	assert.Equal(t, uint32(1000), res.MaxCapacity())

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 2)
	mgr.AssertNumberOfCalls(t, "LeasePartition", 1)
}

func TestSharedResource_Loop_ShutdownWithReservedCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	res := gobatcher.NewSharedResource().
		WithReservedCapacity(2000)
	done := make(chan bool)
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ShutdownEvent:
			close(done)
		}
	})
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	cancel()
	select {
	case <-done:
		// success
	case <-time.After(1 * time.Second):
		// timeout
		assert.Fail(t, "expected shutdown but didn't see one even after 1 second")
	}
}

func TestSharedResource_Loop_ShutdownWithSharedCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mgr := &mockLeaseManager{}
	mgr.On("RaiseEventsTo", mock.Anything)
	mgr.On("Provision", mock.Anything).Return(nil)
	mgr.On("CreatePartitions", mock.Anything, 2)

	res := gobatcher.NewSharedResource().
		WithSharedCapacity(2000, mgr).
		WithFactor(1000)
	done := make(chan bool)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ShutdownEvent:
			close(done)
		case gobatcher.ProvisionDoneEvent:
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(ctx)
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()

	cancel()
	select {
	case <-done:
		// success
	case <-time.After(1 * time.Second):
		// timeout
		assert.Fail(t, "expected shutdown but didn't see one even after 1 second")
	}

	mgr.AssertNumberOfCalls(t, "RaiseEventsTo", 1)
	mgr.AssertNumberOfCalls(t, "Provision", 1)
	mgr.AssertNumberOfCalls(t, "CreatePartitions", 1)
}
