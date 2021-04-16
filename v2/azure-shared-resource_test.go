package batcher_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	gobatcher "github.com/plasne/go-batcher/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

/*
type blockBlobURLMock struct {
	mock.Mock
}

func (b *blockBlobURLMock) Upload(ctx context.Context, reader io.ReadSeeker, headers azblob.BlobHTTPHeaders, metadata azblob.Metadata, conditions azblob.BlobAccessConditions, accessTier azblob.AccessTierType, tags azblob.BlobTagsMap, clientKeyOpts azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobUploadResponse, error) {
	args := b.Called(ctx, reader, headers, metadata, conditions, accessTier, tags, clientKeyOpts)
	return nil, args.Error(1)
}

func (b *blockBlobURLMock) AcquireLease(ctx context.Context, proposedId string, duration int32, conditions azblob.ModifiedAccessConditions) (*azblob.BlobAcquireLeaseResponse, error) {
	args := b.Called(ctx, proposedId, duration, conditions)
	return nil, args.Error(1)
}

type containerURLMock struct {
	mock.Mock
}

func (c *containerURLMock) Create(ctx context.Context, metadata azblob.Metadata, publicAccessType azblob.PublicAccessType) (*azblob.ContainerCreateResponse, error) {
	args := c.Called(ctx, metadata, publicAccessType)
	return nil, args.Error(1)
}

func (c *containerURLMock) NewBlockBlobURL(url string) azblob.BlockBlobURL {
	_ = c.Called(url)
	return azblob.BlockBlobURL{}
}
*/

type StorageError struct {
	serviceCode azblob.ServiceCodeType
}

func (e StorageError) ServiceCode() azblob.ServiceCodeType {
	return e.serviceCode
}

func (e StorageError) Error() string {
	return "this is a mock error"
}

func (e StorageError) Timeout() bool {
	return false
}

func (e StorageError) Temporary() bool {
	return false
}

func (e StorageError) Response() *http.Response {
	return nil
}

/*
func getMocks() (*containerURLMock, *blockBlobURLMock) {

	// build container
	container := new(containerURLMock)
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	// build blob
	blob := new(blockBlobURLMock)
	blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	return container, blob
}
*/

type mockLeaseManager struct {
	mock.Mock
}

func (mgr *mockLeaseManager) Provision(ctx context.Context) (err error) {
	args := mgr.Called(ctx)
	return args.Error(0)
}

func (mgr *mockLeaseManager) CreatePartitions(ctx context.Context, count int) (err error) {
	args := mgr.Called(ctx, count)
	return args.Error(0)
}

func (mgr *mockLeaseManager) LeasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration) {
	args := mgr.Called(ctx, id, index)
	return args.Get(0).(time.Duration)
}

func TestAzureSRStart_FactorDefaultsToOne(t *testing.T) {
	mgr := &mockLeaseManager{}
	mgr.On("Provision", mock.Anything).Return(nil).Once()
	mgr.On("CreatePartitions", mock.Anything, 10).Return(nil).Once()
	res := gobatcher.NewSharedResource("accountName", "containerName").
		WithSharedCapacity(10, mgr)
	var wg sync.WaitGroup
	wg.Add(1)
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ProvisionDoneEvent:
			assert.Equal(t, 10, val)
			wg.Done()
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	mgr.AssertExpectations(t)
}

func TestAzureSRStart_NoMoreThan500Partitions(t *testing.T) {
	mgr := &mockLeaseManager{}
	mgr.On("Provision", mock.Anything).Return(nil).Once()
	mgr.On("CreatePartitions", mock.Anything, 500).Return(nil).Once()
	res := gobatcher.NewSharedResource("accountName", "containerName").
		WithSharedCapacity(10000, mgr)
	var wg sync.WaitGroup
	wg.Add(1)
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ErrorEvent:
			assert.Equal(t, 10000, val)
			wg.Done()
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	mgr.AssertExpectations(t)
}

func TestAzureSRStart_PartialPartitionsRoundUp(t *testing.T) {
	mgr := &mockLeaseManager{}
	mgr.On("Provision", mock.Anything).Return(nil).Once()
	mgr.On("CreatePartitions", mock.Anything, 11).Return(nil).Once()
	res := gobatcher.NewSharedResource("accountName", "containerName").
		WithSharedCapacity(10050, mgr).
		WithFactor(1000)
	var wg sync.WaitGroup
	wg.Add(1)
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ProvisionDoneEvent:
			assert.Equal(t, 11, val)
			wg.Done()
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	mgr.AssertExpectations(t)
}

func TestAzureSRStart_ContainerErrorsCascade(t *testing.T) {
	testCases := map[string]error{
		"unknown error": fmt.Errorf("unknown mocked error"),
		"storage error": StorageError{serviceCode: azblob.ServiceCodeAppendPositionConditionNotMet},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			mgr := &mockLeaseManager{}
			mgr.On("Provision", mock.Anything).Return(serr).Once()
			res := gobatcher.NewSharedResource("accountName", "containerName").
				WithSharedCapacity(10000, mgr).
				WithFactor(1000)
			err := res.Start(context.Background())
			assert.Equal(t, serr, err, "expecting the error to be thrown back from start")
			mgr.AssertExpectations(t)
		})
	}
}

func TestAzureSRStart_BlobErrorsCascade(t *testing.T) {
	testCases := map[string]error{
		"unknown error": fmt.Errorf("unknown mocked error"),
		"storage error": StorageError{serviceCode: azblob.ServiceCodeBlobArchived},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			mgr := &mockLeaseManager{}
			mgr.On("Provision", mock.Anything).Return(nil).Once()
			mgr.On("CreatePartitions", mock.Anything, 10).Return(serr).Once()
			res := gobatcher.NewSharedResource("accountName", "containerName").
				WithSharedCapacity(10000, mgr).
				WithFactor(1000)
			var wg sync.WaitGroup
			wg.Add(1)
			res.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case gobatcher.ErrorEvent:
					assert.Equal(t, serr, metadata)
					wg.Done()
				}
			})
			err := res.Start(context.Background())
			assert.NoError(t, err, "not expecting a start error")
			wg.Wait()
			mgr.AssertExpectations(t)
		})
	}
}

func TestMaxCapacityIsSharedPlusReserved(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName").
		WithReservedCapacity(2000).
		WithSharedCapacity(10000, nil).
		WithFactor(1000)
	max := res.MaxCapacity()
	assert.Equal(t, uint32(12000), max)
}

func TestMaxCapacityCapsAt500Partitions(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName").
		WithReservedCapacity(2000).
		WithSharedCapacity(10000, nil).
		WithFactor(1)
	max := res.MaxCapacity()
	assert.Equal(t, uint32(2500), max)
}

func TestCapacityIsEqualToReservedWhenThereIsNoRequestForCapacity(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName").
		WithReservedCapacity(2000).
		WithSharedCapacity(10000, nil)
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity equal to reserved only because there was no GiveMe()")
}

func TestCapacityIsEqualToReservedPlusShared(t *testing.T) {
	mgr := &mockLeaseManager{}
	mgr.On("Provision", mock.Anything).Return(nil).Once()
	mgr.On("CreatePartitions", mock.Anything, 10).Return(nil).Once()
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(15 * time.Second).Once()

	res := gobatcher.NewSharedResource("accountName", "containerName").
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
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity to equal reserved since there is no GiveMe() yet")

	wg.Add(2)
	res.GiveMe(2500)
	wg.Wait()
	assert.Equal(t, uint32(3000), res.Capacity(), "expecting capacity to equal reserved plus allocated; in this case, it should only have allocated 1 partition")

	mgr.AssertExpectations(t)
}

func TestGiveMeGrantsCapacity(t *testing.T) {
	mgr := &mockLeaseManager{}
	mgr.On("Provision", mock.Anything).Return(nil).Once()
	mgr.On("CreatePartitions", mock.Anything, 10).Return(nil).Once()
	mgr.On("LeasePartition", mock.Anything, mock.Anything, mock.Anything).Return(15 * time.Second).Times(4)

	res := gobatcher.NewSharedResource("accountName", "containerName").
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
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(0), res.Capacity(), "expecting the capacity to be zero before GiveMe()")

	wg.Add(5) // 4 CapacityEvents + 1 TargetEvent
	res.GiveMe(4000)
	wg.Wait()
	assert.Equal(t, uint32(4000), res.Capacity(), "expecting the 4 partitions of capacity")

	mgr.AssertExpectations(t)
}

func TestGiveMeDoesNotGrantIfReserveIsEqual(t *testing.T) {
	mgr := &mockLeaseManager{}
	mgr.On("Provision", mock.Anything).Return(nil).Once()
	mgr.On("CreatePartitions", mock.Anything, 10).Return(nil).Once()

	res := gobatcher.NewSharedResource("accountName", "containerName").
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
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity to equal reserved since there is no GiveMe() yet")

	wg.Add(1)
	res.GiveMe(2000)
	wg.Wait()
	assert.Equal(t, uint32(2000), res.Capacity(), "expecting capacity to equal reserved; no need to allocate capacity")

	mgr.AssertExpectations(t)
}

// LILA STARTED HERE

/*



	t.Run("give-me does not grant if reserve is equal", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithReservedCapacity(2000).
			WithMaxInterval(1)
		var allocated int
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				allocated += 1
			}
		})
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(2000)
		time.Sleep(1 * time.Second)
		assert.Equal(t, 0, allocated, "expecting no allocations because the target was not more than reserved capacity")
		cap := res.Capacity()
		assert.Equal(t, uint32(2000), cap, "expecting capacity to reflect the reserved capacity")
	})

	t.Run("give-me does not grant if reserve is higher", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithReservedCapacity(2000).
			WithMaxInterval(1)
		var allocated int
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				allocated += 1
			}
		})
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(1800)
		time.Sleep(1 * time.Second)
		assert.Equal(t, 0, allocated, "expecting no allocations because the target was not more than reserved capacity")
		cap := res.Capacity()
		assert.Equal(t, uint32(2000), cap, "expecting capacity to reflect the reserved capacity")
	})

	t.Run("give-me grants according to factor", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(777).
			WithMaxInterval(1)
		var allocated uint32
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				atomic.AddUint32(&allocated, 1)
			}
		})
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(1800)
		time.Sleep(1 * time.Second)
		assert.Equal(t, uint32(3), atomic.LoadUint32(&allocated), "expecting 3 allocations because that is the lowest amount higher than the target")
		cap := res.Capacity()
		assert.Equal(t, uint32(2331), cap, "expecting the capacity to reflect 3 allocations")
	})

	t.Run("give-me does not grant above capacity", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithReservedCapacity(2000).
			WithMaxInterval(1)
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(200000)
		time.Sleep(1 * time.Second)
		cap := res.Capacity()
		assert.Equal(t, uint32(12000), cap, "expecting the capacity to be at the maximum")
	})

}

func TestAzureSRStart(t *testing.T) {
	ctx := context.Background()

	t.Run("start is callable only once", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
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
	})

	t.Run("start announces starting capacity", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
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
	})

	t.Run("start can lease and release partitions", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
		var allocated, released uint32
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				atomic.AddUint32(&allocated, 1)
			case gobatcher.ReleasedEvent:
				atomic.AddUint32(&released, 1)
			}
		})
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(1800)
		time.Sleep(2 * time.Second)
		res.GiveMe(0)
		time.Sleep(18 * time.Second)
		assert.Equal(t, uint32(2), atomic.LoadUint32(&allocated), "expecting 2 allocations to meet capacity requirement")
		assert.Equal(t, uint32(2), atomic.LoadUint32(&released), "expecting 2 releases because more than 15 seconds have passed")
	})

	t.Run("start can lease, fail, and handle errors", func(t *testing.T) {
		serr := StorageError{serviceCode: azblob.ServiceCodeLeaseAlreadyPresent}
		unknown := fmt.Errorf("unknown mocked error")
		unrelated := StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists}
		container, _ := getMocks()
		blob := new(blockBlobURLMock)
		blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, nil)
		blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, unknown).
			Once()
		blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, serr).
			Once()
		blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, unrelated).
			Once()
		blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, nil).
			Once()
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000).
			WithMaxInterval(1)
		var allocated, failed, errored uint32
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				atomic.AddUint32(&allocated, 1)
			case gobatcher.FailedEvent:
				atomic.AddUint32(&failed, 1)
			case gobatcher.ErrorEvent:
				atomic.AddUint32(&errored, 1)
			}
		})
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(500)
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, uint32(1), atomic.LoadUint32(&allocated), "expecting 1 allocation")
		assert.Equal(t, uint32(1), atomic.LoadUint32(&failed), "expecting 1 failed (already leased)")
		assert.Equal(t, uint32(2), atomic.LoadUint32(&errored), "expecting 2 errors (unknown and unrelated)")
	})

	t.Run("start only allocates to max capacity", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(80000)
		time.Sleep(100 * time.Millisecond)
		cap := res.Capacity()
		assert.Equal(t, uint32(10000), cap, "expecting capacity equal to max")
	})

}

func TestAzureSRStop(t *testing.T) {
	ctx := context.Background()

	t.Run("stop emits shutdown", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		done := make(chan bool)
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.ShutdownEvent:
				close(done)
			}
		})
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.Stop()
		select {
		case <-done:
			// success
		case <-time.After(1 * time.Second):
			// timeout
			assert.Fail(t, "expected shutdown but didn't see one even after 1 second")
		}
	})

	t.Run("stop before start does not shutdown", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		done := make(chan bool)
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.ShutdownEvent:
				close(done)
			}
		})
		res.Stop()
		select {
		case <-done:
			// success
			assert.Fail(t, "expected no shutdown")
		case <-time.After(1 * time.Second):
			// timeout; no shutdown as expected
		}
	})

	t.Run("multiple stops shutdown only once", func(t *testing.T) {
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		var count uint32
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.ShutdownEvent:
				atomic.AddUint32(&count, 1)
			}
		})
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		go func() {
			res.Stop()
		}()
		go func() {
			res.Stop()
		}()
		time.Sleep(1 * time.Second)
		assert.Equal(t, uint32(1), atomic.LoadUint32(&count), "expecting only a single shutdown")
	})

}

func TestNoEventsRaisedAfterRemoveListener(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
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
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	var start uint32
	atomic.AddUint32(&start, atomic.LoadUint32(&count))
	res.RemoveListener(id)
	res.GiveMe(10000)
	time.Sleep(100 * time.Millisecond)
	assert.Greater(t, atomic.LoadUint32(&start), uint32(0), "expecting there to be some initial events")
	assert.Equal(t, atomic.LoadUint32(&start), atomic.LoadUint32(&count), "expecting no events after removing the listener")
}

func TestSetSharedCapacity(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000)
	var wg sync.WaitGroup
	var start, done uint32
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ProvisionStartEvent:
			atomic.AddUint32(&start, 1)
			wg.Done()
		case gobatcher.ProvisionDoneEvent:
			atomic.AddUint32(&done, 1)
			wg.Done()
		}
	})

	wg.Add(2)
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(1), start)
	assert.Equal(t, uint32(1), done)
	assert.Equal(t, uint32(10000), res.MaxCapacity())

	atomic.StoreUint32(&start, 0)
	atomic.StoreUint32(&done, 0)

	wg.Add(2)
	res.SetSharedCapacity(20000)
	wg.Wait()
	assert.Equal(t, uint32(1), start)
	assert.Equal(t, uint32(1), done)
	assert.Equal(t, uint32(20000), res.MaxCapacity())
}

func TestSetReservedCapacity(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000)
	var wg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CapacityEvent:
			wg.Done()
		}
	})

	wg.Add(1)
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, uint32(10000), res.MaxCapacity())
	assert.Equal(t, uint32(0), res.Capacity())

	wg.Add(1)
	res.SetReservedCapacity(2000)
	wg.Wait()
	assert.Equal(t, uint32(12000), res.MaxCapacity())
	assert.Equal(t, uint32(2000), res.Capacity())
}

func TestAddingSharedCapacityKeepsExistingPartitionLeases(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000).
		WithMaxInterval(1)
	var provisionWg, capacityWg sync.WaitGroup
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ProvisionDoneEvent:
			provisionWg.Done()
		case gobatcher.CapacityEvent:
			capacityWg.Done()
		}
	})

	provisionWg.Add(1)
	capacityWg.Add(1)
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	provisionWg.Wait()
	capacityWg.Wait()
	assert.Equal(t, uint32(0), res.Capacity())

	capacityWg.Add(5)
	res.GiveMe(5000)
	capacityWg.Wait()
	assert.Equal(t, uint32(5000), res.Capacity())

	provisionWg.Add(1)
	capacityWg.Add(1)
	res.SetSharedCapacity(12000)
	provisionWg.Wait()
	capacityWg.Wait()
	assert.Equal(t, uint32(5000), res.Capacity())
}

func TestExpiringLeasesThatAreNoLongerTrackedDoesNotCausePanic(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000).
		WithMaxInterval(1).
		WithLeaseTime(1)
	var wg sync.WaitGroup

	provisionListener := res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ProvisionDoneEvent:
			wg.Done()
		}
	})
	wg.Add(1)
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
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
}

func TestStartingWithZeroSharedCapacity(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 0).
		WithMocks(getMocks()).
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
	err := res.Start(context.Background())
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
}
*/
