package batcher_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	gobatcher "github.com/plasne/go-batcher/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

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

func TestAzureSRStart_FactorDefaultsToOne(t *testing.T) {
	container, blob := getMocks()
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10).
		WithMocks(container, blob)
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
	blob.AssertNumberOfCalls(t, "Upload", 10)
}

func TestAzureSRStart_NoMoreThan500Partitions(t *testing.T) {
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks())
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
}

func TestAzureSRStart_CreateContainerOnce(t *testing.T) {
	container, blob := getMocks()
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
		WithMocks(container, blob).
		WithFactor(1000)
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	container.AssertNumberOfCalls(t, "Create", 1)
}

func TestAzureSRStart_CorrectNumberOfPartitionsCreated(t *testing.T) {
	container, blob := getMocks()
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
		WithMocks(container, blob).
		WithFactor(1000)
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
	blob.AssertNumberOfCalls(t, "Upload", 10)
}

func TestAzureSRStart_PartialPartitionsRoundUp(t *testing.T) {
	container, blob := getMocks()
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10050).
		WithMocks(container, blob).
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
	blob.AssertNumberOfCalls(t, "Upload", 11)
}

func TestAzureSRStart_ContainerCanBeCreated(t *testing.T) {
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000)
	var created int
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case "created-container":
			created += 1
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	assert.Equal(t, 1, created, "expecting a single created event")
}

func TestAzureSRStart_ContainerCanBeVerified(t *testing.T) {
	var serr azblob.StorageError = StorageError{serviceCode: azblob.ServiceCodeContainerAlreadyExists}
	container := new(containerURLMock)
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, serr)
	_, blob := getMocks()
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
		WithMocks(container, blob).
		WithFactor(1000)
	var verified int
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case "verified-container":
			verified += 1
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	assert.Equal(t, 1, verified, "expecting a single verified event")
}

func TestAzureSRStart_ContainerErrorsCascade(t *testing.T) {
	testCases := map[string]error{
		"unknown error": fmt.Errorf("unknown mocked error"),
		"storage error": StorageError{serviceCode: azblob.ServiceCodeAppendPositionConditionNotMet},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			container := new(containerURLMock)
			container.On("Create", mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr)
			_, blob := getMocks()
			res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
				WithMocks(container, blob).
				WithFactor(1000)
			err := res.Start(context.Background())
			assert.Equal(t, serr, err, "expecting the error to be thrown back from start")
		})
	}
}

func TestAzureSRStart_BlobCanBeCreated(t *testing.T) {
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000)
	var wg sync.WaitGroup
	wg.Add(1)
	var created int
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CreatedBlobEvent:
			created += 1
		case gobatcher.ProvisionDoneEvent:
			wg.Done()
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, 10, created, "expecting a creation event per partition")
}

func TestAzureSRStart_BlobCanBeVerified(t *testing.T) {
	testCases := map[string]azblob.StorageError{
		"exists": StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists},
		"leased": StorageError{serviceCode: azblob.ServiceCodeLeaseIDMissing},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			container, _ := getMocks()
			blob := new(blockBlobURLMock)
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr)
			res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
				WithMocks(container, blob).
				WithFactor(1000)
			var wg sync.WaitGroup
			wg.Add(1)
			var verified int
			res.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case gobatcher.VerifiedBlobEvent:
					verified += 1
				case gobatcher.ProvisionDoneEvent:
					wg.Done()
				}
			})
			err := res.Start(context.Background())
			assert.NoError(t, err, "not expecting a start error")
			wg.Wait()
			assert.Equal(t, 10, verified, "expecting a verified event per partition")
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
			container, _ := getMocks()
			blob := new(blockBlobURLMock)
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr)
			res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
				WithMocks(container, blob).
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
		})
	}
}

func TestMaxCapacityIsSharedPlusReserved(t *testing.T) {
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithReservedCapacity(2000).
		WithFactor(1000)
	max := res.MaxCapacity()
	assert.Equal(t, uint32(12000), max)
}

func TestMaxCapacityCapsAt500Partitions(t *testing.T) {
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithReservedCapacity(2000).
		WithFactor(1)
	max := res.MaxCapacity()
	assert.Equal(t, uint32(2500), max)
}

func TestCapacity(t *testing.T) {
	ctx := context.Background()

	t.Run("capacity is reserved-capacity with no sharing", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithReservedCapacity(2000)
		cap := res.Capacity()
		assert.Equal(t, uint32(2000), cap, "expecting capacity to only reflect reserved capacity since there is no start")
	})

	t.Run("capacity is reserved-capacity plus allocated-capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithReservedCapacity(2000).
			WithFactor(1000).
			WithMaxInterval(1)
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(2500)
		time.Sleep(1 * time.Second)
		cap := res.Capacity()
		assert.Equal(t, uint32(3000), cap, "expecting capacity to equal reserved plus allocated; in this case, it should only have allocated 1 factor")
	})

}

func TestGiveMe(t *testing.T) {
	ctx := context.Background()

	t.Run("give-me properly grants capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(4000)
		time.Sleep(1 * time.Second)
		cap := res.Capacity()
		assert.Equal(t, uint32(4000), cap, "expecting the target capacity to be allocated")
	})

	t.Run("give-me grants additional capacity above reserve", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithReservedCapacity(2000).
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
		res.GiveMe(4000)
		time.Sleep(1 * time.Second)
		assert.Equal(t, uint32(2), atomic.LoadUint32(&allocated), "expecting 2 allocations to meet the target which is above the reserved capacity")
		cap := res.Capacity()
		assert.Equal(t, uint32(4000), cap, "expecting the capacity to reflect the reserved capacity plus allocated capacity")
	})

	t.Run("give-me does not grant if reserve is equal", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
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
	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 0).
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
