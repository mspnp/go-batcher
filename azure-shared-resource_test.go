package batcher_test

// NOTE: please review unit-tests

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	gobatcher "github.com/plasne/go-batcher"
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

func TestProvision(t *testing.T) {
	ctx := context.Background()

	t.Run("can only create via new", func(t *testing.T) {
		res := gobatcher.AzureSharedResource{}
		err := res.Provision(ctx)
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Equal(t, gobatcher.UndefinedLeaseManagerError{}, err)
	})

	t.Run("shared-capacity is required", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 0).
			WithMocks(getMocks())
		err := res.Provision(ctx)
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Equal(t, gobatcher.UndefinedSharedCapacityError{}, err)
	})

	t.Run("provision is callable only once", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
		var err1, err2 error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			err1 = res.Provision(ctx)
			wg.Done()
		}()
		go func() {
			err2 = res.Provision(ctx)
			wg.Done()
		}()
		wg.Wait()
		if e, ok := err1.(gobatcher.RateLimiterImproperOrderError); ok && err2 == nil {
			// valid response
			_ = e.Error() // improves code coverage
		} else if e, ok := err2.(gobatcher.RateLimiterImproperOrderError); ok && err1 == nil {
			// valid response
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected one of the two calls to fail (err1: %v) (err2: %v)", err1, err2)
		}
	})

	t.Run("factor defaults to 1", func(t *testing.T) {
		container, blob := getMocks()
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10).
			WithMocks(container, blob)
		err := res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		blob.AssertNumberOfCalls(t, "Upload", 10)
	})

	t.Run("partitions limit is enforced", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks())
		err := res.Provision(ctx)
		if err != nil {
			_ = err.Error() // improves code coverage
		}
		assert.Equal(t, gobatcher.PartitionsOutOfRangeError{
			MaxCapacity:    uint32(10000),
			Factor:         uint32(1),
			PartitionCount: 10000,
		}, err, "expecting an error, but max-capacity should be configured, factor should default to 1, and partition-count should be derived")
	})

	t.Run("create container is called once when provisioning resource", func(t *testing.T) {
		container, blob := getMocks()
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000)
		err := res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		container.AssertNumberOfCalls(t, "Create", 1)
	})

	t.Run("correct number of partitions are created", func(t *testing.T) {
		container, blob := getMocks()
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000)
		err := res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		blob.AssertNumberOfCalls(t, "Upload", 10)
	})

	t.Run("partial partitions round up", func(t *testing.T) {
		container, blob := getMocks()
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10050).
			WithMocks(container, blob).
			WithFactor(1000)
		err := res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		blob.AssertNumberOfCalls(t, "Upload", 11)
	})

	t.Run("container can be created", func(t *testing.T) {
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
		err := res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		assert.Equal(t, 1, created, "expecting a single created event")
	})

	t.Run("container can be verified", func(t *testing.T) {
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
		err := res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		assert.Equal(t, 1, verified, "expecting a single verified event")
	})

	t.Run("unknown container error cascades", func(t *testing.T) {
		unknown := fmt.Errorf("unknown mocked error")
		container := new(containerURLMock)
		container.On("Create", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, unknown)
		_, blob := getMocks()
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000)
		err := res.Provision(ctx)
		assert.Equal(t, unknown, err, "expecting the error to be thrown back from provision")
	})

	t.Run("unrelated container storage error cascades", func(t *testing.T) {
		var serr azblob.StorageError = StorageError{serviceCode: azblob.ServiceCodeAppendPositionConditionNotMet}
		container := new(containerURLMock)
		container.On("Create", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, serr)
		_, blob := getMocks()
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000)
		err := res.Provision(ctx)
		assert.Equal(t, serr, err, "expecting the error to be thrown back from provision")
	})

	t.Run("blob can be created", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
		var created int
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case "created-blob":
				created += 1
			}
		})
		err := res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		assert.Equal(t, 10, created, "expecting a creation event per partition")
	})

	blobErrors := map[string]azblob.StorageError{
		"blob can be verified because it exists":    StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists},
		"blob can be verified because it is leased": StorageError{serviceCode: azblob.ServiceCodeLeaseIDMissing},
	}
	for testName, serr := range blobErrors {
		t.Run(testName, func(t *testing.T) {
			container, _ := getMocks()
			blob := new(blockBlobURLMock)
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr)
			res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
				WithMocks(container, blob).
				WithFactor(1000)
			var verified int
			res.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case "verified-blob":
					verified += 1
				}
			})
			err := res.Provision(ctx)
			assert.NoError(t, err, "not expecting a provision error")
			assert.Equal(t, 10, verified, "expecting a verified event per partition")
		})
	}

	t.Run("unknown blob error cascades", func(t *testing.T) {
		unknown := fmt.Errorf("unknown mocked error")
		container, _ := getMocks()
		blob := new(blockBlobURLMock)
		blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, unknown)
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000)
		err := res.Provision(ctx)
		assert.Equal(t, unknown, err, "expecting the error to be thrown back from provision")
	})

	t.Run("unrelated blob storage error cascades", func(t *testing.T) {
		serr := StorageError{serviceCode: azblob.ServiceCodeBlobArchived}
		container, _ := getMocks()
		blob := new(blockBlobURLMock)
		blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, serr)
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000)
		err := res.Provision(ctx)
		assert.Equal(t, serr, err, "expecting the error to be thrown back from provision")
	})

}

func TestMaxCapacity(t *testing.T) {

	t.Run("max-capacity is shared-capacity plus reserved-capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithReservedCapacity(2000)
		max := res.MaxCapacity()
		assert.Equal(t, uint32(12000), max)
	})

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
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
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
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
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
		var allocated int
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				allocated += 1
			}
		})
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(4000)
		time.Sleep(1 * time.Second)
		assert.Equal(t, 2, allocated, "expecting 2 allocations to meet the target which is above the reserved capacity")
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
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
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
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
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
		var allocated int
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				allocated += 1
			}
		})
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(1800)
		time.Sleep(1 * time.Second)
		assert.Equal(t, 3, allocated, "expecting 3 allocations because that is the lowest amount higher than the target")
		cap := res.Capacity()
		assert.Equal(t, uint32(2331), cap, "expecting the capacity to reflect 3 allocations")
	})

	t.Run("give-me does not grant above capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithReservedCapacity(2000).
			WithMaxInterval(1)
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(200000)
		time.Sleep(1 * time.Second)
		cap := res.Capacity()
		assert.Equal(t, uint32(12000), cap, "expecting the capacity to be at the maximum")
	})

}

func TestAzureSRStart(t *testing.T) {
	ctx := context.Background()

	t.Run("start must be called after provision", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks())
		err := res.Start(ctx)
		if e, ok := err.(gobatcher.RateLimiterImproperOrderError); ok {
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected not provisioned error")
		}
	})

	t.Run("start is callable only once", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
		err := res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
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
		if e, ok := err1.(gobatcher.RateLimiterImproperOrderError); ok && err2 == nil {
			// valid response
			_ = e.Error() // improves code coverage
		} else if e, ok := err2.(gobatcher.RateLimiterImproperOrderError); ok && err1 == nil {
			// valid response
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected one of the two calls to fail (err1: %v) (err2: %v)", err1, err2)
		}
	})

	t.Run("start announces starting capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithReservedCapacity(2000)
		var count, value int
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.CapacityEvent:
				count += 1
				value = val
			}
		})
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, 1, count, "expecting 1 capacity event")
		assert.Equal(t, 2000, value, "expecting only the reserved capacity")
	})

	t.Run("start can lease and release partitions", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
		var allocated, released int
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				allocated += 1
			case gobatcher.ReleasedEvent:
				released += 1
			}
		})
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(1800)
		time.Sleep(2 * time.Second)
		res.GiveMe(0)
		time.Sleep(18 * time.Second)
		assert.Equal(t, 2, allocated, "expecting 2 allocations to meet capacity requirement")
		assert.Equal(t, 2, released, "expecting 2 releases because more than 15 seconds have passed")
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
		var allocated, failed, errored int
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				allocated += 1
			case gobatcher.FailedEvent:
				failed += 1
			case gobatcher.ErrorEvent:
				errored += 1
			}
		})
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(500)
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, 1, allocated, "expecting 1 allocation")
		assert.Equal(t, 1, failed, "expecting 1 failed (already leased)")
		assert.Equal(t, 2, errored, "expecting 2 errors (unknown and unrelated)")
	})

	t.Run("start only allocates to max capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
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
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
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
		err := res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
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
		count := 0
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.ShutdownEvent:
				count += 1
			}
		})
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		go func() {
			res.Stop()
		}()
		go func() {
			res.Stop()
		}()
		time.Sleep(1 * time.Second)
		assert.Equal(t, 1, count, "expecting only a single shutdown")
	})

}

func TestRemoveListener(t *testing.T) {
	ctx := context.Background()

	t.Run("ensure nothing is raised after removing listener", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		count := 0
		id := res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			count += 1
		})
		var err error
		err = res.Provision(ctx)
		assert.NoError(t, err, "not expecting a provision error")
		err = res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		start := count
		res.RemoveListener(id)
		res.GiveMe(10000)
		time.Sleep(100 * time.Millisecond)
		assert.Greater(t, start, 0, "expecting there to be some initial events")
		assert.Equal(t, start, count, "expecting no events after removing the listener")
	})

}
