package batcher_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	gobatcher "github.com/plasne/go-batcher"
	"github.com/stretchr/testify/mock"
)

// TODO switch to using assertions
// TODO add tests for blob errors

type blockBlobURLMock struct {
	mock.Mock
}

func (b *blockBlobURLMock) Upload(ctx context.Context, reader io.ReadSeeker, headers azblob.BlobHTTPHeaders, metadata azblob.Metadata, conditions azblob.BlobAccessConditions, accessTier azblob.AccessTierType, tags azblob.BlobTagsMap) (*azblob.BlockBlobUploadResponse, error) {
	args := b.Called(ctx, reader, headers, metadata, conditions, accessTier, tags)
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

func getMocks() (*containerURLMock, *blockBlobURLMock) {

	// build container
	container := new(containerURLMock)
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	// build blob
	blob := new(blockBlobURLMock)
	blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
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
		if e, ok := err.(gobatcher.UndefinedLeaseManagerError); ok {
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected undefined lease manager")
		}
	})

	t.Run("shared-capacity is required", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 0).
			WithMocks(getMocks())
		err := res.Provision(ctx)
		if e, ok := err.(gobatcher.UndefinedSharedCapacityError); ok {
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected undefined shared-capacity")
		}
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
		if e, ok := err1.(gobatcher.MultipleCallsNotAllowedError); ok && err2 == nil {
			// valid response
			_ = e.Error() // improves code coverage
		} else if e, ok := err2.(gobatcher.MultipleCallsNotAllowedError); ok && err1 == nil {
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
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		blob.AssertNumberOfCalls(t, "Upload", 10)
	})

	t.Run("partitions limit is enforced", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks())
		err := res.Provision(ctx)
		if e, ok := err.(gobatcher.PartitionsOutOfRangeError); ok {
			_ = e.Error() // improves code coverage
			if e.MaxCapacity != 10000 {
				t.Errorf("expected max-capacity to be 10,000 (configured value)")
			}
			if e.Factor != 1 {
				t.Errorf("expected factor to be 1 (the default)")
			}
			if e.PartitionCount != 10000 {
				t.Errorf("expected partition-count to be 10,000 (calculated value)")
			}
		} else {
			t.Errorf("expected partitions out of range")
		}
	})

	t.Run("create container is called once when provisioning resource", func(t *testing.T) {
		container, blob := getMocks()
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000)
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		container.AssertNumberOfCalls(t, "Create", 1)
	})

	t.Run("correct number of partitions are created", func(t *testing.T) {
		container, blob := getMocks()
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000)
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		blob.AssertNumberOfCalls(t, "Upload", 10)
	})

	t.Run("partial partitions round up", func(t *testing.T) {
		container, blob := getMocks()
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10050).
			WithMocks(container, blob).
			WithFactor(1000)
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		blob.AssertNumberOfCalls(t, "Upload", 11)
	})

}

func TestMaxCapacity(t *testing.T) {

	t.Run("max-capacity is shared-capacity plus reserved-capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithReservedCapacity(2000)
		max := res.MaxCapacity()
		if max != 12000 {
			t.Errorf("incorrect max-capacity; want (12000), have (%v)", max)
		}
	})

}

func TestCapacity(t *testing.T) {
	ctx := context.Background()

	t.Run("capacity is reserved-capacity with no sharing", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithReservedCapacity(2000)
		cap := res.Capacity()
		if cap != 2000 {
			t.Errorf("incorrect capacity; want (2000), have (%v)", cap)
		}
	})

	t.Run("capacity is reserved-capacity plus allocated-capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithReservedCapacity(2000).
			WithFactor(1000).
			WithMaxInterval(1)
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if err := res.Start(ctx); err != nil {
			t.Error(err)
			return
		}
		res.GiveMe(2500)
		time.Sleep(1 * time.Second)
		cap := res.Capacity()
		if cap != 3000 {
			t.Errorf("incorrect capacity; want (3000), have (%v)", cap)
		}
	})

}

func TestGiveMe(t *testing.T) {
	ctx := context.Background()

	t.Run("give-me properly grants capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if err := res.Start(ctx); err != nil {
			t.Error(err)
			return
		}
		res.GiveMe(4000)
		time.Sleep(1 * time.Second)
		cap := res.Capacity()
		if cap != 4000 {
			t.Errorf("incorrect capacity; want (4000), have (%v)", cap)
		}
	})

	t.Run("give-me grants additional capacity above reserve", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithReservedCapacity(2000).
			WithMaxInterval(1)
		var allocated int
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "allocated":
				allocated += 1
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if err := res.Start(ctx); err != nil {
			t.Error(err)
			return
		}
		res.GiveMe(4000)
		time.Sleep(1 * time.Second)
		if allocated != 2 {
			t.Errorf("want (2 allocations), have (%v allocations)", allocated)
		}
	})

	t.Run("give-me does not grant below reserve", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithReservedCapacity(2000).
			WithMaxInterval(1)
		var allocated int
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "allocated":
				allocated += 1
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if err := res.Start(ctx); err != nil {
			t.Error(err)
			return
		}
		res.GiveMe(1800)
		time.Sleep(1 * time.Second)
		if allocated != 0 {
			t.Errorf("want (0 allocations), have (%v allocations)", allocated)
		}
	})

}

func TestStart(t *testing.T) {
	ctx := context.Background()

	t.Run("start must be called after provision", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks())
		err := res.Start(ctx)
		if e, ok := err.(gobatcher.NotProvisionedError); ok {
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected not provisioned error")
		}
	})

	t.Run("start is callable only once", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
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
		if e, ok := err1.(gobatcher.MultipleCallsNotAllowedError); ok && err2 == nil {
			// valid response
			_ = e.Error() // improves code coverage
		} else if e, ok := err2.(gobatcher.MultipleCallsNotAllowedError); ok && err1 == nil {
			// valid response
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected one of the two calls to fail (err1: %v) (err2: %v)", err1, err2)
		}
	})

	t.Run("start can lease and release partitions", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000)
		var allocated, released int
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "allocated":
				allocated += 1
			case "released":
				released += 1
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if err := res.Start(ctx); err != nil {
			t.Error(err)
			return
		}
		res.GiveMe(1800)
		time.Sleep(2 * time.Second)
		res.GiveMe(0)
		time.Sleep(18 * time.Second)
		if allocated != 2 || released != 2 {
			t.Errorf("want (2 allocations, 2 releases), have(%v allocations, %v releases)", allocated, released)
		}
	})

}

func TestStop(t *testing.T) {
	ctx := context.Background()

	t.Run("stop emits shutdown", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		done := make(chan bool)
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "shutdown":
				close(done)
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if err := res.Start(ctx); err != nil {
			t.Error(err)
			return
		}
		res.Stop()
		select {
		case <-done:
			// success
		case <-time.After(1 * time.Second):
			// timeout
			t.Errorf("expected shutdown but didn't see one even after 1 second")
		}
	})

	t.Run("stop before start does not shutdown", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
			WithMocks(getMocks()).
			WithFactor(1000).
			WithMaxInterval(1)
		done := make(chan bool)
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "shutdown":
				close(done)
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		res.Stop()
		select {
		case <-done:
			// success
			t.Errorf("expected no shutdown")
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
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "shutdown":
				count += 1
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if err := res.Start(ctx); err != nil {
			t.Error(err)
			return
		}
		go func() {
			res.Stop()
		}()
		go func() {
			res.Stop()
		}()
		time.Sleep(1 * time.Second)
		if count != 1 {
			t.Errorf("want (1 shutdown); have (%v shutdowns) after 1 seconds", count)
		}
	})

}
