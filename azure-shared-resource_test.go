package batcher_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	gobatcher "github.com/plasne/go-batcher"
	"github.com/stretchr/testify/mock"
)

type blockBlobURLMock struct {
	mock.Mock
}

func (b *blockBlobURLMock) Upload(context.Context, io.ReadSeeker, azblob.BlobHTTPHeaders, azblob.Metadata, azblob.BlobAccessConditions, azblob.AccessTierType, azblob.BlobTagsMap) (*azblob.BlockBlobUploadResponse, error) {
	fmt.Println("upload-a-doodles")
	return nil, nil
}

func (b *blockBlobURLMock) AcquireLease(context.Context, string, int32, azblob.ModifiedAccessConditions) (*azblob.BlobAcquireLeaseResponse, error) {
	fmt.Println("aquire-a-lease")
	return nil, nil
}

type containerURLMock struct {
	mock.Mock
}

func (c *containerURLMock) Create(context.Context, azblob.Metadata, azblob.PublicAccessType) (*azblob.ContainerCreateResponse, error) {
	fmt.Println("create-a-saurus")
	return nil, nil
}

func (c *containerURLMock) NewBlockBlobURL(string) azblob.BlockBlobURL {
	fmt.Println("new-blob")

	/*
		azblob.BlockBlockURL{}

		azblob.blockblob

		blob := new(blockBlobURLMock)
		var bbb gobatcher.IAzureBlob = blob

		return bbb

		///return bbb.(azblob.BlockBlobURL)
	*/

	return azblob.BlockBlobURL{}
}

func TestFake(t *testing.T) {
	ctx := context.Background()

	container := new(containerURLMock)

	res := gobatcher.NewAzureSharedResource("accountName", "containerName", 100).
		WithMasterKey("key").
		WithMocks(container)
	err := res.Provision(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(1 * time.Second)

}

/*


import (
	"context"
	"sync"
	"testing"
	"time"

	gobatcher "github.com/plasne/go-batcher"
)

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
		res := gobatcher.NewAzureSharedResourceMock(0)
		err := res.Provision(ctx)
		if e, ok := err.(gobatcher.UndefinedSharedCapacityError); ok {
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected undefined shared-capacity")
		}
	})

	t.Run("provision is callable only once", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
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
		res := gobatcher.NewAzureSharedResourceMock(10)
		count := 0
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "test:create-partition":
				count++
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if count != 10 {
			t.Errorf("want (10 partitions), have (%v partitions) because factor should be 1", count)
		}
	})

	t.Run("partitions limit is enforced", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000)
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

	t.Run("lease manager provision is called once when provisioning resource", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
			WithFactor(1000)
		count := 0
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "test:provision":
				count++
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if count != 1 {
			t.Errorf("expected a single call to provision")
		}
	})

	t.Run("correct number of partitions are created", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
			WithFactor(1000)
		count := 0
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "test:create-partition":
				count++
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if count != 10 {
			t.Errorf("expected 10 partitions to be created")
		}
	})

	t.Run("partial partitions round up", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10050).
			WithFactor(1000)
		count := 0
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "test:create-partition":
				count++
			}
		})
		if err := res.Provision(ctx); err != nil {
			t.Error(err)
			return
		}
		if count != 11 {
			t.Errorf("expected 11 partitions to be created")
		}
	})

}

func TestMaxCapacity(t *testing.T) {

	t.Run("max-capacity is shared-capacity plus reserved-capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
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
		res := gobatcher.NewAzureSharedResourceMock(10000).
			WithReservedCapacity(2000)
		cap := res.Capacity()
		if cap != 2000 {
			t.Errorf("incorrect capacity; want (2000), have (%v)", cap)
		}
	})

	t.Run("capacity is reserved-capacity plus allocated-capacity", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
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
		res := gobatcher.NewAzureSharedResourceMock(10000).
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

	/*
	t.Run("give-me grants additional capacity above reserve", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
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
*/

/*
	t.Run("give-me does not grant below reserve", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
			WithFactor(1000).
			WithReservedCapacity(2000)
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
		res.GiveMe(4000)
		time.Sleep(2 * time.Second)
		res.GiveMe(0)
		time.Sleep(18 * time.Second)
		if allocated != 2 || released != 2 {
			t.Errorf("want (2 allocations, 2 releases), have(%v allocations, %v releases)", allocated, released)
		}
	})
*/

/*
}

func TestStart(t *testing.T) {
	ctx := context.Background()

	t.Run("start must be called after provision", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000)
		err := res.Start(ctx)
		if e, ok := err.(gobatcher.NotProvisionedError); ok {
			_ = e.Error() // improves code coverage
		} else {
			t.Errorf("expected not provisioned error")
		}
	})

	t.Run("start is callable only once", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
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
		res := gobatcher.NewAzureSharedResourceMock(10000).
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
		res := gobatcher.NewAzureSharedResourceMock(10000).
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
		res := gobatcher.NewAzureSharedResourceMock(10000).
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
		res := gobatcher.NewAzureSharedResourceMock(10000).
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

*/
