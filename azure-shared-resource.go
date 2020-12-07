package batcher

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/google/uuid"
)

type AzureSharedResource struct {
	eventer
	accountName      *string
	masterKey        *string
	containerName    *string
	factor           uint32
	maxCapacity      uint32
	maxInterval      uint32
	reservedCapacity uint32
	capacity         uint32
	container        azblob.ContainerURL
	partitions       []*string
	partlock         sync.RWMutex
	stop             chan bool
	shutdown         sync.WaitGroup
	target           uint32
}

func NewAzureSharedResource() *AzureSharedResource {
	return &AzureSharedResource{}
}

func (r *AzureSharedResource) WithAccount(val string) *AzureSharedResource {
	r.accountName = &val
	return r
}

func (r *AzureSharedResource) WithMasterKey(val string) *AzureSharedResource {
	r.masterKey = &val
	return r
}

func (r *AzureSharedResource) WithContainer(val string) *AzureSharedResource {
	r.containerName = &val
	return r
}

func (r *AzureSharedResource) WithFactor(val uint32) *AzureSharedResource {
	r.factor = val
	return r
}

func (r *AzureSharedResource) WithMaxCapacity(val uint32) *AzureSharedResource {
	r.maxCapacity = val
	return r
}

func (r *AzureSharedResource) WithReservedCapacity(val uint32) *AzureSharedResource {
	r.reservedCapacity = val
	return r
}

func (r *AzureSharedResource) WithMaxInterval(val uint32) *AzureSharedResource {
	r.maxInterval = val
	return r
}

func (r *AzureSharedResource) Provision(ctx context.Context) (err error) {

	// check requirements
	if r.accountName == nil || r.masterKey == nil || r.containerName == nil {
		err = fmt.Errorf("you must supply Account, MasterKey, and Container to provision AzureSharedResource.")
		return
	}
	if r.factor == 0 {
		r.factor = 1 // assume 1:1
	}
	if r.maxCapacity < 1 {
		err = fmt.Errorf("you must specify MaxCapacity for AzureSharedResource.")
		return
	}
	if r.maxInterval < 1 {
		r.maxInterval = 500 // default to 500 ms
	}

	// get a write lock
	r.partlock.Lock()
	defer r.partlock.Unlock()

	// create credential
	credential, err := azblob.NewSharedKeyCredential(*r.accountName, *r.masterKey)
	if err != nil {
		return
	}

	// create the pipeline
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// create the container reference
	url, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", *r.accountName, *r.containerName))
	if err != nil {
		return
	}
	r.container = azblob.NewContainerURL(*url, pipeline)

	// make 1 partition per factor
	count := int(math.Ceil(float64(r.maxCapacity) / float64(r.factor)))
	if count > 500 {
		err = fmt.Errorf("more than 500 partitions is not supported, consider increasing Factor.")
		return
	}
	r.partitions = make([]*string, count)

	// create a blob for each partition
	for i := 0; i < len(r.partitions); i++ {
		blob := r.container.NewBlockBlobURL(fmt.Sprint(i))
		var empty []byte
		reader := bytes.NewReader(empty)
		cond := azblob.BlobAccessConditions{
			ModifiedAccessConditions: azblob.ModifiedAccessConditions{
				IfNoneMatch: "*",
			},
		}
		_, err = blob.Upload(ctx, reader, azblob.BlobHTTPHeaders{}, nil, cond, azblob.AccessTierHot, nil)
		if err != nil {
			if serr, ok := err.(azblob.StorageError); ok {
				switch serr.ServiceCode() {
				case azblob.ServiceCodeBlobAlreadyExists, azblob.ServiceCodeLeaseIDMissing:
					err = nil // these are legit conditions
				default:
					return
				}
			} else {
				return
			}
		}
	}

	return
}

func (r *AzureSharedResource) MaxCapacity() uint32 {
	return r.maxCapacity + r.reservedCapacity
}

func (r *AzureSharedResource) Capacity() uint32 {
	sharedCapacity := atomic.LoadUint32(&r.capacity)
	return sharedCapacity + r.reservedCapacity
}

func (r *AzureSharedResource) calc() (total uint32) {

	// get a read lock
	r.partlock.RLock()
	defer r.partlock.RUnlock()

	// count the allocated partitions
	for i := 0; i < len(r.partitions); i++ {
		if r.partitions[i] != nil {
			total++
		}
	}

	// multiple by the factor
	total *= r.factor

	// set the capacity variable
	atomic.StoreUint32(&r.capacity, total)

	return
}

func (r *AzureSharedResource) GiveMe(target uint32) {

	// reduce capacity request by reserved capacity
	if target >= r.reservedCapacity {
		target -= r.reservedCapacity
	} else {
		target = 0
	}

	// determine the number of partitions needed
	actual := math.Ceil(float64(target) / float64(r.factor))

	// store
	atomic.StoreUint32(&r.target, uint32(actual))

}

func (r *AzureSharedResource) getAllocatedAndRandomUnallocatedPartition() (count, index uint32, err error) {

	// get a read lock
	r.partlock.RLock()
	defer r.partlock.RUnlock()

	// get the list of unallocated
	unallocated := make([]uint32, 0)
	for i := 0; i < len(r.partitions); i++ {
		if r.partitions[i] == nil {
			unallocated = append(unallocated, uint32(i))
		} else {
			count++
		}
	}

	// make sure there is at least 1 unallocated
	len := len(unallocated)
	if len < 1 {
		err = fmt.Errorf("all partitions are already allocated")
		return
	}

	// pick a random partition
	i := rand.Intn(len)
	index = unallocated[i]

	return
}

func (r *AzureSharedResource) setPartitionId(index uint32, id string) {

	// get a write lock
	r.partlock.Lock()
	defer r.partlock.Unlock()

	// set the id
	r.partitions[index] = &id

}

func (r *AzureSharedResource) clearPartitionId(index uint32) {

	// get a write lock
	r.partlock.Lock()
	defer r.partlock.Unlock()

	// clear the id
	r.partitions[index] = nil

}

func (r *AzureSharedResource) Start(ctx context.Context) {

	// calculate capacity change
	recalc := func() {
		go func() {
			capacity := r.calc()
			r.emit("capacity", int(capacity), nil)
		}()
	}

	// prepare for shutdown
	r.shutdown = sync.WaitGroup{}
	r.shutdown.Add(1)
	r.stop = make(chan bool, 1)

	// run the loop to try and allocate resources
	go func() {

		// shutdown
		defer func() {
			r.emit("shutdown", 0, nil)
			r.shutdown.Done()
		}()

	Loop:
		for {

			// check for a stop
			select {
			case <-r.stop:
				return
			default:
				// continue
			}

			// sleep for a bit before trying to obtain a new lease
			interval := rand.Intn(int(r.maxInterval))
			time.Sleep(time.Duration(interval) * time.Millisecond)

			// see how many partitions are allocated and if there any that can be allocated
			count, index, err := r.getAllocatedAndRandomUnallocatedPartition()
			target := atomic.LoadUint32(&r.target)
			if err == nil && count < target {

				// attempt to allocate the partition
				blob := r.container.NewBlockBlobURL(fmt.Sprint(index))
				id := fmt.Sprint(uuid.New())
				_, err := blob.AcquireLease(ctx, id, 15, azblob.ModifiedAccessConditions{})
				if err != nil {
					if serr, ok := err.(azblob.StorageError); ok {
						switch serr.ServiceCode() {
						case azblob.ServiceCodeLeaseAlreadyPresent:
							// you cannot allocate a lease that is already assigned; try again in a bit
							r.emit("failed", int(index), nil)
							continue Loop
						default:
							msg := err.Error()
							r.emit("error", 0, &msg)
						}
					} else {
						msg := err.Error()
						r.emit("error", 0, &msg)
					}
				}

				// clear the partition after the lease
				go func(i uint32) {
					time.Sleep(15 * time.Second)
					r.clearPartitionId(i)
					r.emit("cleared", int(index), nil)
					recalc()
				}(index)

				// mark the partition as allocated
				r.setPartitionId(index, id)
				r.emit("allocated", int(index), nil)
				recalc()

			}

		}
	}()

}

func (r *AzureSharedResource) Stop() {
	r.stop <- true
	r.shutdown.Wait()
}
