package batcher

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type NoAuthenticationProvidedError struct{}

func (e NoAuthenticationProvidedError) Error() string {
	return "you must provide a master key."
}

type azureBlobLeaseManager struct {
	repeater

	// configuration items that should not change after Provision()
	accountName   *string
	masterKey     *string
	containerName *string

	// internal properties
	container azblob.ContainerURL
}

func newAzureBlobLeaseManager(parent ieventer, accountName, containerName string) *azureBlobLeaseManager {
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
	}
	mgr.parent = parent
	return mgr
}

func (m *azureBlobLeaseManager) WithMasterKey(val string) *azureBlobLeaseManager {
	m.masterKey = &val
	return m
}

func (m *azureBlobLeaseManager) provision(ctx context.Context) (err error) {

	// ensure the master key is provided
	if m.masterKey == nil {
		err = NoAuthenticationProvidedError{}
		return
	}

	// create credential
	credential, err := azblob.NewSharedKeyCredential(*m.accountName, *m.masterKey)
	if err != nil {
		return
	}

	// NOTE: managed identity or AAD tokens could be used this way; tested
	//credential := azblob.NewTokenCredential("-access-token-goes-here-", nil)

	// create the pipeline
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// create container if it doesn't exist
	ref := fmt.Sprintf("https://%s.blob.core.windows.net/%s", *m.accountName, *m.containerName)
	url, err := url.Parse(ref)
	if err != nil {
		return
	}
	m.container = azblob.NewContainerURL(*url, pipeline)
	_, err = m.container.Create(ctx, nil, azblob.PublicAccessNone)
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok {
			switch serr.ServiceCode() {
			case azblob.ServiceCodeContainerAlreadyExists:
				err = nil // this is a legit condition
				m.emit("verified-container", 0, &ref)
			default:
				return
			}
		} else {
			return
		}
	} else {
		m.emit("created-container", 0, &ref)
	}

	return
}

func (m *azureBlobLeaseManager) createPartitions(ctx context.Context, count int) (err error) {

	// create a blob for each partition
	for i := 0; i < count; i++ {
		blob := m.container.NewBlockBlobURL(fmt.Sprint(i))
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
					m.emit("verified-blob", i, nil)
				default:
					return
				}
			} else {
				return
			}
		} else {
			m.emit("created-blob", i, nil)
		}
	}

	return
}

func (m *azureBlobLeaseManager) leasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration) {
	var secondsToLease int32 = 15

	// attempt to allocate the partition
	blob := m.container.NewBlockBlobURL(fmt.Sprint(index))
	_, err := blob.AcquireLease(ctx, id, secondsToLease, azblob.ModifiedAccessConditions{})
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok {
			switch serr.ServiceCode() {
			case azblob.ServiceCodeLeaseAlreadyPresent:
				// you cannot allocate a lease that is already assigned; try again in a bit
				m.emit("failed", int(index), nil)
				return
			default:
				msg := err.Error()
				m.emit("error", 0, &msg)
				return
			}
		} else {
			msg := err.Error()
			m.emit("error", 0, &msg)
			return
		}
	}

	// return the lease time
	leaseTime = time.Duration(secondsToLease) * time.Second

	return
}
