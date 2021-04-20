package batcher

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type azureBlobLeaseManager struct {

	// configuration items that should not change after Provision()
	parent        Eventer
	accountName   *string
	masterKey     *string
	containerName *string

	// internal properties
	container AzureContainer
	blob      AzureBlob
}

func NewAzureBlobLeaseManager(accountName, containerName, masterKey string) LeaseManager {
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
		masterKey:     &masterKey,
	}
	return mgr
}

func newMockAzureBlobLeaseManager(accountName, containerName, masterKey string, container AzureContainer, blob AzureBlob) LeaseManager {
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
		masterKey:     &masterKey,
		container:     container,
		blob:          blob,
	}
	return mgr
}

func (m *azureBlobLeaseManager) Parent(e Eventer) {
	m.parent = e
}

func (m *azureBlobLeaseManager) Provision(ctx context.Context) (err error) {

	// choose the appropriate credential
	var credential azblob.Credential
	if m.masterKey != nil {
		credential, err = azblob.NewSharedKeyCredential(*m.accountName, *m.masterKey)
		if err != nil {
			return
		}
	}

	// NOTE: managed identity or AAD tokens could be used this way; tested
	//credential := azblob.NewTokenCredential("-access-token-goes-here-", nil)

	// create pipeline and container reference
	// NOTE: we only check for a mock container at the end to improve code-coverage
	ref := fmt.Sprintf("https://%s.blob.core.windows.net/%s", *m.accountName, *m.containerName)
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	var url *url.URL
	url, err = url.Parse(ref)
	if err != nil {
		return
	}
	if m.container == nil {
		m.container = azblob.NewContainerURL(*url, pipeline)
	}

	// create the container if it doesn't exist
	_, err = m.container.Create(ctx, nil, azblob.PublicAccessNone)
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok {
			switch serr.ServiceCode() {
			case azblob.ServiceCodeContainerAlreadyExists:
				err = nil // this is a legit condition
				m.parent.Emit(VerifiedContainerEvent, 0, ref, nil)
			default:
				return
			}
		} else {
			return
		}
	} else {
		m.parent.Emit(CreatedContainerEvent, 0, ref, nil)
	}

	return
}

func (m *azureBlobLeaseManager) getBlob(index int) AzureBlob {
	if m.blob != nil {
		return m.blob
	} else {
		// NOTE: m.container only exists after provision()
		return m.container.NewBlockBlobURL(fmt.Sprint(index))
	}
}

func (m *azureBlobLeaseManager) CreatePartitions(ctx context.Context, count int) {
	for i := 0; i < count; i++ {
		blob := m.getBlob(i)
		var empty []byte
		reader := bytes.NewReader(empty)
		cond := azblob.BlobAccessConditions{
			ModifiedAccessConditions: azblob.ModifiedAccessConditions{
				IfNoneMatch: "*",
			},
		}
		_, err := blob.Upload(ctx, reader, azblob.BlobHTTPHeaders{}, nil, cond, azblob.AccessTierHot, nil, azblob.ClientProvidedKeyOptions{})
		if err != nil {
			if serr, ok := err.(azblob.StorageError); ok {
				switch serr.ServiceCode() {
				case azblob.ServiceCodeBlobAlreadyExists, azblob.ServiceCodeLeaseIDMissing:
					m.parent.Emit(VerifiedBlobEvent, i, "", nil)
				default:
					m.parent.Emit(ErrorEvent, 0, "creating partitions raised an error", serr)
				}
			} else {
				m.parent.Emit(ErrorEvent, 0, "creating partitions raised an error", err)
			}
		} else {
			m.parent.Emit(CreatedBlobEvent, i, "", nil)
		}
	}
}

func (m *azureBlobLeaseManager) LeasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration) {
	secondsToLease := 15

	// attempt to allocate the partition
	blob := m.getBlob(int(index))
	_, err := blob.AcquireLease(ctx, id, int32(secondsToLease), azblob.ModifiedAccessConditions{})
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok {
			switch serr.ServiceCode() {
			case azblob.ServiceCodeLeaseAlreadyPresent:
				// you cannot allocate a lease that is already assigned; try again in a bit
				m.parent.Emit(FailedEvent, int(index), "", nil)
				return
			default:
				m.parent.Emit(ErrorEvent, 0, err.Error(), nil)
				return
			}
		} else {
			m.parent.Emit(ErrorEvent, 0, err.Error(), nil)
			return
		}
	}

	// return the lease time
	leaseTime = time.Duration(secondsToLease) * time.Second

	return
}
