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
	repeater

	// configuration items that should not change after Provision()
	accountName   *string
	masterKey     *string
	containerName *string

	// internal properties
	container  AzureContainer
	blob       AzureBlob
	mocksInUse bool
}

func newAzureBlobLeaseManager(parent ieventer, accountName, containerName string) *azureBlobLeaseManager {
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
	}
	mgr.parent = parent
	return mgr
}

func (m *azureBlobLeaseManager) withMocks(container azureContainer, blob azureBlob) *azureBlobLeaseManager {
	m.container = container
	m.blob = blob
	m.mocksInUse = true
	return m
}

func (m *azureBlobLeaseManager) withMasterKey(val string) *azureBlobLeaseManager {
	m.masterKey = &val
	return m
}

func (m *azureBlobLeaseManager) provision(ctx context.Context) (err error) {

	// choose the appropriate credential
	var credential azblob.Credential
	if m.masterKey != nil {
		credential, err = azblob.NewSharedKeyCredential(*m.accountName, *m.masterKey)
		if err != nil {
			return
		}
	} else {
		credential = azblob.NewAnonymousCredential()
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
				m.emit(VerifiedContainerEvent, 0, ref, nil)
			default:
				return
			}
		} else {
			return
		}
	} else {
		m.emit(CreatedContainerEvent, 0, ref, nil)
	}

	return
}

func (m *azureBlobLeaseManager) getBlob(index int) azureBlob {
	if m.blob != nil {
		return m.blob
	} else {
		// NOTE: m.container only exists after provision()
		return m.container.NewBlockBlobURL(fmt.Sprint(index))
	}
}

func (m *azureBlobLeaseManager) createPartitions(ctx context.Context, count int) (err error) {

	// create a blob for each partition
	for i := 0; i < count; i++ {
		blob := m.getBlob(i)
		var empty []byte
		reader := bytes.NewReader(empty)
		cond := azblob.BlobAccessConditions{
			ModifiedAccessConditions: azblob.ModifiedAccessConditions{
				IfNoneMatch: "*",
			},
		}
		_, err = blob.Upload(ctx, reader, azblob.BlobHTTPHeaders{}, nil, cond, azblob.AccessTierHot, nil, azblob.ClientProvidedKeyOptions{})
		if err != nil {
			if serr, ok := err.(azblob.StorageError); ok {
				switch serr.ServiceCode() {
				case azblob.ServiceCodeBlobAlreadyExists, azblob.ServiceCodeLeaseIDMissing:
					err = nil // these are legit conditions
					m.emit(VerifiedBlobEvent, i, "", nil)
				default:
					return
				}
			} else {
				return
			}
		} else {
			m.emit(CreatedBlobEvent, i, "", nil)
		}
	}

	return
}

func (m *azureBlobLeaseManager) leasePartition(ctx context.Context, id string, index uint32, secondsToLease uint32) (leaseTime time.Duration) {

	// constrain the secondsToLease (Azure only supports 15-60 seconds)
	switch {
	case !m.mocksInUse && secondsToLease < 15:
		secondsToLease = 15
	case !m.mocksInUse && secondsToLease > 60:
		secondsToLease = 60
	}

	// attempt to allocate the partition
	blob := m.getBlob(int(index))
	_, err := blob.AcquireLease(ctx, id, int32(secondsToLease), azblob.ModifiedAccessConditions{})
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok {
			switch serr.ServiceCode() {
			case azblob.ServiceCodeLeaseAlreadyPresent:
				// you cannot allocate a lease that is already assigned; try again in a bit
				m.emit(FailedEvent, int(index), "", nil)
				return
			default:
				m.emit(ErrorEvent, 0, err.Error(), nil)
				return
			}
		} else {
			m.emit(ErrorEvent, 0, err.Error(), nil)
			return
		}
	}

	// return the lease time
	leaseTime = time.Duration(secondsToLease) * time.Second

	return
}
