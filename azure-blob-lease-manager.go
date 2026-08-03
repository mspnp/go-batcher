package batcher

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

type azureBlobLeaseManager struct {
	repeater

	// configuration items that should not change after Provision()
	accountName   *string
	masterKey     *string
	containerName *string

	// internal properties
	container IAzureContainer
	blob      IAzureBlob
}

func newAzureBlobLeaseManager(parent ieventer, accountName, containerName string) *azureBlobLeaseManager {
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
	}
	mgr.parent = parent
	return mgr
}

func (m *azureBlobLeaseManager) withMocks(container IAzureContainer, blob IAzureBlob) *azureBlobLeaseManager {
	m.container = container
	m.blob = blob
	return m
}

func (m *azureBlobLeaseManager) withMasterKey(val string) *azureBlobLeaseManager {
	m.masterKey = &val
	return m
}

func (m *azureBlobLeaseManager) provision(ctx context.Context) (err error) {

	// NOTE: we only check for a mock container at the end to improve code-coverage
	ref := fmt.Sprintf("https://%s.blob.core.windows.net/%s", *m.accountName, *m.containerName)
	if m.container == nil {
		var client *container.Client
		if m.masterKey != nil {
			// secondary/legacy credential path: Storage Account shared key
			var credential *container.SharedKeyCredential
			credential, err = container.NewSharedKeyCredential(*m.accountName, *m.masterKey)
			if err != nil {
				return
			}
			client, err = container.NewClientWithSharedKeyCredential(ref, credential, nil)
		} else {
			// default/first-class credential path: Microsoft Entra ID via DefaultAzureCredential, which
			// supports Managed Identity, Azure CLI, Workload Identity, and other environment-based credentials
			var credential *azidentity.DefaultAzureCredential
			credential, err = azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				return
			}
			client, err = container.NewClient(ref, credential, nil)
		}
		if err != nil {
			return
		}
		m.container = &azureContainerClient{client: client}
	}

	// create the container if it doesn't exist
	_, err = m.container.Create(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
			err = nil // this is a legit condition
			m.emit(VerifiedContainerEvent, 0, ref, nil)
		} else {
			return
		}
	} else {
		m.emit(CreatedContainerEvent, 0, ref, nil)
	}

	return
}

func (m *azureBlobLeaseManager) getBlob(index int) IAzureBlob {
	if m.blob != nil {
		return m.blob
	} else {
		// NOTE: m.container only exists after provision()
		return m.container.NewBlockBlobClient(fmt.Sprint(index))
	}
}

func (m *azureBlobLeaseManager) createPartitions(ctx context.Context, count int) (err error) {

	// create a blob for each partition
	for i := 0; i < count; i++ {
		b := m.getBlob(i)
		var empty []byte
		reader := streaming.NopCloser(bytes.NewReader(empty))
		opts := &blockblob.UploadOptions{
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfNoneMatch: to.Ptr(azcore.ETag("*")),
				},
			},
		}
		_, err = b.Upload(ctx, reader, opts)
		if err != nil {
			if bloberror.HasCode(err, bloberror.BlobAlreadyExists, bloberror.LeaseIDMissing) {
				err = nil // these are legit conditions
				m.emit(VerifiedBlobEvent, i, "", nil)
			} else {
				return
			}
		} else {
			m.emit(CreatedBlobEvent, i, "", nil)
		}
	}

	return
}

func (m *azureBlobLeaseManager) leasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration) {
	var secondsToLease int32 = 15

	// attempt to allocate the partition
	b := m.getBlob(int(index))
	_, err := b.AcquireLease(ctx, id, secondsToLease, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.LeaseAlreadyPresent) {
			// you cannot allocate a lease that is already assigned; try again in a bit
			m.emit(FailedEvent, int(index), "", nil)
			return
		}
		m.emit(ErrorEvent, 0, err.Error(), nil)
		return
	}

	// return the lease time
	leaseTime = time.Duration(secondsToLease) * time.Second

	return
}
