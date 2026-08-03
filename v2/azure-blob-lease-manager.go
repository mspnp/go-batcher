package batcher

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
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

	// configuration items that should not change after Provision()
	eventer       Eventer
	accountName   *string
	masterKey     *string
	containerName *string

	// internal properties
	container azureContainer
	blob      azureBlob
}

// This method creates a new AzureBlobLeaseManager to allow the SharedResource to use Azure Blob Storage to manage leases
// across instances. You must provide an Azure Storage accountName and containerName. By default, the manager
// authenticates using Microsoft Entra ID via DefaultAzureCredential (which supports Managed Identity, Azure CLI,
// Workload Identity, and other environment-based credentials). Call WithMasterKey(key) for the legacy Storage
// Account shared-key credential path.
func NewAzureBlobLeaseManager(accountName, containerName string) *azureBlobLeaseManager {
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
	}
	return mgr
}

// WithMasterKey configures the lease manager to authenticate with a Storage Account shared key instead of the
// default Microsoft Entra ID (DefaultAzureCredential) path. This is a secondary/legacy credential option; prefer
// Managed Identity or another Entra ID-based credential where possible.
func (m *azureBlobLeaseManager) WithMasterKey(val string) *azureBlobLeaseManager {
	m.masterKey = &val
	return m
}

// Events raised by AzureBlobLeaseManager must be raised to an Eventer. Specifically the SharedResource it is associated with
// will be used as the Eventer. This method is called in SharedResource.WithSharedCapacity().
func (m *azureBlobLeaseManager) RaiseEventsTo(e Eventer) {
	m.eventer = e
}

// This is called by SharedResource when the Azure Blob Storage Container should be created or verified.
func (m *azureBlobLeaseManager) Provision(ctx context.Context) (err error) {

	// NOTE: we only check for a mock container at the end to improve code-coverage
	ref := fmt.Sprintf("https://%s.blob.core.windows.net/%s", *m.accountName, *m.containerName)
	if m.container == nil {
		var parsed *url.URL
		parsed, err = url.Parse(ref)
		if err != nil {
			return
		}
		var client *container.Client
		if m.masterKey != nil {
			// secondary/legacy credential path: Storage Account shared key
			var credential *container.SharedKeyCredential
			credential, err = container.NewSharedKeyCredential(*m.accountName, *m.masterKey)
			if err != nil {
				return
			}
			client, err = container.NewClientWithSharedKeyCredential(parsed.String(), credential, nil)
		} else {
			// default/first-class credential path: Microsoft Entra ID via DefaultAzureCredential, which
			// supports Managed Identity, Azure CLI, Workload Identity, and other environment-based credentials
			var credential *azidentity.DefaultAzureCredential
			credential, err = azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				return
			}
			client, err = container.NewClient(parsed.String(), credential, nil)
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
			m.eventer.Emit(VerifiedContainerEvent, 0, ref, nil)
		} else {
			return
		}
	} else {
		m.eventer.Emit(CreatedContainerEvent, 0, ref, nil)
	}

	return
}

func (m *azureBlobLeaseManager) getBlob(index int) azureBlob {
	if m.blob != nil {
		return m.blob
	} else {
		// NOTE: m.container only exists after provision()
		return m.container.NewBlockBlobClient(fmt.Sprint(index))
	}
}

// This is called by SharedResource when the Azure Blob Storage blobs (partitions) should be created or verified.
func (m *azureBlobLeaseManager) CreatePartitions(ctx context.Context, count int) {
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
		_, err := b.Upload(ctx, reader, opts)
		if err != nil {
			if bloberror.HasCode(err, bloberror.BlobAlreadyExists, bloberror.LeaseIDMissing) {
				m.eventer.Emit(VerifiedBlobEvent, i, "", nil)
			} else {
				m.eventer.Emit(ErrorEvent, 0, "creating partitions raised an error", err)
			}
		} else {
			m.eventer.Emit(CreatedBlobEvent, i, "", nil)
		}
	}
}

// This is called by SharedResource when it needs to lease partitions for capacity.
func (m *azureBlobLeaseManager) LeasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration) {
	var secondsToLease int32 = 15

	// attempt to allocate the partition
	b := m.getBlob(int(index))
	_, err := b.AcquireLease(ctx, id, secondsToLease, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.LeaseAlreadyPresent) {
			// you cannot allocate a lease that is already assigned; try again in a bit
			m.eventer.Emit(FailedEvent, int(index), "", nil)
			return
		}
		m.eventer.Emit(ErrorEvent, 0, err.Error(), nil)
		return
	}

	// return the lease time
	leaseTime = time.Duration(secondsToLease) * time.Second

	return
}
