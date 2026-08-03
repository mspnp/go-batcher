package batcher

import (
	"context"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
)

// This interface describes an Azure Storage Container that can be mocked.
type azureContainer interface {
	Create(context.Context, *container.CreateOptions) (container.CreateResponse, error)
	NewBlockBlobClient(string) azureBlob
}

// This interface describes an Azure Storage Blob that can be mocked.
type azureBlob interface {
	Upload(context.Context, io.ReadSeekCloser, *blockblob.UploadOptions) (blockblob.UploadResponse, error)
	AcquireLease(context.Context, string, int32, *lease.BlobAcquireOptions) (lease.BlobAcquireResponse, error)
}

// azureContainerClient adapts a real *container.Client to the azureContainer interface.
type azureContainerClient struct {
	client *container.Client
}

func (c *azureContainerClient) Create(ctx context.Context, o *container.CreateOptions) (container.CreateResponse, error) {
	return c.client.Create(ctx, o)
}

func (c *azureContainerClient) NewBlockBlobClient(blobName string) azureBlob {
	return &azureBlockBlobClient{client: c.client.NewBlockBlobClient(blobName)}
}

// azureBlockBlobClient adapts a real *blockblob.Client to the azureBlob interface. AcquireLease hides
// the Track 2 SDK's separate lease-client construction so callers keep the simpler "proposed lease ID"
// calling convention used throughout this package.
type azureBlockBlobClient struct {
	client *blockblob.Client
}

func (b *azureBlockBlobClient) Upload(ctx context.Context, body io.ReadSeekCloser, o *blockblob.UploadOptions) (blockblob.UploadResponse, error) {
	return b.client.Upload(ctx, body, o)
}

func (b *azureBlockBlobClient) AcquireLease(ctx context.Context, proposedID string, duration int32, o *lease.BlobAcquireOptions) (lease.BlobAcquireResponse, error) {
	leaseClient, err := lease.NewBlobClient(b.client, &lease.BlobClientOptions{LeaseID: &proposedID})
	if err != nil {
		return lease.BlobAcquireResponse{}, err
	}
	return leaseClient.AcquireLease(ctx, duration, o)
}
