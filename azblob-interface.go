package batcher

import (
	"context"
	"io"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

/*
type IAzureStorage interface {
	NewContainerURL(url.URL, pipeline.Pipeline) azblob.ContainerURL
}
*/

type IAzureContainer interface {
	Create(context.Context, azblob.Metadata, azblob.PublicAccessType) (*azblob.ContainerCreateResponse, error)
	NewBlockBlobURL(string) azblob.BlockBlobURL
	//NewBlockBlobURL(string) IAzureBlob
}

type IAzureBlob interface {
	Upload(context.Context, io.ReadSeeker, azblob.BlobHTTPHeaders, azblob.Metadata, azblob.BlobAccessConditions, azblob.AccessTierType, azblob.BlobTagsMap) (*azblob.BlockBlobUploadResponse, error)
	AcquireLease(context.Context, string, int32, azblob.ModifiedAccessConditions) (*azblob.BlobAcquireLeaseResponse, error)
}

/*
type AzureStorage struct {
	container IAzureContainer
}

func (a *AzureStorage) NewContainerURL(u url.URL, p pipeline.Pipeline) IAzureContainer {
	if a.container == nil {
		c := azblob.NewContainerURL(u, p)
		return &AzureContainer{
			containerURL: c,
		}
	} else {
		return a.container
	}
}

type AzureContainer struct {
	containerURL azblob.ContainerURL
}

func (a *AzureContainer) Create(ctx context.Context, metadata azblob.Metadata, publicAccessType azblob.PublicAccessType) (*azblob.ContainerCreateResponse, error) {
	return a.containerURL.Create(ctx, metadata, publicAccessType)
}

func (a *AzureContainer) NewBlockBlobURL(url string) IAzureBlob {
	return a.containerURL.NewBlockBlobURL(url)
}
*/
