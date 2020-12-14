package batcher

import (
	"context"
	"io"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type IAzureContainer interface {
	Create(context.Context, azblob.Metadata, azblob.PublicAccessType) (*azblob.ContainerCreateResponse, error)
	NewBlockBlobURL(string) azblob.BlockBlobURL
}

type IAzureBlob interface {
	Upload(context.Context, io.ReadSeeker, azblob.BlobHTTPHeaders, azblob.Metadata, azblob.BlobAccessConditions, azblob.AccessTierType, azblob.BlobTagsMap) (*azblob.BlockBlobUploadResponse, error)
	AcquireLease(context.Context, string, int32, azblob.ModifiedAccessConditions) (*azblob.BlobAcquireLeaseResponse, error)
}
