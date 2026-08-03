package batcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockBlob struct {
	mock.Mock
}

func (b *mockBlob) Upload(ctx context.Context, body io.ReadSeekCloser, o *blockblob.UploadOptions) (blockblob.UploadResponse, error) {
	args := b.Called(ctx, body, o)
	return blockblob.UploadResponse{}, args.Error(1)
}

func (b *mockBlob) AcquireLease(ctx context.Context, proposedID string, duration int32, o *lease.BlobAcquireOptions) (lease.BlobAcquireResponse, error) {
	args := b.Called(ctx, proposedID, duration, o)
	return lease.BlobAcquireResponse{}, args.Error(1)
}

type mockContainer struct {
	mock.Mock
}

func (c *mockContainer) Create(ctx context.Context, o *container.CreateOptions) (container.CreateResponse, error) {
	args := c.Called(ctx, o)
	return container.CreateResponse{}, args.Error(1)
}

func (c *mockContainer) NewBlockBlobClient(blobName string) azureBlob {
	_ = c.Called(blobName)
	return nil
}

type mockEventer struct {
	mock.Mock
}

func (sr *mockEventer) AddListener(fn func(event string, val int, msg string, metadata interface{})) uuid.UUID {
	args := sr.Called(fn)
	return args.Get(0).(uuid.UUID)
}

func (sr *mockEventer) RemoveListener(id uuid.UUID) {
	sr.Called(id)
}

func (sr *mockEventer) Emit(event string, val int, msg string, metadata interface{}) {
	sr.Called(event, val, msg, metadata)
}

// mockStorageError builds an error that mimics the *azcore.ResponseError the Track 2 Azure SDK
// returns for a given blob storage error code, so the bloberror.HasCode(...) checks in the
// production code can be exercised without a live storage account.
func mockStorageError(code bloberror.Code) error {
	return &azcore.ResponseError{ErrorCode: string(code)}
}

func TestAzureBlobLeaseManager_Provision_ContainerIsCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", CreatedContainerEvent, mock.Anything, "https://accountName.blob.core.windows.net/containerName", mock.Anything)
	container := &mockContainer{}
	container.On("Create", mock.Anything, mock.Anything).Return(nil, nil).Once()
	accountName := "accountName"
	containerName := "containerName"
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
		container:     container,
	}
	mgr.RaiseEventsTo(e)
	err := mgr.Provision(ctx)
	assert.NoError(t, err, "expecting no provision error")
	container.AssertNumberOfCalls(t, "Create", 1)
	e.AssertNumberOfCalls(t, "Emit", 1)
}

func TestAzureBlobLeaseManager_Provision_ContainerIsVerified(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", VerifiedContainerEvent, mock.Anything, "https://accountName.blob.core.windows.net/containerName", mock.Anything)
	container := &mockContainer{}
	serr := mockStorageError(bloberror.ContainerAlreadyExists)
	container.On("Create", mock.Anything, mock.Anything).Return(nil, serr).Once()
	accountName := "accountName"
	containerName := "containerName"
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
		container:     container,
	}
	mgr.RaiseEventsTo(e)
	err := mgr.Provision(ctx)
	assert.NoError(t, err, "expecting no provision error")
	container.AssertNumberOfCalls(t, "Create", 1)
	e.AssertNumberOfCalls(t, "Emit", 1)
}

func TestAzureBlobLeaseManager_Provision_Errors(t *testing.T) {
	testCases := map[string]struct {
		err error
	}{
		"unknown":     {err: mockStorageError(bloberror.AccountIsDisabled)},
		"non-storage": {err: errors.New("non-storage error")},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container := &mockContainer{}
			container.On("Create", mock.Anything, mock.Anything).Return(nil, testCase.err)
			accountName := "accountName"
			containerName := "containerName"
			mgr := &azureBlobLeaseManager{
				accountName:   &accountName,
				containerName: &containerName,
				container:     container,
			}
			err := mgr.Provision(ctx)
			assert.Equal(t, testCase.err, err)
			container.AssertNumberOfCalls(t, "Create", 1)
		})
	}
}

func TestAzureBlobLeaseManager_Provision_InvalidMasterKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accountName := "accountName"
	containerName := "containerName"
	masterKey := "invalid"
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
		masterKey:     &masterKey,
	}
	err := mgr.Provision(ctx)
	assert.Contains(t, err.Error(), "illegal base64 data")
}

func TestAzureBlobLeaseManager_Provision_InvalidUrl(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accountName := "accoun\tName"
	containerName := "containerName"
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
	}
	err := mgr.Provision(ctx)
	assert.Contains(t, err.Error(), "invalid control character in URL")
}

func TestAzureBlobLeaseManager_CreatePartitions_CorrectNumberCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", CreatedBlobEvent, mock.Anything, mock.Anything, mock.Anything)
	blob := &mockBlob{}
	blob.On("Upload", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil).Times(5)
	mgr := &azureBlobLeaseManager{
		blob: blob,
	}
	mgr.RaiseEventsTo(e)
	mgr.CreatePartitions(ctx, 5)
	blob.AssertNumberOfCalls(t, "Upload", 5)
	e.AssertNumberOfCalls(t, "Emit", 5)
}

func TestAzureBlobLeaseManager_CreatePartitions_BlobIsVerified(t *testing.T) {
	testCases := map[string]bloberror.Code{
		"exists": bloberror.BlobAlreadyExists,
		"leased": bloberror.LeaseIDMissing,
	}
	for testName, code := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := &mockEventer{}
			e.On("Emit", VerifiedBlobEvent, mock.Anything, mock.Anything, mock.Anything)
			blob := &mockBlob{}
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything).
				Return(nil, mockStorageError(code)).Once()
			mgr := &azureBlobLeaseManager{
				blob: blob,
			}
			mgr.RaiseEventsTo(e)
			mgr.CreatePartitions(ctx, 1)
			blob.AssertNumberOfCalls(t, "Upload", 1)
			e.AssertNumberOfCalls(t, "Emit", 1)
		})
	}
}

func TestAzureBlobLeaseManager_CreatePartitions_BlobErrors(t *testing.T) {
	testCases := map[string]error{
		"unknown":     mockStorageError(bloberror.AuthenticationFailed),
		"non-storage": errors.New("non-storage error"),
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := &mockEventer{}
			e.On("Emit", ErrorEvent, mock.Anything, mock.Anything, serr)
			blob := &mockBlob{}
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr).Once()
			mgr := &azureBlobLeaseManager{
				blob: blob,
			}
			mgr.RaiseEventsTo(e)
			mgr.CreatePartitions(ctx, 1)
			blob.AssertNumberOfCalls(t, "Upload", 1)
			e.AssertNumberOfCalls(t, "Emit", 1)
		})
	}
}

func TestAzureBlobLeaseManager_LeasePartition_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blob := &mockBlob{}
	blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mgr := &azureBlobLeaseManager{
		blob: blob,
	}
	dur := mgr.LeasePartition(ctx, "my-lease-id", 0)
	assert.Equal(t, 15*time.Second, dur)
	blob.AssertNumberOfCalls(t, "AcquireLease", 1)
}

func TestAzureBlobLeaseManager_LeasePartition_Failures(t *testing.T) {
	testCases := map[string]struct {
		event string
		err   error
	}{
		"failed to obtain lease": {event: FailedEvent, err: mockStorageError(bloberror.LeaseAlreadyPresent)},
		"unknown":                {event: ErrorEvent, err: mockStorageError(bloberror.BlobAlreadyExists)},
		"non-storage":            {event: ErrorEvent, err: fmt.Errorf("unknown mocked error")},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := &mockEventer{}
			e.On("Emit", testCase.event, mock.Anything, mock.Anything, mock.Anything)
			blob := &mockBlob{}
			blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, testCase.err)
			mgr := &azureBlobLeaseManager{
				blob: blob,
			}
			mgr.RaiseEventsTo(e)
			dur := mgr.LeasePartition(ctx, "my-lease-id", 0)
			assert.Equal(t, 0*time.Second, dur)
			blob.AssertNumberOfCalls(t, "AcquireLease", 1)
			e.AssertNumberOfCalls(t, "Emit", 1)
		})
	}
}
