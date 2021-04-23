package batcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockBlob struct {
	mock.Mock
}

func (b *mockBlob) Upload(ctx context.Context, reader io.ReadSeeker, headers azblob.BlobHTTPHeaders, metadata azblob.Metadata, conditions azblob.BlobAccessConditions, accessTier azblob.AccessTierType, tags azblob.BlobTagsMap, clientKeyOpts azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobUploadResponse, error) {
	args := b.Called(ctx, reader, headers, metadata, conditions, accessTier, tags, clientKeyOpts)
	return nil, args.Error(1)
}

func (b *mockBlob) AcquireLease(ctx context.Context, proposedId string, duration int32, conditions azblob.ModifiedAccessConditions) (*azblob.BlobAcquireLeaseResponse, error) {
	args := b.Called(ctx, proposedId, duration, conditions)
	return nil, args.Error(1)
}

type mockContainer struct {
	mock.Mock
}

func (c *mockContainer) Create(ctx context.Context, metadata azblob.Metadata, publicAccessType azblob.PublicAccessType) (*azblob.ContainerCreateResponse, error) {
	args := c.Called(ctx, metadata, publicAccessType)
	return nil, args.Error(1)
}

func (c *mockContainer) NewBlockBlobURL(url string) azblob.BlockBlobURL {
	_ = c.Called(url)
	return azblob.BlockBlobURL{}
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

type StorageError struct {
	serviceCode azblob.ServiceCodeType
}

func (e StorageError) ServiceCode() azblob.ServiceCodeType {
	return e.serviceCode
}

func (e StorageError) Error() string {
	return "this is a mock error"
}

func (e StorageError) Timeout() bool {
	return false
}

func (e StorageError) Temporary() bool {
	return false
}

func (e StorageError) Response() *http.Response {
	return nil
}

func TestAzureLeaseManager_ProvisionWithCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", CreatedContainerEvent, mock.Anything, "https://accountName.blob.core.windows.net/containerName", mock.Anything).Once()
	container := &mockContainer{}
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
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
	container.AssertExpectations(t)
	e.AssertExpectations(t)
}

func TestAzureLeaseManager_ProvisionWithVerified(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", VerifiedContainerEvent, mock.Anything, "https://accountName.blob.core.windows.net/containerName", mock.Anything).Once()
	container := &mockContainer{}
	var serr azblob.StorageError = StorageError{serviceCode: azblob.ServiceCodeContainerAlreadyExists}
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil, serr).Once()
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
	container.AssertExpectations(t)
	e.AssertExpectations(t)
}

func TestAzureLeaseManager_ProvisionWithUnrelatedStorageError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	container := &mockContainer{}
	var serr azblob.StorageError = StorageError{serviceCode: azblob.ServiceCodeAccountIsDisabled}
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil, serr).Once()
	accountName := "accountName"
	containerName := "containerName"
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
		container:     container,
	}
	err := mgr.Provision(ctx)
	assert.Equal(t, serr, err)
	container.AssertExpectations(t)
}

func TestAzureLeaseManager_ProvisionWithNonStorageError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	container := &mockContainer{}
	serr := errors.New("non-storage error")
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil, serr).Once()
	accountName := "accountName"
	containerName := "containerName"
	mgr := &azureBlobLeaseManager{
		accountName:   &accountName,
		containerName: &containerName,
		container:     container,
	}
	err := mgr.Provision(ctx)
	assert.Equal(t, serr, err)
	container.AssertExpectations(t)
}

func TestAzureLeaseManager_ProvisionWithInvalidMasterKey(t *testing.T) {
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

func TestAzureLeaseManager_ProvisionWithInvalidUrl(t *testing.T) {
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

func TestAzureLeaseManager_CorrectNumberOfPartitionsCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", CreatedBlobEvent, mock.Anything, mock.Anything, mock.Anything).Times(5)
	blob := &mockBlob{}
	blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil).Times(5)
	mgr := &azureBlobLeaseManager{
		blob: blob,
	}
	mgr.RaiseEventsTo(e)
	mgr.CreatePartitions(ctx, 5)
	blob.AssertExpectations(t)
	e.AssertExpectations(t)
}

func TestAzureLeaseManager_BlobCanBeVerified(t *testing.T) {
	testCases := map[string]azblob.StorageError{
		"exists": StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists},
		"leased": StorageError{serviceCode: azblob.ServiceCodeLeaseIDMissing},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := &mockEventer{}
			e.On("Emit", VerifiedBlobEvent, mock.Anything, mock.Anything, mock.Anything).Once()
			blob := &mockBlob{}
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr).Once()
			mgr := &azureBlobLeaseManager{
				blob: blob,
			}
			mgr.RaiseEventsTo(e)
			mgr.CreatePartitions(ctx, 1)
			blob.AssertExpectations(t)
			e.AssertExpectations(t)
		})
	}
}

func TestAzureLeaseManager_BlobWithErrors(t *testing.T) {
	testCases := map[string]error{
		"unrelated":   StorageError{serviceCode: azblob.ServiceCodeAuthenticationFailed},
		"non-storage": errors.New("non-storage error"),
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := &mockEventer{}
			e.On("Emit", ErrorEvent, mock.Anything, mock.Anything, serr).Once()
			blob := &mockBlob{}
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr).Once()
			mgr := &azureBlobLeaseManager{
				blob: blob,
			}
			mgr.RaiseEventsTo(e)
			mgr.CreatePartitions(ctx, 1)
			blob.AssertExpectations(t)
			e.AssertExpectations(t)
		})
	}
}

func TestAzureLeaseManager_LeaseSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blob := &mockBlob{}
	blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
	mgr := &azureBlobLeaseManager{
		blob: blob,
	}
	dur := mgr.LeasePartition(ctx, "my-lease-id", 0)
	assert.Equal(t, 15*time.Second, dur)
	blob.AssertExpectations(t)
}

func TestAzureLeaseManager_LeaseFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", FailedEvent, mock.Anything, mock.Anything, mock.Anything).Once()
	blob := &mockBlob{}
	serr := StorageError{serviceCode: azblob.ServiceCodeLeaseAlreadyPresent}
	blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, serr).Once()
	mgr := &azureBlobLeaseManager{
		blob: blob,
	}
	mgr.RaiseEventsTo(e)
	dur := mgr.LeasePartition(ctx, "my-lease-id", 0)
	assert.Equal(t, 0*time.Second, dur)
	blob.AssertExpectations(t)
	e.AssertExpectations(t)
}

func TestAzureLeaseManager_LeaseError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", ErrorEvent, mock.Anything, mock.Anything, mock.Anything).Once()
	blob := &mockBlob{}
	unknownErr := fmt.Errorf("unknown mocked error")
	blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, unknownErr).Once()
	mgr := &azureBlobLeaseManager{
		blob: blob,
	}
	mgr.RaiseEventsTo(e)
	dur := mgr.LeasePartition(ctx, "my-lease-id", 0)
	assert.Equal(t, 0*time.Second, dur)
	blob.AssertExpectations(t)
	e.AssertExpectations(t)
}

func TestAzureLeaseManager_LeaseUnrelatedError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", ErrorEvent, mock.Anything, mock.Anything, mock.Anything).Once()
	blob := &mockBlob{}
	unrelatedErr := StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists}
	blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, unrelatedErr).Once()
	mgr := &azureBlobLeaseManager{
		blob: blob,
	}
	mgr.RaiseEventsTo(e)
	dur := mgr.LeasePartition(ctx, "my-lease-id", 0)
	assert.Equal(t, 0*time.Second, dur)
	blob.AssertExpectations(t)
	e.AssertExpectations(t)
}
