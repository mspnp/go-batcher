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

func TestAzureBlobLeaseManager_Provision_ContainerIsCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", CreatedContainerEvent, mock.Anything, "https://accountName.blob.core.windows.net/containerName", mock.Anything)
	container := &mockContainer{}
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "", container, nil)
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
	var serr azblob.StorageError = StorageError{serviceCode: azblob.ServiceCodeContainerAlreadyExists}
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil, serr)
	mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "", container, nil)
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
		"unknown":     {err: StorageError{serviceCode: azblob.ServiceCodeAccountIsDisabled}},
		"non-storage": {err: errors.New("non-storage error")},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			container := &mockContainer{}
			container.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil, testCase.err)
			mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "", container, nil)
			err := mgr.Provision(ctx)
			assert.Equal(t, testCase.err, err)
			container.AssertNumberOfCalls(t, "Create", 1)
		})
	}
}

func TestAzureBlobLeaseManager_Provision_InvalidMasterKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "masterKey", nil, nil)
	err := mgr.Provision(ctx)
	assert.Contains(t, err.Error(), "illegal base64 data")
}

func TestAzureBlobLeaseManager_Provision_InvalidUrl(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr := newMockAzureBlobLeaseManager("accoun\tName", "containerName", "", nil, nil)
	err := mgr.Provision(ctx)
	assert.Contains(t, err.Error(), "invalid control character in URL")
}

func TestAzureBlobLeaseManager_CreatePartitions_CorrectNumberCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", CreatedBlobEvent, mock.Anything, mock.Anything, mock.Anything)
	blob := &mockBlob{}
	blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "", nil, blob)
	mgr.RaiseEventsTo(e)
	mgr.CreatePartitions(ctx, 5)
	blob.AssertNumberOfCalls(t, "Upload", 5)
	e.AssertNumberOfCalls(t, "Emit", 5)
}

func TestAzureBlobLeaseManager_CreatePartitions_BlobIsVerified(t *testing.T) {
	testCases := map[string]azblob.StorageError{
		"exists": StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists},
		"leased": StorageError{serviceCode: azblob.ServiceCodeLeaseIDMissing},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := &mockEventer{}
			e.On("Emit", VerifiedBlobEvent, mock.Anything, mock.Anything, mock.Anything)
			blob := &mockBlob{}
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr)
			mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "", nil, blob)
			mgr.RaiseEventsTo(e)
			mgr.CreatePartitions(ctx, 1)
			blob.AssertNumberOfCalls(t, "Upload", 1)
			e.AssertNumberOfCalls(t, "Emit", 1)
		})
	}
}

func TestAzureBlobLeaseManager_CreatePartitions_BlobErrors(t *testing.T) {
	testCases := map[string]error{
		"unknown":     StorageError{serviceCode: azblob.ServiceCodeAuthenticationFailed},
		"non-storage": errors.New("non-storage error"),
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := &mockEventer{}
			e.On("Emit", ErrorEvent, mock.Anything, mock.Anything, serr)
			blob := &mockBlob{}
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr)
			mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "", nil, blob)
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
	mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "", nil, blob)
	dur := mgr.LeasePartition(ctx, "my-lease-id", 0)
	assert.Equal(t, 15*time.Second, dur)
	blob.AssertNumberOfCalls(t, "AcquireLease", 1)
}

func TestAzureBlobLeaseManager_LeasePartition_Failures(t *testing.T) {
	testCases := map[string]struct {
		event string
		err   error
	}{
		"failed to obtain lease": {event: FailedEvent, err: StorageError{serviceCode: azblob.ServiceCodeLeaseAlreadyPresent}},
		"unknown":                {event: ErrorEvent, err: StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists}},
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
			mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "", nil, blob)
			mgr.RaiseEventsTo(e)
			dur := mgr.LeasePartition(ctx, "my-lease-id", 0)
			assert.Equal(t, 0*time.Second, dur)
			blob.AssertNumberOfCalls(t, "AcquireLease", 1)
			e.AssertNumberOfCalls(t, "Emit", 1)
		})
	}
}
