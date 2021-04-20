package batcher

import (
	"context"
	"io"
	"testing"

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

func TestAzureLeaseManager_Provision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", "created-container", mock.Anything, "https://accountName.blob.core.windows.net/containerName", mock.Anything).Once()
	container := &mockContainer{}
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
	mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "", container, nil)
	mgr.Parent(e)
	err := mgr.Provision(ctx)
	assert.NoError(t, err, "expecting no provision error")
	container.AssertExpectations(t)
	e.AssertExpectations(t)
}

func TestAzureLeaseManager_ProvisionWithInvalidMasterKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "masterKey", nil, nil)
	err := mgr.Provision(ctx)
	assert.Contains(t, err.Error(), "illegal base64 data")
}

func TestAzureLeaseManager_CorrectNumberOfPartitionsCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &mockEventer{}
	e.On("Emit", "created-blob", mock.Anything, mock.Anything, mock.Anything).Times(5)
	blob := &mockBlob{}
	blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil).Times(5)
	mgr := newMockAzureBlobLeaseManager("accountName", "containerName", "masterKey", nil, blob)
	mgr.Parent(e)
	mgr.CreatePartitions(ctx, 5)
	blob.AssertExpectations(t)
	e.AssertExpectations(t)
}

/*

"github.com/Azure/azure-storage-blob-go/azblob"


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


func TestAzureSRStart_ContainerCanBeCreated(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000)
	var created int
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case "created-container":
			created += 1
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	assert.Equal(t, 1, created, "expecting a single created event")
}

func TestAzureSRStart_ContainerCanBeVerified(t *testing.T) {
	var serr azblob.StorageError = StorageError{serviceCode: azblob.ServiceCodeContainerAlreadyExists}
	container := new(containerURLMock)
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, serr)
	_, blob := getMocks()
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(container, blob).
		WithFactor(1000)
	var verified int
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case "verified-container":
			verified += 1
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	assert.Equal(t, 1, verified, "expecting a single verified event")
}

func TestAzureSRStart_BlobCanBeCreated(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000)
	var wg sync.WaitGroup
	wg.Add(1)
	var created int
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CreatedBlobEvent:
			created += 1
		case gobatcher.ProvisionDoneEvent:
			wg.Done()
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, 10, created, "expecting a creation event per partition")
}

func TestAzureSRStart_BlobCanBeVerified(t *testing.T) {
	testCases := map[string]azblob.StorageError{
		"exists": StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists},
		"leased": StorageError{serviceCode: azblob.ServiceCodeLeaseIDMissing},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			container, _ := getMocks()
			blob := new(blockBlobURLMock)
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr)
			res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
				WithMocks(container, blob).
				WithFactor(1000)
			var wg sync.WaitGroup
			wg.Add(1)
			var verified int
			res.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case gobatcher.VerifiedBlobEvent:
					verified += 1
				case gobatcher.ProvisionDoneEvent:
					wg.Done()
				}
			})
			err := res.Start(context.Background())
			assert.NoError(t, err, "not expecting a start error")
			wg.Wait()
			assert.Equal(t, 10, verified, "expecting a verified event per partition")
		})
	}
}

*/

/*
func TestAzureSRStart(t *testing.T) {
	ctx := context.Background()

	t.Run("start can lease, fail, and handle errors", func(t *testing.T) {
		serr := StorageError{serviceCode: azblob.ServiceCodeLeaseAlreadyPresent}
		unknown := fmt.Errorf("unknown mocked error")
		unrelated := StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists}
		container, _ := getMocks()
		blob := new(blockBlobURLMock)
		blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, nil)
		blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, unknown).
			Once()
		blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, serr).
			Once()
		blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, unrelated).
			Once()
		blob.On("AcquireLease", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, nil).
			Once()
		res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
			WithMocks(container, blob).
			WithFactor(1000).
			WithMaxInterval(1)
		var allocated, failed, errored uint32
		res.AddListener(func(event string, val int, msg string, metadata interface{}) {
			switch event {
			case gobatcher.AllocatedEvent:
				atomic.AddUint32(&allocated, 1)
			case gobatcher.FailedEvent:
				atomic.AddUint32(&failed, 1)
			case gobatcher.ErrorEvent:
				atomic.AddUint32(&errored, 1)
			}
		})
		err := res.Start(ctx)
		assert.NoError(t, err, "not expecting a start error")
		res.GiveMe(500)
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, uint32(1), atomic.LoadUint32(&allocated), "expecting 1 allocation")
		assert.Equal(t, uint32(1), atomic.LoadUint32(&failed), "expecting 1 failed (already leased)")
		assert.Equal(t, uint32(2), atomic.LoadUint32(&errored), "expecting 2 errors (unknown and unrelated)")
	})
*/

/*
func TestAzureSRStart_ContainerErrorsCascade(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testCases := map[string]error{
		"unknown error": fmt.Errorf("unknown mocked error"),
		"storage error": StorageError{serviceCode: azblob.ServiceCodeAppendPositionConditionNotMet},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			mgr := &mockLeaseManager{}
			mgr.On("Provision", mock.Anything).Return(serr).Once()
			res := gobatcher.NewSharedResource("accountName", "containerName").
				WithSharedCapacity(10000, mgr).
				WithFactor(1000)
			err := res.Start(ctx)
			assert.Equal(t, serr, err, "expecting the error to be thrown back from start")
			mgr.AssertExpectations(t)
		})
	}
}

func TestAzureSRStart_BlobErrorsCascade(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testCases := map[string]error{
		"unknown error": fmt.Errorf("unknown mocked error"),
		"storage error": StorageError{serviceCode: azblob.ServiceCodeBlobArchived},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			mgr := &mockLeaseManager{}
			mgr.On("Provision", mock.Anything).Return(nil).Once()
			mgr.On("CreatePartitions", mock.Anything, 10).Return(serr).Once()
			res := gobatcher.NewSharedResource("accountName", "containerName").
				WithSharedCapacity(10000, mgr).
				WithFactor(1000)
			var wg sync.WaitGroup
			wg.Add(1)
			res.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case gobatcher.ErrorEvent:
					assert.Equal(t, serr, metadata)
					wg.Done()
				}
			})
			err := res.Start(ctx)
			assert.NoError(t, err, "not expecting a start error")
			wg.Wait()
			mgr.AssertExpectations(t)
		})
	}
}
*/
