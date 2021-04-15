# Unit Testing

This section documents ways to facilitate unit testing when using Batcher. [Testify](https://github.com/stretchr/testify) was used in the examples below, but it is not a requirement.

- [Using mocks](#using-mocks)
  - [AzureSharedResource](#azuresharedresource)
  - [ProvisionedResource](#provisionedresource)
  - [RateLimiter](#ratelimiter)
- [Using events](#using-events)

## Using mocks

There are public interfaces provided for Batcher, Watcher, Operation, AzureSharedResource, ProvisionedResource, and RateLimiter to use for mocking. To mock using Testify, you can follow this pattern:

1. Implement the mock interface (for example, a mock Watcher):

    ```go
    import (
        "testing"
        "time"

        gobatcher "github.com/plasne/go-batcher/v2"
        "github.com/stretchr/testify/assert"
        "github.com/stretchr/testify/mock"
    )

    type mockWatcher struct {
        mock.Mock
    }

    func (w *mockWatcher) WithMaxAttempts(val uint32) gobatcher.Watcher {
        args := w.Called(val)
        return args.Get(0).(gobatcher.Watcher)
    }

    func (w *mockWatcher) WithMaxBatchSize(val uint32) gobatcher.Watcher {
        args := w.Called(val)
        return args.Get(0).(gobatcher.Watcher)
    }

    func (w *mockWatcher) WithMaxOperationTime(val time.Duration) gobatcher.Watcher {
        args := w.Called(val)
        return args.Get(0).(gobatcher.Watcher)
    }

    func (w *mockWatcher) MaxAttempts() uint32 {
        args := w.Called()
        return args.Get(0).(uint32)
    }

    func (w *mockWatcher) MaxBatchSize() uint32 {
        args := w.Called()
        return args.Get(0).(uint32)
    }

    func (w *mockWatcher) MaxOperationTime() time.Duration {
        args := w.Called()
        return args.Get(0).(time.Duration)
    }

    func (w *mockWatcher) ProcessBatch(batch []gobatcher.Operation) {
        w.Called(batch)
    }
    ```

1. Write a test method mocking any calls that are used in the underlying methods (for example, Enqueue calls MaxAttempts):

    ```go
    func TestEnqueueOnlyOnce(t *testing.T) {
        batcher := gobatcher.NewBatcher()
        watcher := &mockWatcher{}
        watcher.On("MaxAttempts").Return(uint32(1))
        op := gobatcher.NewOperation(watcher, 0, struct{}{}, false)
        op.MakeAttempt() // fake a previous attempt
        err := batcher.Enqueue(op)
        assert.Error(t, err, "should only be allowed one time")
    }
    ```

### AzureSharedResource

One of the configuration options for AzureSharedResource is `WithMocks()`. For unit testing, you can pass mocks to AzureSharedResource to emulate an Azure Storage Account (specifically a mock blob and a mock container). This allows you to unit test without needing a real Azure Storage Account.

1. Implement the mock interface for blob and container:

    ```go
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
    ```

1. Write a test method mocking any calls that are used in the underlying methods (for example, Start calls container.Create and blob.Upload):

    ```go
    func TestStart(t *testing.T) {
        container := &mockContainer{}
        container.On("Create", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
        blob := &mockBlob{}
        blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
            Return(nil, nil).Times(10)
        res := gobatcher.NewAzureSharedResource("accountName", "containerName", 10000).
            WithFactor(1000).
            WithMocks(container, blob)
        var wg sync.WaitGroup
        wg.Add(1)
        res.AddListener(func(event string, val int, msg string, metadata interface{}) {
            switch event {
            case gobatcher.ErrorEvent:
                panic(errors.New(msg))
            case gobatcher.ProvisionDoneEvent:
                wg.Done()
            }
        })
        err := res.Start(context.Background())
        assert.NoError(t, err)
        wg.Wait() // wait for provisioning (which is an async process) to finish
        container.AssertExpectations(t)
        blob.AssertExpectations(t)
    }
    ```

### ProvisionedResource

The ProvisionedResource does not need a `WithMocks()` method as it does not provision any resources in Azure. You can mock this interface as you would any other interface.

### RateLimiter

The RateLimiter interface allows you to create your own RateLimiters and use them with Batcher. However, this is outside of the scope of this unit test document.

## Using events

Both Batcher and RateLimiters raise events that you can interrogate in your unit tests to validate expected behaviors. Consider the following implementation:

```go
func WriteString(batcher gobatcher.IBatcher, data []string) {
    var wg sync.WaitGroup
    wg.Add(len(data))
    watcher := gobatcher.NewWatcher(func(batch []gobatcher.IOperation) {
        fmt.Println("START-OF-BATCH")
        for _, op := range batch {
            val := op.Payload().(string)
            fmt.Println(val)
        }
        fmt.Println("END-OF-BATCH")
        for i := 0; i < len(batch); i++ {
            wg.Done()
        }
    })
    for i, element := range data {
        cost := 100 + i // do some fancy cost calculation
        if err := batcher.Enqueue(gobatcher.NewOperation(watcher, uint32(cost), element, true)); err != nil {
            panic(err)
        }
    }
    wg.Wait()
}
```

Then consider the following unit test that validates that all operation costs were 100 or greater and that each operation is put into a separate batch due to that cost:

```go
func TestWriteString_CostIs100OrMore(t *testing.T) {
    res := gobatcher.NewProvisionedResource(1000)
    // NOTE: The FlushInterval is 100ms so there will be 10 flushes per second with 100 capacity each, so operations that are 100 or more should be in their own batches
    batcher := gobatcher.NewBatcher().
        WithRateLimiter(res).
        WithFlushInterval(100 * time.Millisecond).
        WithEmitBatch()
    var batches uint32 = 0
    batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
        switch event {
        case gobatcher.BatchEvent:
            assert.Equal(t, 1, val)
            atomic.AddUint32(&batches, 1)
            batch := metadata.([]gobatcher.IOperation)
            for _, op := range batch {
                assert.GreaterOrEqual(t, op.Cost(), uint32(100))
            }
        }
    })
    err := batcher.Start()
    assert.NoError(t, err)
    data := []string{"red", "blue", "green"}
    WriteString(batcher, data)
    assert.Equal(t, uint32(3), batches)
}
```
