# Unit Testing

This section documents ways to facilitate unit testing when using Batcher. [Testify](https://github.com/stretchr/testify) was used in the examples below, but it is not a requirement.

- [Using mocks](#using-mocks)
  - [SharedResource](#sharedresource)
  - [RateLimiter](#ratelimiter)
- [Using events](#using-events)

## Using mocks

There are public interfaces provided for Batcher, Watcher, Operation, SharedResource, LeaseManager, Eventer and RateLimiter to use for mocking. To mock using Testify, you can follow this pattern:

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

### SharedResource
<!-- TODO: Review -->
One of the configuration options for SharedResource is `WithSharedCapacity` using a `LeaseManager`. For unit testing, you can mock the LeaseManager. This allows you to unit test without needing a real Azure Storage Account.

1. Implement the mock interface for LeaseManager:

    ```go
    type mockLeaseManager struct {
        mock.Mock
    }

    func (mgr *mockLeaseManager) Parent(sr gobatcher.Eventer) {
        mgr.Called(sr)
    }

    func (mgr *mockLeaseManager) Provision(ctx context.Context) (err error) {
        args := mgr.Called(ctx)
        return args.Error(0)
    }

    func (mgr *mockLeaseManager) CreatePartitions(ctx context.Context, count int) {
        mgr.Called(ctx, count)
    }

    func (mgr *mockLeaseManager) LeasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration) {
        args := mgr.Called(ctx, id, index)
        return args.Get(0).(time.Duration)
    }
    ```

1. Write a test method mocking any calls that are used in the underlying methods (for example, Start calls mgr.CreatePartitions):

    ```go
    func TestStart(t *testing.T) {
        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()

        mgr := &mockLeaseManager{}
        mgr.On("Parent", mock.Anything).Once()
        mgr.On("Provision", mock.Anything).Return(nil).Once()
        mgr.On("CreatePartitions", mock.Anything, 10).Once()
        res := gobatcher.NewSharedResource().
            WithSharedCapacity(10000, mgr).
            WithFactor(1000)
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
        err := res.Start(ctx)
        assert.NoError(t, err)
        wg.Wait() // wait for provisioning (which is an async process) to finish
        mgr.AssertExpectations(t)
    }
    ```

### RateLimiter

The RateLimiter interface allows you to create your own RateLimiters and use them with Batcher. However, this is outside of the scope of this unit test document.

## Using events

Both Batcher and RateLimiter raise events that you can interrogate in your unit tests to validate expected behaviors. Consider the following implementation:

```go
func WriteString(batcher gobatcher.Batcher, data []string) {
    var wg sync.WaitGroup
    wg.Add(len(data))
    watcher := gobatcher.NewWatcher(func(batch []gobatcher.Operation) {
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
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    res := gobatcher.NewSharedResource().
        WithReservedCapacity(1000)
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
            batch := metadata.([]gobatcher.Operation)
            for _, op := range batch {
                assert.GreaterOrEqual(t, op.Cost(), uint32(100))
            }
        }
    })
    err := batcher.Start(ctx)
    assert.NoError(t, err)
    data := []string{"red", "blue", "green"}
    WriteString(batcher, data)
    assert.Equal(t, uint32(3), batches)
}
```
