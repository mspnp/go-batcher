# Unit Testing

This section documents ways to facilitate unit testing when using Batcher. [Testify](https://github.com/stretchr/testify) was used in the examples below, but it is not a requirement.

- [Using events](#using-events)
- [Using mocks](#using-mocks)
  - [AzureSharedResource](#azuresharedresource)

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

One of the configuration options for AzureSharedResource is `withMocks()`. For unit testing, you can pass mocks to AzureSharedResource to emulate an Azure Storage Account and specifically a mock blob and a mock container.

### RateLimiter

This is provided so you can write your own.

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
    res := gobatcher.NewProvisionedResource(999)
    // NOTE: The FlushInterval is 100ms so there will be 10 flushes per second with 99 capacity each, so operations that are 100 or more should be in their own batches
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
