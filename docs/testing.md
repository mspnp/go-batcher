# Unit Testing

This section documents ways to facilitate unit testing when using Batcher. [Testify](https://github.com/stretchr/testify) was used in the examples below, but it is not a requirement.

- [Using events](#using-events)
- [Using mocks](#using-mocks)
  - [AzureSharedResource](#azuresharedresource)

## Using mocks

There are interfaces provided for Batcher, Watcher, Operation, AzureSharedResource, ProvisionedResource, RateLimiter

Interfaces IBatcher, IWatcher and IOperation have beed added and can be used to facilitate unit testing by creating mocks, with the consideration that mock implementation will be needed for all the features that are used.

### AzureSharedResource

One of the configuration options for AzureSharedResource is `withMocks()`. For unit testing, you can pass mocks to AzureSharedResource to emulate an Azure Storage Account and specifically a mock blob and a mock container.

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
