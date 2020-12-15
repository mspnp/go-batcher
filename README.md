
# Batcher

Datastores have performance limits and work executed against those datastores have costs in terms of memory, CPU, disk, network, and so on (whether you have quantified those costs or not). The goal of Batcher is to provide an easy way for developers to consume all available resources on the datastore without exceeding the limits.

Consider this example...

You have an Azure Cosmos database that you have provisioned with 20k RU. Your service is in a Pod with 4 replicas on Kubernetes that need to share the capacity. Your service gets large jobs for processing, commonly 100k records or more at a time. Each record costs 10 RU. If we imagine that 2 jobs come in at the same time (1 job to each of 2 replicas), then we have 100k x 2 x 10 RU or 2m RU of work that needs to be done. Given that we have 20k capacity per second, we know that we could complete the work in 100 seconds if we could spread it out. Without something like Batcher, each process might try and send their own 100k messages in parallel and with no knowledge of the other. This would cause Cosmos to start issuing 429 TooManyRequests error messages and given the volume might even cut you off with 503 Service Unavailable error messages. Batcher solves this problem by allowing you to share the capacity across multiple replicas and controlling the flow of traffic so you don't exceed the 20k RU per second.

## Terminology

There are several components related to Batcher...

- Batcher: You will create one Batcher for each datastore that has capacity you wish to respect. Lots of Watcher will share the same Batcher. Batchers are long-lived.

- Operation: You will enqueue Operations into Batcher. An operation has an associated "Watcher", a "cost", a designation of whether or not it can be batched, a counter for the number of times it has been "attempted", and a "payload" (which can be anything you want).

- Watcher: You will create one Watcher per process that you wish to manage. The Watcher receives the batches as they become available. Watchers are short-lived. For instance, if your solution is an HTTP server, you will probably create a Watcher with each request, send your Operations to a shared Batcher, and then get batches for processing back on your Watcher. If you need to handle different types of Operations that are processed in different ways or if they have different characteristics (such as an optimal batchsize), you might create a separate Watcher for each of those use-cases.

There are also 2 rate limiters provided out-of-the-box...

- ProvisionedResource: This is a simple rate limiter that has a fixed capacity per second.

- AzureSharedResource: This rate limiter allows you to reserve a fixed amount of capacity and then share a fixed amount of capacity across multiple processes.

Some other terms will be used throughout...

- Target: As operations are enqueued or marked done in Batcher it updates a "target" which is the total cost Batcher thinks is necessary to process any outstanding operations. In other words, as operations are enqueued, the target grows by the cost of that operation. When a batch is marked done, the target is reduced by the cost of all operations in that batch.

TODO show a diagram

## Features

- Datastore Agnostic: Batcher does not process the operations it batches, it just notifies the caller when a batch is ready for processing. This design means the solution can work with any datastore.

- Batching: You may specify that operations can be batched (ex. writes) and then specify constraints, like how often operations should be flushed, maximum batch size, datastore capacity, etc. Batcher will send you batches of operations ready for you to process within all your constraints.

- Rate Limiting: You may optionally attach a rate limiter to Batcher that can restrict the operations so they don't exceed a certain cost per second.

- Shared Capacity: Batcher supports using a rate limiter. One of the included rate limiters is AzureSharedResource which allows for sharing capacity across multiple processes/containers/replicas. Sharing capacity in this way can reduce cost.

- Reserved Capacity: AzureSharedResource also supports a reserved capacity to improve latency. For instance, you might have 4 containers that need to share 20K RU in a Cosmos database. You might give each 2K reserved capacity and share the remaining 14K RU. This gives each process low latency up to 2K RU but allows each process to request more.

- Cost per Operation: Each operation that you enqueue to Batcher will have an associated cost.

- Limit Retries: Commonly datastores have transient faults. You want to retry those operations after a short time because they might succeed, but you don't want to retry them forever. Watchers can be set to enforce a maximum number of retries.

- Pause: When your datastore is getting too much pressure (throwing timeouts or too-many-requests), you can pause the Batcher for a short period of time to give it some time to catch-up.

## Cost

shared vs reserved

`(4 processes) x (4 lease operations per second) x (60 seconds per minute) x (60 minutes per hour) x 730 (hours per month) / (10,000 operations per billing unit) * ($0.004 per billing unit) = ~$168 month`

## Rate Limiting

could have a cost of 1 to limit by a certain number of operations per second

## Batcher Configuration

Creating a new Batcher with all defaults looks like this...

```go
batcher := NewBatcher()
```

- __Buffer__ [DEFAULT: 10,0000]: The buffer determines how many operations can be enqueued at a time. When ErrorOnFullBuffer is "false" (the default), the Enqueue() method blocks until a slot is available. When ErrorOnFullBuffer is "true" an error of type "BufferFullError" is returned from Enqueue().

- __RateLimiter__ [OPTIONAL]: If provided, it will be used to ensure that the cost of operations does not exceed the capacity available per second.

- __FlushInterval__ [DEFAULT: 100ms]: This determines how often operations in the buffer are examined. Each time the interval fires, operations will be dequeued and added to batches or released individually (if not batchable) until such time as the aggregate cost of everything considered in the interval exceeds the capacity allotted this timeslice. For the 100ms default, there will be 10 intervals per second, so the capacity allocated is 1/10th the available capacity. Generally you want FlushInterval to be under 1 second though it could technically go higher.

- __CapacityInterval__ [DEFAULT: 100ms]: This determines how often the Batcher asks the RateLimiter for capacity. Generally you should leave this alone, but you could increase it to slow down the number of storage operations required for sharing capacity. Please be aware that this only applies to Batcher asking for capacity, it doesn't mean the rate limiter will allocate capacity any faster, just that it is being asked more often.

- __AuditInterval__ [DEFAULT: 10s]: This determines how often the target (see [terminology](#Terminology)) is audited to ensure it is accurate. The target is manipulated with atomic operations and abandoned batches are cleaned up after MaxOperationTime so target should always be accurate. Therefore, we should expect to only see "audit-pass" and "audit-skip" events. This audit interval is a failsafe that if the buffer is empty and the MaxOperationTime is exceeded and the target is greater than zero, it is reset and an "audit-fail" event is raised. Since Batcher is a long-lived process, this audit helps ensure a broken process does not monopolize shared capacity when it isn't needed.

- __MaxOperationTime__ [DEFAULT: 1m]: This determines how long the system should wait for the done() function to be called on the batch before it assumes it is done and decreases the target (see [terminology](#Terminology)) anyway. It is critical that the target reflect the current cost of outstanding operations. The MaxOperationTime ensures that a batch isn't orphaned and continues reserving capacity long after it is no longer needed. Please note there is also a MaxOperationTime on the Watcher which takes precident over this time.

- __PauseTime__ [DEFAULT: 500ms]: This determines how long the FlushInterval, CapacityInterval, and AuditIntervals are paused when Batcher.Pause() is called. Typically you would pause because the datastore cannot keep up with the volume of requests (if it happens maybe adjust your rate limiter).

- __ErrorOnFullBuffer__ [OPTIONAL]: Normally the Enqueue() method will block if the buffer is full, however, you can set this configuration flag if you want it to return an error instead.

Creating a new Batcher with all available configuration items might look like this...

```go
batcher := NewBatcherWithBuffer(10000).
    WithRateLimiter(myRateLimiter).
    WithFlushInterval(100 * time.Millisecond).
    WithCapacityInterval(100 * time.Millisecond).
    WithAuditInterval(10 * time.Second).
    WithMaxOperationTime(1 * time.Minute).
    WithPauseTime(500 * time.Millisecond).
    WithErrorOnFullBuffer()
```

## Usage

code samples

## Events

## Guidance for FlushInterval

## Areas for improvement

- This tool was originally designed to limit transactions against Azure Cosmos which has a cost model expressed as a single composite value (Request Unit). For datastores that might have more granular capacities, it would be nice to be able to provision Batcher with all those capacities and have an enqueue method that supported those costs. For example, memory, CPU, disk, network, etc. might all have separate capacities and individual queries might have individual costs.

- There is currently no way to change capacity in the rate limiters once they are provisioned, but there is no good reason this is fixed.

- There is currently no good way to model a datastore that autoscales but might require some time to increase capacity. Ideally something that allowed for capacity to increase by "no more than x amount over y time" would helpful. This could be a rate limiter or a feature that is added to existing rate limiters.

- The pause logic is a simple fixed amount of time to delay new batches, but it might be nice to have an exponential back-off.
