<!-- markdownlint-disable no-inline-html -->

# Determining the cost of operations in Cosmos

This document should help you determine the cost of operations when Batcher is applied to [Azure Cosmos DB](https://azure.microsoft.com/en-us/services/cosmos-db/).

Cosmos is a rate limited NoSQL database solution. This means that operations you execute against the datastore have a cost that is counted against your provisioned capacity. Specifically, when you create a Cosmos database and/or collection, you are specifying a capacity in Request Units (RU) per second. Each operation also has a cost in RU. If you exceed your allotment<sup>1</sup>, you will receive TooManyRequests errors and eventually will be cutoff with ServiceUnavailable errors.

For more on Cosmos Request Units, see <https://docs.microsoft.com/en-us/azure/cosmos-db/request-units>.

<sup>1</sup> Different protocols in Cosmos behave differently, in some cases you will immediately get TooManyRequests errors and in some cases the Cosmos gateway will buffer some requests for a short period of time. However, sustaining costs above your capacity will in every case result in TooManyRequests errors.

## Workflow

These steps are described in more detail below, but a workflow for determining cost might look like this...

1. Profile your application so you have data about what writes and queries are used.

1. Define all indexes in Cosmos.

1. Populate the collection with a reasonable amount of data (could be sample data). If you expect your application to normally have millions of records, populate it with millions of records.

1. Write your representative documents and record the RU cost.

1. Run your representative queries and record the RU cost.

1. Implement a function in your application to determine the cost based on your findings. The cost will be applied during enqueue.

1. Load test your application and verify that you don't exceed the provisioned capacity.

1. Re-test the RU costs periodically.

## Indexing

Before measuring any costs, you should configure all indexes. If you change indexes, you will need to re-run all cost calculations.

Depending on protocol, Cosmos will index all properties, you should never run a system in this mode - always specifically choose the properties to index.

## Measuring Cost

There are some key concepts to measuring cost before we get into specifics...

- The following factors will affect RU usage <https://docs.microsoft.com/en-us/azure/cosmos-db/request-units#request-unit-considerations>:
  - Payload size
  - Indexing
  - Data consistency
  - Query patterns
  - Number of partitions in scope of query
  - Script usage

- You should use representative documents and representative queries. These are documents and queries that you think are close to what the operational system will encounter.

  - The best way to get these representative documents and queries is to instrument the usage of your application. It is always better to make this a data-driven decision.

- You will want to measure costs periodically (maybe once a month).

  - Index changes, the size of indexes, and the number of partitions can affect the cost. The volume of data can also have an affect in some cases, for example, if you pack lots of documents into a single logical partition.

  - It will be helpful to create some repeatable (maybe even automated) test of the representative documents and queries.

  - This is also a good time to ensure your representative documents and queries are still representative.

### Measuring cost using the Core API

Every operation executed under the Core API returns the cost of the operation. Specifics on how to access that property on the return is here: <https://docs.microsoft.com/en-us/azure/cosmos-db/find-request-unit-charge?tabs=dotnetv2>.

### Measuring cost using the Mongo API

Unfortunately, the Mongo API does not return the cost of the operation. You can run a database command of "getLastRequestStatistics" to get the cost of the last operation in this session, details here: <https://docs.microsoft.com/en-us/azure/cosmos-db/find-request-unit-charge-mongodb>. In a system that supports asynchronous operations, the last operation is not guaranteed to be the operation you wanted to measure. Typically you will need your measurement tests to be synchronous in a different session from your operational session.

There is no RU cost for running the "getLastRequestStatistics" command.

## Writes

The cost of write operations tends to be easy to predict. You will insert records and document the cost that Cosmos reported.

If you have documents of different size and/or documents that will use different indexes, it is important to measure all of them. You may find that all your representative documents are close enough in cost that you can assign a single value across all writes. For example, if you found costs of 13.14 RU, 16.01 RU, and 12.63 RU, you might average those to a cost of 14 RU.

It is common to batch write operations, but generally in Batcher you will enqueue each individual operation, so you need to know the cost of 1 operation. Generally in Cosmos the cost of writing in batches is linear (10 documents are 10x the cost of a 1 document), make sure you are getting costs for a batch of operations that you divide by the number of operations.

## Queries

Unfortunately, the cost of query operations tends to be much harder to predict for the following reasons...

- If your system supports user-defined queries, you will need to map the incoming queries to the representative queries to help determine the cost. There are various forms this might take...

  - It may be possible to match the queries exactly. If there is no direct match, you may have to find the representative query that it is closest to.

  - You may find that you can calculate a cost based on characteristics of the query. For example, you may find that each clause of the query has a certain cost, or that indexed properties cost "x" while those not indexed cost "y", etc.

- The number of results can vary and unless you have statistics on this, you won't be able to predict the RU impact from the return payload.

It is likely you will not have a single cost of query operations, but rather some function that evaluates the query and calculates a cost. If you are using the Core API, you could then evaluate the actual cost of the operation and determine how accurate your estimation was (tuning of this estimation could even happen automatically within the code). Unfortunately, the Mongo API affords you no such realtime evaluation of actual cost.

## Capacity

Ultimately the goal of Batcher with rate limiting is to ensure that operations run in Cosmos don't exceed the provisioned capacity. You can use the metrics in the Azure portal to verify this. If you find...

- ...that you are still getting TooManyRequests, it is probable that your costs are too low.

- ...that you have too much unutilized capacity, it is probable that your costs are too high.

When using Batcher, you typically want to provision a fixed capacity for Cosmos. This allows you to select the cost you are willing to pay for your database. Batcher will ensure that you don't exceed this capacity by lengthening the time it takes for your operations to complete. Therefore, if you find that your application takes too long for operations to complete, you can increase the capacity. If you want to save money, you can decrease the capacity.

Neither of the provided rate limiters support auto-scale because they do not provide a way to increase capacity after provisioning nor do they have any way to determine the current capacity of the database. You could solve these issues with a custom rate limiter.
