<!-- markdownlint-disable no-inline-html -->

# Determining the cost of operations in a datastore that is not rate limited

You might have a datastore (SQL Server for example) that does not have a native rate limiting feature, but you can use Batcher to apply one. Just because you can send as many requests to your datastore as you want in a timeframe, doesn't mean it can process those requests efficiently or without error.

Every datastore has limitations, but they are not often expressed in simple terms like a query costs "x" and you have "y" capacity. Instead, a system might have 4 CPU cores, 64 GB of memory, a disk capable of 1 GiB/s throughput, etc. This document will help you through the process of assigning a capacity to a datastore and assigning costs to operations.

## Workflow

These steps are described in more detail below, but a workflow for determining cost might look like this...

1. Profile your application so you have data about what writes and queries are used.

1. Populate the datastore with a reasonable amount of data (could be sample data). If you expect your application to have normally have millions of records, populate it with millions of records.

1. Define your datastore in Batcher as 100,000 capacity.

1. For each representative record, determine a write cost<sup>1</sup>...

    1. Set the cost to 100 for a write.

    1. Ingest a large number of records while monitoring the CPU, memory, disk, and network capacity.

    1. If the load on the system is too great, increase the cost and repeat.

    1. If the load on the system is too little, decrease the cost and repeat.

1. For each representative query, determine a query cost<sup>1</sup>...

    1. Set the cost to 100 for a query.

    1. Run a large number of these queries while monitoring the CPU, memory, disk, and network capacity.

    1. If the load on the system is too great, increase the cost and repeat.

    1. If the load on the system is too little, decrease the cost and repeat.

1. Implement a function in your application to determine the cost based on your findings. The cost will be applied during enqueue.

1. Load test your application and verify that the CPU, memory, disk, and network capacity do not exceed desired limits.

1. Continually monitor the CPU, memory, disk, and network capacity of your datastore to ensure the resource utilization meets expectations.

<sup>1</sup> This will be an iterative process that you may have to perform many times adjusting the cost until you find something reasonable.

## Representative records and queries

You will need to identify representative records (write) and representative queries (read). These are records and queries that you think are close to what the operational system will encounter. The best way to get these representative documents and queries is to instrument the usage of your application. It is always better to make this a data-driven decision.

## Queries

The impact of query operations tends to be hard to predict for the following reasons...

- If your system supports user-defined queries, you will need to map the incoming queries to the representative queries to help determine the cost. There are various forms this might take...

  - It may be possible to match the queries exactly. If there is no direct match, you may have to find the representative query that it is closest to.

  - You may find that you can calculate a cost based on characteristics of the query. For example, you may find that each clause of the query has a certain cost, or that indexed properties cost "x" while those not indexed cost "y", etc.

- The number of results can vary and unless you have statistics on this, you won't be able to predict the impact from the return payload.

It is likely you will not have a single cost of query operations, but rather some function that evaluates the query and calculates a cost.
