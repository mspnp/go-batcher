
# docs

COST

`(4 processes) x (4 lease operations per second) x (60 seconds per minute) x (60 minutes per hour) x 730 (hours per month) / (10,000 operations per billing unit) * ($0.004 per billing unit) = ~$168 month`

## additional

- cost per operation
- shared capacity (which fluxuates)
- reserved capacity (to improve latency)
- maximum number of retries per transient fault
- backoff when getting 16500 and 50
- batching
