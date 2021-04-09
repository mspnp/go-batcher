# Events

Events are raised with a "name" (string), "val" (int), and "msg" (*string).

## Events raised by Batcher

The following events can be raised by Batcher...

- __shutdown__: This is raised after Stop() is called on a Batcher instance.

- __pause__: This is raised after Pause() is called on a Batcher instance. The val is the number of milliseconds that it was paused for.

- __audit-fail__: This is raised if an error was found during the AuditInterval. The msg contains more details. Should an audit fail, there is no additional action required, the Target will automatically be remediated.

- __audit-pass__: This is raised if the AuditInterval found no issues.

- __audit-skip__: If the Buffer is not empty or if MaxOperationTime (on Batcher) has not been exceeded by the last batch raised, the audit will be skipped. It is normal behavior to see lots of skipped audits.

## Events raised by ProvisionedResource and AzureSharedResource

The following events can be raised by ProvisionedResource and AzureSharedResource...

- __shutdown__: This is raised after Stop() is called on a rate limiter instance.

- __capacity__: This is raised anytime the Capacity changes. The val is the available capacity.

- __batch__: This is raised only when WithEmitBatch has been added to Batcher and whenever a batch is raised to any Watcher. The val is the count of the operations in the batch.

## Events raised by AzureSharedResource

In addition, the following events can be raised by AzureSharedResource...

- __failed__: This is raised if the rate limiter fails to procure capacity. This does not indicate an error condition, it is expected that attempts to procure additional capacity will have failures. The val is the index of the partition that was not obtained.

- __released__: This is raised whenever the rate limiter releases capacity. The val is the index of the partition for which the lease was released.

- __allocated__: This is raised whenever the rate limiter gains capacity. The val is the index of the partition for which an exclusive lease was obtained.

- __error__: This is raised if there was some unexpected error condition, such as an authentication failure when attempting to allocate a partition.

- __created-container__: The Azure Storage Account must exist, but the container can be created in Provision(). This event is raised if that happens. The msg is the fully qualified path to the container.

- __verified-container__: During Provision(), if the container already exists, this event is raised. The msg is the fully qualified path to the container.

- __created-blob__: During Provision(), if a zero-byte blob needs to be created for a partition, this event is raised. The val is the index of the partition created.

- __verified-blob__: During Provision(), if a zero-byte blob partition was found to exist, this event is raised. The val is the index of the partition verified.
