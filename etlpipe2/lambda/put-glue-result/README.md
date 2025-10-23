Purpose
-------
This lambda persists Glue job results into the `EtlGlueJobs` DynamoDB table.

Environment
-----------
- TABLE_NAME: name of the DynamoDB table to write to.

Payload
-------
The function expects either:
- { jobDetails: {...}, timestamp: '...' }
or
- the raw logged object containing logged.jobDetails and logged.timestamp.

It writes an item with keys: jobId (partition key), jobState, timestamp, fullResponse (Map).
