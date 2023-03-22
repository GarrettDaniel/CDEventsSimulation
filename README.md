# CDEventsSimulation
Quick Overview:
- This is a project to create simulated events in CDEvent style, and send them through an automated Data Engineering pipeline for storage, processing, reporting, and potentially modeling.

Goals:
- Generate realistic simulated CDEvents
- Develop an automated pipeline to ingest, store, and process events at high volume (starting at a rate of ~250k records/day or ~3 records/second)
- Catalog data schema with Glue Crawlers and query with Athena
- Build a PowerBI Dashboard to show CD stats per user, pipeline, etc.

CD Event Example:

![image](https://user-images.githubusercontent.com/36463300/227051894-0f57fd81-a546-490a-88b3-82254d2b5d5b.png)

Architecture Overview: (TODO: Add Architecture Diagram)

Possible Improvements:
- Replace SQS and Lambda with Kinesis Firehose and Lambda to direct and transform near real-time batches of data (~1 minute intervals)
- Connect PowerBI directly to data in S3 using the Glue Data Catalog to allow for real-time dashboard updates
- Add additional event types and data, such as pipeline run-time
- Partition data by year > month > day and store in parquet format to improve compression and query performance
