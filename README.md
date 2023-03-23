# CDEventsSimulation
Quick Overview:
- This is a project to create simulated events in CDEvent style (see https://github.com/cdevents/spec/blob/main/spec.md), and send them through an automated Data Engineering pipeline for storage, processing, reporting, and potentially modeling.

Goals:
- Generate realistic simulated CDEvents
- Develop an automated pipeline to ingest, store, and process events at high volume (starting at a rate of ~250k records/day or ~3 records/second)
- Catalog data schema with Glue Crawlers and query with Athena
- Build a PowerBI Dashboard to show CD stats per user, pipeline, etc.

CD Event Example:

![image](https://user-images.githubusercontent.com/36463300/227051894-0f57fd81-a546-490a-88b3-82254d2b5d5b.png)

Architecture Overview:
![CDEvents Simulation AWS Arch drawio](https://user-images.githubusercontent.com/36463300/227257049-b562eb4e-985b-4a20-a746-e8652809ac6b.png)


Current Progress:
- Created realistic simulated CDEvents
- Wrote and documented functions to create and send events to S3
- Setup S3 bucket and event notifications for raw data upload
- Created an SQS queue and dead-letter queue for the event notifications to be polled
- Updated the SQS access policy to allow for event notifications from S3
- Created architecture diagram

Need To Do:
- Document class definitions and functions
- Create a Lambda function to process CDEvents by polling the SQS queue
- Crawl the data with Glue
- Query with Athena
- Visualize wtih PowerBI

Possible Improvements:
- Replace SQS and Lambda with Kinesis Firehose and Lambda to direct and transform near real-time batches of data (~1 minute intervals)
- Connect PowerBI directly to data in S3 using the Glue Data Catalog to allow for real-time dashboard updates
- Add additional event types and data, such as pipeline run-time
- Partition data by year > month > day and store in parquet format to improve compression and query performance
