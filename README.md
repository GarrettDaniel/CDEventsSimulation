# CDEventsSimulation
Quick Overview:
- This is a project to create simulated events in CDEvent style (see https://github.com/cdevents/spec/blob/main/spec.md), and send them through an automated Data Engineering pipeline for storage, processing, reporting, and potentially modeling.

Goals:
- Generate realistic simulated CDEvents
- Develop an automated pipeline to ingest, store, and process events at high volume (starting at a rate of ~250k records/day or ~3 records/second)
- Catalog data schema with Glue Crawlers and query with Athena
- Build a PowerBI Dashboard to show CD stats per user, pipeline, etc.

Architecture Overview:
![CDEvents Simulation AWS Arch drawio](https://user-images.githubusercontent.com/36463300/227257049-b562eb4e-985b-4a20-a746-e8652809ac6b.png)


CD Event Example (Original/Raw vs. Processed/Flattened:

![image](https://user-images.githubusercontent.com/36463300/227565258-720cc6e0-72e1-4dd8-a691-4cbb4b71b965.png)

Current Progress:
- Created realistic simulated CDEvents
- Wrote and documented functions to create and send events to S3
- Setup S3 bucket and event notifications for raw data upload
- Created an SQS queue and dead-letter queue for the event notifications to be polled
- Updated the SQS access policy to allow for event notifications from S3
- Created architecture diagram
- Wrote function to process events and join them together in a CSV file for analysis and reporting
- Documented all classes, functions, and code
- Created a first draft Power BI Dashboard of Users, Environments, and Run Outcomes
- Created a Lambda function to process CDEvents by polling the SQS queue
- Debugged the Lambda function
- Crawl the processed data with Glue
- Query with Athena

Need To Do:
- Visualize wtih PowerBI (in progress)
- Instead of generating event state, type, etc. completely randomly, simulate each event going through its full lifecycle so you can actually track the full timeline of an event, predict if it's likely to fail or not, etc.
- Create a Terraform config file with Terraformer to automate infrastructure deployment

Possible Improvements:
- Replace SQS and Lambda with Kinesis Firehose and Lambda to direct and transform near real-time batches of data (~1 minute intervals)
- Connect PowerBI directly to data in S3 using the Glue Data Catalog to allow for real-time dashboard updates
- Partition data by year > month > day and store in parquet format to improve compression and query performance
