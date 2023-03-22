from CDEvent import CDEvent
import boto3
import uuid
import time
import json

# Step 0: Create a test event to make sure that CDEvent, PipelineRun, and TaskRun
# work properly and look visually correct

test_event = CDEvent()
test_event.to_string()

## Brainstorming

## I want to estimate ~250k events per day:
## Per hour, that would be 250,000 / 24 = 10,416.67
## Per minute, that would be 10,416.67 / 60 = 173.61
## Per second, that would be 173.61 / 60 = 2.89
## I will start off testing 3 records every second, 
## and then potentially scale up from there.

## Because of these numbers, I don't think it will really be necessary
## to use Kinesis given the fact that there were only be around ~175
## records per minute.  For Kinesis to truly be relevant,
## we would need to be pumping out a lot more requests per second.
## Additionally, Kinesis is not supported for free tier, so I'll be using
## SQS instead.

## Step 1: Create clients for Parameter store and S3

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

## Step 2: Grab S3 bucket info from ssm

bucket_parameter = ssm.get_parameter(Name="CDEVENT_BUCKET")
bucket_name = bucket_parameter['Parameter']['Value']
s3_folder = "raw/"

## Step 3: Create records and send them to S3

## Step 3a: Create functions for making records and sending them to S3

def create_events(num_events):
    '''
    Input:
        `num_events` (dtype: int): The number of simulated CDEvents you want to generate
    
    Function Overview:
        Based on CDEvents Subjects: https://github.com/cdevents/spec/blob/main/spec.md
        This function will create simulated CDEvents following this style:
        {
            "context": {
                "version": "0.0.2",
                "id": "6d8f3fc7-f2c2-4511-badc-362d318d2d70",
                "source": "/staging/userC/",
                "type": "staging.simulated_events.taskRun.finished",
                "timestamp": "2023-03-22 21:34:25.586944"
            },
            "subject": {
                "id": "bbfaf73e-c7af-4cd9-883c-5267c764e25c",
                "type": "taskRun",
                "content": {
                    "task": "task3",
                    "url": "/apis/userC.staging/veta/namespaces/default/taskRuns/taskRun2",
                    "taskRun": {
                        "id": "a25a5e3d-5244-4df5-865d-c59a7f938308",
                        "source": "/staging/userC/",
                        "type": "taskRun",
                        "pipelineName": "pipeline3",
                        "url": "https://api.example_stystem.com/namespace/pipeline3",
                        "outcome": "failure",
                        "errors": "Unit tests failed"
                    }
                }
            }
        }
        
        It will then create `num_events` events, store them in a list `events_list`
        and store the ids in a list `ids_list`.
    
    Returns: 
        `events_list` (dtype: list): A list of all CDEvents with a unique event_id for each event
            as the key, and the full event as the value (i.e. {event_id: CDEvent})
        `ids_list` (dtype: list): A list of all the event_ids created for each CDEvent
    
    '''
    
    events_list = []
    ids_list = []
    
    for i in range(num_events):
        event_entry = {}
        event_id = str(uuid.uuid4())
        
        new_event = CDEvent()
        event_entry[event_id] = new_event.entry
        
        events_list.append(event_entry)
        ids_list.append(event_id)
    
    return events_list, ids_list

def send_events(events_list, ids_list, bucket_name, responses_map=None):
    '''
    Inputs:
        `events_list` (dtype: list): This is a list of event dictionaries from `create_events`
        `ids_list` (dtype: list): This a list of event_id strings from `create_events`
        `bucket_name` (dtype: str): This is a string value for the name of the S3 bucket you
            intend to send your JSON files to
        `responses_map` (dtype: dict): This is a dictionary that will store all of the responses
            from each event sent to S3, using their event_id as the name of the JSON file.
        
    Function Overview:
        This function will take the events and ids created and stored from `create_events`
        and then send them to Amazon S3 as individual JSON files
        
    Returns:
        responses_map (dtype: dict): See input parameter `responses_map`
    '''
    
    if responses_map is None:
        responses_map = {}
    
    for i, event in enumerate(events_list):
        
        event_id = ids_list[i]
        json_filename = "{}.json".format(event_id)
        json_event = json.dumps(event)
        
        response = s3.put_object(
            Bucket=bucket_name, 
            Key=s3_folder + json_filename, 
            Body=json_event
        )
        
        responses_map[event_id] = response
    
    return responses_map
    
def create_and_send_events(num_events, bucket_name):
    '''
    Inputs:
        `events_list` (dtype: list): This is a list of event dictionaries from `create_events`
        `bucket_name` (dtype: str): This is a string value for the name of the S3 bucket you
            intend to send your JSON files to
    
    Function Overview:
        This function will call `create_events` and `send_events` to create `num_events` number
        of simulated CDEvents and send them to `bucket_name` S3 bucket.
    
    Returns:
        `events_list` (dtype: list): This is a list of event dictionaries from `create_events`
        `ids_list` (dtype: list): This a list of event_id strings from `create_events`
        `responses_map` (dtype: dict): This is a dictionary that will store all of the responses
            from each event sent to S3, using their event_id as the name of the JSON file.
    '''
    
    responses_map = {}
    
    events_list, ids_list = create_events(num_events)
    responses_map = send_events(events_list, ids_list, bucket_name, responses_map)
    
    return events_list, ids_list, responses_map
    
## Step 3b: Run the function to create and send events to S3

for i in range(300):
    
    events_list, ids_list, responses_map = create_and_send_events(num_events=3, bucket_name=bucket_name)
    
    time.sleep(1)
