import json
import csv
import boto3

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

bucket_parameter = ssm.get_parameter(Name="CDEVENT_BUCKET")
bucket_name = bucket_parameter['Parameter']['Value']

s3_folder = "processed/"

def flatten_event(event):
    '''
    Input: `event` (dtype: dict) A CDEvent-style dictionary that is ready to be flattened 
    into a single level for easier storage and querying. The original schema follows this format:
    {
        "context": {
            "version": "0.0.1",
            "id": "CDEventID123",
            "source": "/dev/userC/",
            "type:" "dev.simulated_events.pipelineRun.queued",
            timestamp": "2023-03-24 10:55:33.124459"
        },
        "subject": {
            "id": "subjectID123",
            "type": "pipelineRun",
            "content": {
                "task": "task3",
                "url": "/apis/userC.dev/veta/namespaces/default/pipelineRuns/pipelineRun3",
                "pipelineRun": {
                    "id": "pipelineRunID123",
                    "source": "/dev/userC/",
                    "type": "pipelineRun",
                    "pipelineName": "pipeline3",
                    "url": "https://api.example.com/namespace/pipeline3"
                }
            }
        }
    }
    
    Returns: `flattened_event` (dtype: dict) A dictionary representation of a CDEvent with one level:
    {
        "event_id": "eventID123",
        "context_version": "0.0.1",
        "context_id": "CDEventID123",
        "context_source": "/dev/userC/",
        "context_type": "dev.simulated_events.pipelineRun.queued",
        "context_timestamp": "2023-03-24 10:55:33.124459",
        "subject_id": "subjectID123",
        "subject_type": "pipelineRun",
        "content_task": "task3",
        "content_url": "/apis/userC.dev/veta/namespaces/default/pipelineRuns/pipelineRun3",
        "run_id": "pipelineRunID123",
        "run_source": "source": "/dev/userC/",
        "run_type": "type": "pipelineRun",
        "run_pipelineName": "pipeline3",
        "run_url": "https://api.example.com/namespace/pipeline3"
        "run_outcome": null (or one of ['success', 'error', 'failure'])
        "errors": null (or one of ['Invalid input param 123', 'Timeout during execution', 'pipelineRun cancelled by user', 'Unknown error', 'Unit tests failed']
    }
    
    Function Overview:
        This function will take a raw CDEvent object, and flatten it into one level to simulate the processing that will
        be done by Lambda oncea new object is put to S3.  This is simply to create a simulated file to experiment with locally
        and to practice creating dashboards and analyses.  It does not interact with AWS in any way.
    '''
    
    flattened_event = {}
    flattened_event['event_id'] = event['event_id']
    
    ## Flatten the `context` section
    for k, v in event['context'].items():
        new_key = "context_" + k
        flattened_event[new_key] = v
        
    ## Flatten the first layer of the `subject` section
    for k, v in event['subject'].items():
        if k == 'content':
            continue
        
        new_key = "subject_" + k
        flattened_event[new_key] = v

    ## Flatten the second layer of the `subject` section: `content`
    event_type = None        
    for k, v in event['subject']['content'].items():
        if k in ['taskRun', 'pipelineRun']:
            event_type = k
            continue
        
        new_key = "content_" + k
        flattened_event[new_key] = v
        
    ### Flatten the third layer of the `subject` section: `taskRun` or `pipelineRun`
    for k, v in event['subject']['content'][event_type].items():
        new_key = "run_" + k
        flattened_event[new_key] = v
        
    ## Add `run_outcome` and/or `errors` fields if not included in the original CDEvent.
    ## These sections will only be populated if the event is in the `failure` or `error` state
    if 'run_outcome' not in flattened_event.keys():
        flattened_event['run_outcome'] = None
        flattened_event['run_errors'] = None
    
    if 'run_errors' not in flattened_event.keys():
        flattened_event['run_errors'] = None
    
    print("Flattened Event:\n", flattened_event)
    
    return flattened_event
    
def send_event(event):
    '''
    Input: event (dtype: dict) in CDEvent-style (see `flatten_event`)
    Returns: response (dtype: json) returns the json payload of putting the flattened
        event to the CDEvents S3 bucket.  This includes the status code and the body
        of the flattened event
        
    Function Overview:
        This function will convert a dictionary object of a CDEvent to JSON, and then
        upload it to the processed folder of the CDEvent S3 bucket.
    '''
    
    json_filename = "{}.json".format(event['event_id'])
    json_event = json.dumps(event)
    
    print("Sending event to: s3://{}/{}{}".format(bucket_name, s3_folder, json_filename))
    response = s3.put_object(
        Bucket=bucket_name, 
        Key=s3_folder + json_filename, 
        Body=json_event
    )
    
    return response
    
def get_event_body(event):
    
    print("Incoming Event:,\n", event)
    
     # User these lines if you use SQS as the trigger #
    body_dict = json.loads(event['Records'][0]['body'])
    print("Event Body:\n", json.dumps(body_dict))
    
    key = body_dict['Records'][0]['s3']['object']['key']
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    event_body = json.loads(obj['Body'].read())
    
    print("Event Body:\n", event_body)
    
    return event_body

def lambda_handler(event, context):
    
    event_body = get_event_body(event)
    flattened_event_body = flatten_event(event_body)
    response = send_event(flattened_event_body)
    
    return response
