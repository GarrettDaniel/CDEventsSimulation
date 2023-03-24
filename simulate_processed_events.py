from CDEvent import CDEvent
import boto3
import uuid
import time
import json
import pandas as pd

# Step 0: Create a test event to make sure that CDEvent, PipelineRun, and TaskRun
# work properly and look visually correct
print("Original Event:")
test_event = CDEvent()
test_event.to_string()

def flatten_event(event):
    '''
    Input: `event` (dtype: `CDEvent`) A CDEvent Object that is ready to be flattened into a single level for easier
    storage and querying.  The original schema follows this format:
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
    flattened_event['event_id'] = str(uuid.uuid4()) #This will be taken care of in simulate_events.py
    
    ## Flatten the `context` section
    for k, v in event.entry['context'].items():
        new_key = "context_" + k
        flattened_event[new_key] = v
        
    ## Flatten the first layer of the `subject` section
    for k, v in event.entry['subject'].items():
        if k == 'content':
            continue
        
        new_key = "subject_" + k
        flattened_event[new_key] = v

    ## Flatten the second layer of the `subject` section: `content`
    event_type = None        
    for k, v in event.entry['subject']['content'].items():
        if k in ['taskRun', 'pipelineRun']:
            event_type = k
            continue
        
        new_key = "content_" + k
        flattened_event[new_key] = v
        
    ### Flatten the third layer of the `subject` section: `taskRun` or `pipelineRun`
    for k, v in event.entry['subject']['content'][event_type].items():
        new_key = "run_" + k
        flattened_event[new_key] = v
        
    ## Add `run_outcome` and/or `errors` fields if not included in the original CDEvent.
    ## These sections will only be populated if the event is in the `failure` or `error` state
    if 'run_outcome' not in flattened_event.keys():
        flattened_event['run_outcome'] = None
        flattened_event['errors'] = None
    
    if 'run_errors' not in flattened_event.keys():
        flattened_event['errors'] = None
    
    return flattened_event
    
print("\n\n Flattened event:")
    
print(json.dumps(flatten_event(test_event), indent=4, default=str))

## Data Processing Simulation:
n_events = 100000
flattened_events_list = []

## Step 1: Create and flatten `n_events` number of CDEvents
for i in range(n_events):
    new_event = CDEvent()
    flattened_event = flatten_event(new_event)
    flattened_events_list.append(flattened_event)
    
# print(flattened_events_list)

## Step 2: Join flattened events into a DataFrame
processed_df = pd.DataFrame(flattened_events_list)

## Step 3: Save to CSV for later analysis and reporting
processed_df.to_csv("simulated_processed_events.csv", index=False)
