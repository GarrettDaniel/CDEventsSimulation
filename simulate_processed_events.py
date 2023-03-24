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
    
    flattened_event = {}
    flattened_event['event_id'] = str(uuid.uuid4()) #This will be taken care of in simulate_events.py
    for k, v in event.entry['context'].items():
        new_key = "context_" + k
        flattened_event[new_key] = v
        
    for k, v in event.entry['subject'].items():
        if k == 'content':
            continue
        
        new_key = "subject_" + k
        flattened_event[new_key] = v

    event_type = None        
    for k, v in event.entry['subject']['content'].items():
        if k in ['taskRun', 'pipelineRun']:
            event_type = k
            continue
        
        new_key = "content_" + k
        flattened_event[new_key] = v
        
    for k, v in event.entry['subject']['content'][event_type].items():
        new_key = "run_" + k
        flattened_event[new_key] = v
        
    if 'run_outcome' not in flattened_event.keys():
        flattened_event['run_outcome'] = None
        flattened_event['errors'] = None
    
    if 'run_errors' not in flattened_event.keys():
        flattened_event['errors'] = None
    
    return flattened_event
    
print("\n\n Flattened event:")
    
print(json.dumps(flatten_event(test_event), indent=4, default=str))

n_events = 100000

flattened_events_list = []

for i in range(n_events):
    new_event = CDEvent()
    flattened_event = flatten_event(new_event)
    flattened_events_list.append(flattened_event)
    
# print(flattened_events_list)

processed_df = pd.DataFrame(flattened_events_list)

processed_df.to_csv("simulated_processed_events.csv", index=False)