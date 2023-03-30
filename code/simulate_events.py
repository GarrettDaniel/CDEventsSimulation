from CDEvent import CDEvent
import boto3
import uuid
import time
import json
from simulation_functions import flatten_event_entry, create_and_send_events
import pandas as pd

# Step 0: Create a test event to make sure that CDEvent, PipelineRun, and TaskRun
# work properly and look visually correct
print("Original Event:")
test_event = CDEvent()
test_event.to_string()

# Step 1: Create and send raw CDEvents to S3 and save locally to JSON
all_events = []

for i in range(100):
    
    events_list, ids_list, responses_map = create_and_send_events(num_events=5)
    
    all_events.extend(events_list)
    time.sleep(0.5)


# Serializing json
json_object = json.dumps(all_events)
 
# Writing to sample.json
with open("simulated_raw_events.json", "w") as outfile:
    outfile.write(json_object)
    
# Step 2: Flatten Events and save locally (Lambda will flatten them from S3)    
# Step 2a: Show how flatten_event will flatten a single event
print("\n\n Flattened event:")
    
print(json.dumps(flatten_event_entry(all_events[0]), indent=4, default=str))

## Step 2b: Flatten all raw CDEvent entries
flattened_events_list = [flatten_event_entry(event_entry) for event_entry in all_events]

## Step 2c: Join flattened events into a DataFrame
processed_df = pd.DataFrame(flattened_events_list)
print("Flattened data in a DataFrame:\n")
print("All columns:\n", processed_df.columns, '\n')
print(processed_df.head())

## Step 2d: Save to CSV for later analysis and reporting
processed_df.to_csv("simulated_processed_events.csv", index=False)
