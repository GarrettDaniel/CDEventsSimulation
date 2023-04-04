from CDEvent import CDEvent
from PipelineRun import PipelineRun
from TaskRun import TaskRun
from simulation_functions import create_event_lifecycle, create_events, send_events, create_and_send_events, flatten_event_entry
from testing_functions import test_context, test_subject, test_event_state, test_event_type
from testing_functions import test_taskRun_format, test_pipelineRun_format
import unittest
import uuid
import boto3
from botocore.exceptions import ClientError

## Globals #################################################

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

bucket_parameter = ssm.get_parameter(Name="CDEVENT_BUCKET")
bucket_name = bucket_parameter['Parameter']['Value']
s3_folder = "raw/"

expected_flat_format = {
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
    "run_source": "/dev/userC/",
    "run_type": "pipelineRun",
    "run_pipelineName": "pipeline3",
    "run_url": "https://api.example.com/namespace/pipeline3",
    "run_outcome": 'error',
    "errors": 'Timeout during execution'
}

class TestSimulations(unittest.TestCase):
    
    def test_create_event_lifecycle(self):
        
        events_list = []
        ids_list = []
        states_list = []
        timestamps_list = []
        possible_types = ['pipelineRun', 'taskRun']
        
        events_list, ids_list = create_event_lifecycle(events_list, ids_list)
        
        ## Make sure they follow correct CDEvent format
        for event_entry in events_list:
            test_context(context=event_entry['context'])
            test_subject(subject=event_entry['subject'])
            
            event_state = event_entry['context']['type'].split(".")[-1] # Last element in the type element delimited by "."
            test_event_state(event_state=event_state)
            states_list.append(event_state)
            
            timestamp = event_entry['context']['timestamp']
            timestamps_list.append(timestamp)
            
            event_type = event_entry['context']['type'].split(".")[-2] # 2nd to last element in the type element delimited by "."
            self.assertIn(event_type, possible_types)
            
        ## Make sure there are no duplicates in any of these lists
        self.assertEqual(len(ids_list), len(set(ids_list)))
        self.assertEqual(len(states_list), len(set(states_list)))
        self.assertEqual(len(timestamps_list), len(set(timestamps_list)))
        
        return
    
    def test_flatten_event_entry(self):
        
        original_event = CDEvent()
        original_event.entry['event_id'] = str(uuid.uuid4())
        flattened_event = flatten_event_entry(original_event.entry)
        
        for k, v in flattened_event.items():
        
            if k not in flattened_event.keys():
                raise ValueError("{} not in expected format keys: {}".format(k, flattened_event.keys()))
                
            if k in ['run_outcome', 'errors']:
                if v is None: 
                    continue
                self.assertEqual(type(v), str)
            
            elif type(v) != type(flattened_event[k]):
                raise ValueError("type({})={} does not match expected type {}".format(k, type(k),type(flattened_event[k])))
        
        return
    
    def test_send_events(self):
        
        events_list = []
        ids_list = []
        
        events_list, ids_list = create_event_lifecycle(events_list, ids_list)
        responses_map = send_events(events_list, ids_list, bucket_name=bucket_name)
        
        for id_ in ids_list:
            ## Would it be good to include a retry here if it isn't a 200 code?  Maybe in `send_events`?
            self.assertEqual(responses_map[id_]['ResponseMetadata']['HTTPStatusCode'], 200)
            
            s3_location = s3_folder + "{}.json".format(id_)
            
            try:
                s3.head_object(Bucket=bucket_name, Key=s3_location)
            except ClientError as e:
                return int(e.response['Error']['Code'] != 404)
        
        return


if __name__ == '__main__':
    unittest.main()