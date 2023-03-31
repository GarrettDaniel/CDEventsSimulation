from CDEvent import CDEvent
from PipelineRun import PipelineRun
from TaskRun import TaskRun
from simulation_functions import create_event_lifecycle, create_events, send_events, create_and_send_events
import unittest
from datetime import datetime
import json
from copy import deepcopy

## Helper Functions
def test_context(context):
    print("Testing Context:\n", json.dumps(context, indent=4))
    
    expected_context_format = {
        "version": "0.0.2",
        "id": "6d8f3fc7-f2c2-4511-badc-362d318d2d70",
        "source": "/staging/userC/",
        "type": "staging.simulated_events.taskRun.finished",
        "timestamp": "2023-03-22 21:34:25.586944"
    }
    
    for k, v in context.items():
        
        if k not in expected_context_format.keys():
            raise ValueError("{} not in expected format keys: {}".format(k, expected_context_format.keys()))
        
        if type(v) != type(expected_context_format[k]):
            raise ValueError("type({})={} does not match expected type {}".format(k, type(k), type(expected_context_format[k])))
    
    return

def test_subject(subject):
    
    print("Testing Subject:\n", json.dumps(subject, indent=4))
    
    expected_subject_format = {
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
    
    for k, v in subject.items():
        
        if k not in expected_subject_format.keys():
            raise ValueError("{} not in expected format keys: {}".format(k, expected_subject_format.keys()))
        
        if type(v) != type(expected_subject_format[k]):
            raise ValueError("type({})={} does not match expected type {}".format(k, type(k),type(expected_subject_format[k])))
    
    return

def test_content(content):
    
    print("Testing content:\n", json.dumps(content, indent=4))
    
    expected_content_format = {
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
    
    for k, v in content.items():
        
        if k not in expected_content_format.keys():
            raise ValueError("{} not in expected format keys: {}".format(k, expected_content_format.keys()))
        
        if type(v) != type(expected_content_format[k]):
            raise ValueError("type({})={} does not match expected type {}".format(k, type(k),type(expected_content_format[k])))
    
    return

def test_event_state(event_state):
    
    possible_states = ['queued', 'started', 'finished']
    
    if event_state not in possible_states:
        raise ValueError
    
    return

def test_taskRun_format(taskRun_entry):
    
    print("Testing taskRun:\n", json.dumps(taskRun_entry, indent=4))
    
    expected_taskRun_entry_format = {
        "id": "a25a5e3d-5244-4df5-865d-c59a7f938308",
        "source": "/staging/userC/",
        "type": "taskRun",
        "pipelineName": "pipeline3",
        "url": "https://api.example_stystem.com/namespace/pipeline3",
        "outcome": "failure",
        "errors": "Unit tests failed"
    }
    
    for k, v in taskRun_entry.items():
        
        if k not in expected_taskRun_entry_format.keys():
            raise ValueError("{} not in expected format keys: {}".format(k, expected_taskRun_entry_format.keys()))
        
        if type(v) != type(expected_taskRun_entry_format[k]):
            raise ValueError("type({})={} does not match expected type {}".format(k, type(k),type(expected_taskRun_entry_format[k])))
            
    if taskRun_entry['type'] != "taskRun":
        raise ValueError("Incorrect event_type:", taskRun_entry['type'], "\nExpected event_type = taskRun")
    
    return

def test_pipelineRun_format(pipelineRun_entry):
    
    expected_pipelineRun_entry_format = {
        "id": "a25a5e3d-5244-4df5-865d-c59a7f938308",
        "source": "/staging/userC/",
        "type": "pipelineRun",
        "pipelineName": "pipeline3",
        "url": "https://api.example_stystem.com/namespace/pipeline3",
        "outcome": "failure",
        "errors": "Unit tests failed"
    }
    
    for k, v in pipelineRun_entry.items():
        
        if k not in expected_pipelineRun_entry_format.keys():
            raise ValueError("{} not in expected format keys: {}".format(k, expected_pipelineRun_entry_format.keys()))
        
        if type(v) != type(expected_pipelineRun_entry_format[k]):
            raise ValueError("type({})={} does not match expected type {}".format(k, type(k),type(expected_pipelineRun_entry_format[k])))
            
    if pipelineRun_entry['type'] != "pipelineRun":
        raise ValueError("Incorrect event_type:", pipelineRun_entry['type'], "\nExpected event_type = pipelineRun")
    
    return
    
def test_event_type(event_type, event):
    
    possible_types = ['pipelineRun', 'taskRun']
    
    if event_type not in possible_types:
        raise ValueError("Unexpected event_type:", event_type, "\nExpected event_type in", possible_types)
    
    if event_type == 'pipelineRun':
        if event.pipelineRun is None:
            raise ValueError("Missing pipelineRun object")
            
        test_pipelineRun_format(pipelineRun_entry=event.pipelineRun.entry)
    else:
        if event.taskRun is None:
            raise ValueError("Missing taskRun object")
        test_taskRun_format(taskRun_entry=event.taskRun.entry)
        
    return

class TestCDEvent(unittest.TestCase):
    
    def test_cdevent(self, test_event=None):

        expected_raw_format = {
            "context": {
                "version": "0.0.1",
                "id": "CDEventID123",
                "source": "/dev/userC/",
                "type": "dev.simulated_events.pipelineRun.queued",
                "timestamp": "2023-03-24 10:55:33.124459"
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
        
        if test_event is None:
            test_event = CDEvent()
        
        test_context(context=test_event.context)
        test_subject(subject=test_event.subject)
        test_event_state(event_state=test_event.event_state)
        test_event_type(event_type=test_event.event_type, event=test_event)
        
        return
        
    def test_overloaded_cdevent(self):
        
        ## Create a test event, and an overloaded event based on the test event
        test_event_original = CDEvent()
        test_event_overloaded = CDEvent(kwargs=deepcopy(test_event_original))
        
        # Make sure the timestamp for the overloaded event is after the original event
        original_timestamp = datetime.strptime(test_event_original.timestamp, '%Y-%m-%d %H:%M:%S.%f')
        overloaded_timestamp = datetime.strptime(test_event_overloaded.timestamp, '%Y-%m-%d %H:%M:%S.%f')
        self.assertTrue(overloaded_timestamp > original_timestamp)
        
        # Check that both events have different event_state
        self.assertNotEqual(test_event_original.event_state, test_event_overloaded.event_state)
        
        # Check that all other member variables are the same
        self.assertEqual(test_event_original.user, test_event_overloaded.user)
        self.assertEqual(test_event_original.environment, test_event_overloaded.environment)
        self.assertEqual(test_event_original.event_type, test_event_overloaded.event_type)
        self.assertEqual(test_event_original.event_name, test_event_overloaded.event_name)
        self.assertEqual(test_event_original.version, test_event_overloaded.version)
        self.assertEqual(test_event_original.context_id, test_event_overloaded.context_id)
        self.assertEqual(test_event_original.id, test_event_overloaded.id)
        self.assertEqual(test_event_original.task, test_event_overloaded.task)
        self.assertEqual(test_event_original.url, test_event_overloaded.url)
        
        return
        

if __name__ == '__main__':
    unittest.main()
