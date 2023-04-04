from CDEvent import CDEvent
from PipelineRun import PipelineRun
from TaskRun import TaskRun
from simulation_functions import create_event_lifecycle, create_events, send_events, create_and_send_events
import unittest
from datetime import datetime
import json
from copy import deepcopy

## Helper Functions for Unit Testing CDEvent, PipelineRun, and TaskRun classes

def test_context(context):
    '''
    Input: context (type: dict; from: CDEvent.context) This is a context dictionary from the CDEvent.context
        member variable.
        
    Returns: None
    
    Function Overview:
        This function will test the format of the CDEvent.context dictionary to ensure
        that it meets the standard format from the CDEvent specs.
    '''
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
    '''
    Input: subject (type: dict; from: CDEvent.subject) This is a subject dictionary from the CDEvent.subject
        member variable.
        
    Returns: None
    
    Function Overview:
        This function will test the format of the CDEvent.subject dictionary to ensure
        that it meets the standard format from the CDEvent specs.
    '''
    
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
    '''
    Input: content (type: dict; from: CDEvent.content) This is a content dictionary from the CDEvent.content
        member variable.
        
    Returns: None
    
    Function Overview:
        This function will test the format of the CDEvent.content dictionary to ensure
        that it meets the standard format from the CDEvent specs.
    '''
    
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
    '''
    Input: event_state (type: str; from: CDEvent.event_state) This is a string from the CDEvent.event_state
        member variable, indicating what state the CDEvent is in.
        
    Returns: None
    
    Function Overview:
        This function will confirm that the current event_state of the CDEvent you are testing
        is a valid state, according to the CDEvent specs.
    '''
    
    possible_states = ['queued', 'started', 'finished']
    
    if event_state not in possible_states:
        raise ValueError
    
    return

def test_event_type(event_type, event):
    '''
    Input: 
        event_type (type: str; from: CDEvent.event_type) This is a string from the CDEvent.event_state
        member variable, indicating what state the CDEvent is in.
        
        event (type: CDEvent)
        
    Returns: None
    
    Function Overview:
        This function will confirm that the current event_state of the CDEvent you are testing
        is a valid state, according to the CDEvent specs.
    '''
    
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

def test_taskRun_format(taskRun_entry):
    '''
    Input: taskRun_entry (type: dict; from: CDEvent.taskRun.entry) This is a TaskRun.entry 
        dictionary from the CDEvent.taskRun.entry member variable.
        
    Returns: None
    
    Function Overview:
        This function will test the format of the CDEvent.taskRun.entry dictionary to ensure
        that it meets the standard format from the CDEvent specs.
    '''
    
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
    '''
    Input: pipelineRun_entry (type: dict; from: CDEvent.pipelineRun.entry) This is a PipelineRun.entry 
        dictionary from the CDEvent.pipelineRun.entry member variable.
        
    Returns: None
    
    Function Overview:
        This function will test the format of the CDEvent.pipelineRun.entry dictionary to ensure
        that it meets the standard format from the CDEvent specs.
    '''
    
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