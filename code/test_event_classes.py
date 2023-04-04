from CDEvent import CDEvent
from PipelineRun import PipelineRun
from TaskRun import TaskRun
from simulation_functions import create_event_lifecycle, create_events, send_events, create_and_send_events
import unittest
from datetime import datetime
import json
from copy import deepcopy
from testing_functions import test_context, test_subject, test_event_state, test_event_type
from testing_functions import test_taskRun_format, test_pipelineRun_format

class TestEventClasses(unittest.TestCase):
    '''
    Class Overview:
        This is a class dedicated to unit testing CDEvent, TaskRun, and PipelineRun event types.
        It will utilize helper functions from `testing_functions.py` to granularly test each element
        of these various events to ensure that they are created properly before being sent to S3
        for further processing.  Additional testing for sending raw data to S3 and processing it 
        are found in `test_simulations.py`.
    '''
    
    def test_cdevent(self, test_event=None):
        '''
        Input: test_event (type: CDEvent) This object will be created if the parameter
            is ommitted from the function call, or will use the passed in event if given
            at the function call
            
        Return: None
        
        Function Overview:
            This function will use various functions from `testing_functions.py` to test
            each aspect of a CDEvent's structure.  This includes context, subject, content,
            and then pipelineRun or taskRun depending on what kind of event_type it is.
        '''
        
        print("Testing CDEvent creation and formatting:")
        
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
        '''
        Input: None
        
        Return: None
        
        Function Overview:
            This test is intended to validate that "overloading" the CDEvent constructor
            with an existing event will still work properly. In particular, it tests to ensure
            that if you overload the constructor, that the following event will be a continuation
            in the pipeline process from the previous one (i.e. going from a "queued" state to a "started"
            state, or from "started" to "finished").  It will also ensure that the timestamp for the overloaded
            entry is not the same as the original, to simulate the time it takes for an actual CDEvent
            to go from one state to the next.  It will then ensure that the rest of the eleemnts in each 
            CDEvent are identical.
        '''
        
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
    
    def test_taskRun(self):
        '''
        Input: None
        Return: None
        
        Function Overview:
            This function tests the format of a TaskRun entry using `test_taskRun_format`
            from `testing_functions.py`.
        '''
        
        event = CDEvent()
        self.test_cdevent(test_event = event)
        
        taskRun = TaskRun(event)
        test_taskRun_format(taskRun_entry = taskRun.entry)
        
        return
    
    def test_pipelineRun(self):
        '''
        Input: None
        Return: None
        
        Function Overview:
            This function tests the format of a PipelineRun entry using `test_pipelineRun_format`
            from `testing_functions.py`.
        '''
        
        event = CDEvent()
        self.test_cdevent(test_event = event)
        
        pipelineRun = PipelineRun(event)
        test_pipelineRun_format(pipelineRun_entry = pipelineRun.entry)
        
        return
        

if __name__ == '__main__':
    unittest.main()