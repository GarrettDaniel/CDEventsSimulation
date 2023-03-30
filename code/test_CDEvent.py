from CDEvent import CDEvent
from PipelineRun import PipelineRun
from TaskRun import TaskRun
from simulation_functions import create_event_lifecycle, create_events, send_events, create_and_send_events
import unittest
from copy import deepcopy

class TestCDEvent(unittest.TestCase):
    
    def test_context(self, context):
        
        expected_context_format = {
            "context": {
                "version": "0.0.2",
                "id": "6d8f3fc7-f2c2-4511-badc-362d318d2d70",
                "source": "/staging/userC/",
                "type": "staging.simulated_events.taskRun.finished",
                "timestamp": "2023-03-22 21:34:25.586944"
            }
        }
        
        for k, v in context:
            
            self.assertIn(k, expected_context_format.keys())
            self.assertEquals(k.dtype, expected_context_format[k].dtype)
        
        return
    
    def test_subject(self, subject):
        
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
        
        for k, v in subject:
            
            self.assertIn(k, expected_subject_format.keys())
            self.assertEquals(k.dtype, expected_subject_format[k].dtype)
        
        return
    
    def test_content(self, content):
        
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
        
        for k, v in content:
            
            self.assertIn(k, expected_content_format.keys())
            self.assertEquals(k.dtype, expected_content_format[k].dtype)
        
        return
    
    def test_event_state(self, event_state):
        
        possible_states = ['queued', 'started', 'finished']
        
        self.assertIn(event_state, possible_states)
        
        return
    
    def test_taskRun_format(self, taskRun_entry):
        
        expected_taskRun_entry_format = {
            "id": "a25a5e3d-5244-4df5-865d-c59a7f938308",
            "source": "/staging/userC/",
            "type": "taskRun",
            "pipelineName": "pipeline3",
            "url": "https://api.example_stystem.com/namespace/pipeline3",
            "outcome": "failure",
            "errors": "Unit tests failed"
        }
        
        for k, v in taskRun_entry:
            
            self.assertIn(k, expected_taskRun_entry_format.keys())
            self.assertEquals(k.dtype, expected_taskRun_entry_format[k].dtype)
            
        self.assertEquals(taskRun_entry['type'], "taskRun")
        
        return
    
    def test_pipelineRun_format(self, pipelineRun_entry):
        
        expected_pipelineRun_entry_format = {
            "id": "a25a5e3d-5244-4df5-865d-c59a7f938308",
            "source": "/staging/userC/",
            "type": "pipelineRun",
            "pipelineName": "pipeline3",
            "url": "https://api.example_stystem.com/namespace/pipeline3",
            "outcome": "failure",
            "errors": "Unit tests failed"
        }
        
        for k, v in pipelineRun_entry:
            
            self.assertIn(k, expected_pipelineRun_entry_format.keys())
            self.assertEquals(k.dtype, expected_pipelineRun_entry_format[k].dtype)
            
        self.assertEquals(pipelineRun_entry['type'], "pipelineRun")
        
        return
        
    def test_event_type(self, event_type, event):
        
        possible_types = ['pipelineRun', 'taskRun']
        
        self.assertIn(event_type, possible_types)
        
        if event_type == 'pipelineRun':
            self.assertIsNotNone(event.pipelineRun)
            self.test_pipelineRun_format(event.pipelineRun.entry)
        else:
            self.assertIsNotNone(event.taskRun)
            self.test_taskRun_format(event.taskRun.entry)
            
        return
    
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
        
        self.test_context(test_event.context)
        self.test_subject(test_event.subject)
        self.test_event_state(test_event.event_state)
        self.test_event_type(test_event.event_type, test_event)
        
        return
        
    def test_overloaded_cdevent(self):
        
        test_event_original = CDEvent()
        test_event_overloaded = CDEvent(kwargs=deepcopy(test_event_original))
        
        self.test_cdevent(test_event_original)
        self.test_cdevent(test_event_overloaded)
        
        ## TODO: 
        # assert timestamp of overloaded is > original
        # assert that event_state is not the same
        # assert that everything else is the same (user, version, id, etc.)
        
        return
        

if __name__ == '__main__':
    unittest.main()