import json
import random
from datetime import datetime
import uuid

## Based on CDEvents taskRun https://github.com/cdevents/spec/blob/main/core.md#taskrun

class TaskRun():
    def __init__(self, cdevent):
        '''
        Input: cdevent (dtype: CDEvent): A baseline, simulated CDEvent, generated by `CDEvent.py`
        Return: object (dtype: TaskRun): An object representing the taskRun section of a CDEvent, following this schema:
            { 
                "id": "taskRunID123",
                "source": "/staging/userA/",
                "type": "taskRun",
                "pipelineName": "pipeline1",
                "url": "https://api.example.com/namespace/pipeline1"
            }
        
        Overview:
            This class is intended to add the data schema for `taskRun` events to a baseline CDEvent, generated by `CDEvent.py`.
            It is only instantiated when a simulated CDEvent has an event_type of taskRun.  Otherwise, it will create a
            `PipelineRun` object (see `PipelineRun.py`).
        '''
        
        self.cdevent = cdevent
        self.id = str(uuid.uuid4())
        self.source = cdevent.source
        self.type = "taskRun"
        self.pipelineName = random.choices(['pipeline1', 'pipeline2', 'pipeline3', 'pipeline4'], weights=(10,20,50,30), k=1)[0]
        self.url = "https://api.example_system.com/namespace/{}".format(self.pipelineName)
        
        self.entry = {
            "id": self.id,
            "source": self.source,
            "type": self.type,
            "pipelineName": self.pipelineName,
            "url": self.url
        }
        
        self.cdevent = cdevent
        
        if cdevent.taskRun is None:
            self.id = str(uuid.uuid4())
            self.source = cdevent.source
            self.type = "taskRun"
            self.pipelineName = random.choices(['pipeline1', 'pipeline2', 'pipeline3', 'pipeline4'], weights=(10,20,50,30), k=1)[0]
            self.url = "https://api.example_system.com/namespace/{}".format(self.pipelineName)
            
            self.entry = {
                "id": self.id,
                "source": self.source,
                "type": self.type,
                "pipelineName": self.pipelineName,
                "url": self.url
            }
            
        else:
            self.id = cdevent.taskRun.id
            self.source = cdevent.source
            self.type = "taskRun"
            self.pipelineName = cdevent.taskRun.pipelineName
            self.url = cdevent.taskRun.url
            
            self.entry = {
                "id": self.id,
                "source": self.source,
                "type": self.type,
                "pipelineName": self.pipelineName,
                "url": self.url
            }
        
        if cdevent.event_state == "finished":
            
            self.outcome = random.choices(['success', 'error', 'failure'], weights=(50, 30, 20), k=1)[0]
            self.entry['outcome'] = self.outcome
            
            if self.outcome is 'error':
                
                possible_errors = ['Invalid input param 123', 'Timeout during execution', 'pipelineRun cancelled by user', 'Unknown error']
                self.errors = random.choices(possible_errors, weights=(10, 30, 30, 30), k=1)[0]
                self.entry['run_errors'] = self.errors
                
            elif self.outcome is 'failure':
                
                self.errors = "Unit tests failed"
                self.entry['run_errors'] = self.errors
        
        return
    
    def to_string(self):
        '''
        Function Overview:
            Prints a JSON-style representation of a taskRun event.  This is intended for debugging and visualization purposes.
        '''
        
        print(json.dumps(self.entry, indent=4, default=str))
        
        return
