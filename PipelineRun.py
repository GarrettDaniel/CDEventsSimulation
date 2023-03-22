import json
import random
from datetime import datetime
import uuid

## Based on CDEvents pipelineRun https://github.com/cdevents/spec/blob/main/core.md#pipelinerun

class PipelineRun():
    def __init__(self, cdevent):
        self.cdevent = cdevent
        self.id = str(uuid.uuid4())
        self.source = cdevent.source
        self.type = "pipelineRun"
        self.pipelineName = random.choices(['pipeline1', 'pipeline2', 'pipeline3', 'pipeline4'], weights=(10,20,50,30), k=1)[0]
        self.url = "https://api.example_stystem.com/namespace/{}".format(self.pipelineName)
        
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
                
            elif self.outcome is 'failure':
                
                self.errors = "Unit tests failed"
                self.entry['errors'] = self.errors
        
        return
    
    def to_string(self):
        
        print(json.dumps(self.entry, indent=4, default=str))
        
        return