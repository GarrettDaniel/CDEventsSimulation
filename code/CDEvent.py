import json
import random
from datetime import datetime
import uuid
from PipelineRun import PipelineRun
from TaskRun import TaskRun

## Based on CDEvents Subjects https://github.com/cdevents/spec/blob/main/spec.md#source-subject

class CDEvent():
    def __init__(self, **kwargs):
        '''
        Input: None
        Returns: object (dtype: CDEvent) An object in the format of a standard CDEvent (see https://github.com/cdevents/spec/blob/main/spec.md):
            {
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
            NOTE: the "pipelineRun": {...} section will be filled in with a `PipelineRun` object (see `PipelineRun.py`) or a 
                `TaskRun` object if it were of type "taskRun" (see `TaskRun.py`).
                
        Overview:
            The CDEvent class is intended to create realistic, simulated CDEvent data for practice with storing, processing, analyzing, 
            and reporting actual CDEvent data from various CD data producers.  None of the environments, pipeline names, etc. are real,
            they are all fictitious, but follow the same format of standard CDEvents naming conventions and styling.
        '''
        
        ## Option 1: If we give the CDEvent constructor a previous CDEvent to go off of
        if len(kwargs) > 0:
            self.original_event = kwargs['kwargs']
            self.taskRun = self.original_event.taskRun
            self.pipelineRun = self.original_event.pipelineRun
            
            self.user = self.original_event.user
            self.environment = self.original_event.environment
            self.event_type = self.original_event.event_type
            self.event_name = self.original_event.event_name
            self.event_type = self.original_event.event_type
            
            if self.original_event.event_state == 'queued':
                self.event_state = 'started'
            elif self.original_event.event_state == 'started':
                self.event_state = 'finished'
            else: ## This would be unknown input.  Deal with this when creating events
                self.event_state = 'unknown'
            
            ## Populate subject from original event
            self.subject = self.original_event.subject
            self.id = self.original_event.id
            self.task = self.original_event.task
            self.url = self.original_event.url
            
            ## Populate context from original event
            self.context = self.original_event.context
            self.context['type'] = self.original_event.context['type'].replace(self.original_event.event_state, self.event_state)
            self.version = self.original_event.version
            self.context_id = self.original_event.context_id
            self.source = self.original_event.source
            self.type = self.original_event.type
            
            ## change context timestamp to simulate run-time of the task
            original_timestamp = datetime.strptime(self.original_event.timestamp, '%Y-%m-%d %H:%M:%S.%f')
            run_time_seconds = abs(np.random.normal(loc=120, scale=20))
            new_timestamp = original_timestamp + timedelta(seconds = run_time_seconds)

            self.timestamp = str(new_timestamp)
            self.context['timestamp'] = self.timestamp

        ## Option 2: We are creating a new task/event
        else:
            self.original_event = None
            self.taskRun = None
            self.pipelineRun = None
            
            self.user = random.choices(['userA', 'userB', 'userC'], weights=(30,20,50), k=1)[0]
            self.environment = random.choices(['dev', 'staging', 'prod'], weights=(40,40,20), k=1)[0]
            self.event_type = random.choices(['pipelineRun','taskRun'], weights=(30,70), k=1)[0]
            self.event_name = random.choices(['{}1'.format(self.event_type), '{}2'.format(self.event_type), '{}3'.format(self.event_type)], weights=(20, 60, 20), k=1)[0]
            
            if self.event_type == 'taskRun':
                self.event_state = 'started'
            else:
                self.event_state = random.choices(['queued', 'started'], weights=(30,60), k=1)[0]
            
            self.subject = self.create_subject()
            self.context = self.create_context()
        
        ## Either way this logic is the same, regardless of if we provide a previous CDEvent to the constructor or not.
        self.entry = {}
        self.entry['context'] = self.context
        self.entry['subject'] = self.subject
        
        if self.event_type == 'pipelineRun':
            self.create_pipeline_run()
            
        elif self.event_type == 'taskRun':
            self.create_task_run()
        
        return
    
    def create_context(self):
        '''
        Input: None
        Return: `context` (dtype: dict) A dictionary that models the structure of the context section of a CDEvent
            {
                "version": "0.0.1",
                "id": "CDEventID123",
                "source": "/dev/userC/",
                "type:" "dev.simulated_events.pipelineRun.queued",
                timestamp": "2023-03-24 10:55:33.124459"
            }
        
        Function Overview:
            This function will generate simulated data to fill the `context` section of a CDEvent.
        '''
        
        self.version = random.choices(['0.0.1', '0.0.2', '0.1.0'], weights=(20, 40, 40), k=1)[0]
        self.context_id = str(uuid.uuid4())
        self.source = "/{}/{}/".format(self.environment, self.user)
        self.timestamp = str(datetime.now())
        self.type = "{}.simulated_events.{}.{}".format(self.environment, self.event_type, self.event_state)
        
        context = {}
        context['version'] = self.version
        context['id'] = self.context_id
        context['source'] = self.source
        context['type'] = self.type
        context['timestamp'] = self.timestamp
        
        return context
    
    def create_subject(self):
        '''
        Input: None
        Return: `subject` (dtype: dict) A dictionary that models the structure of the subject section of a CDEvent
            {
                "id": "subjectID123",
                "type": "pipelineRun",
                "content": {
                    "task": "task3",
                    "url": "/apis/userC.dev/veta/namespaces/default/pipelineRuns/pipelineRun3",
                }
            }
        
        Function Overview:
            This function will generate simulated data to fill the `subject` section of a CDEvent.
            NOTE: This will not include the `"pipelineRun": {...}` or `"taskRun": {...}` section, 
            as that will be generated by either `create_pipeline_run` or `create_task_run` below.
        '''
        
        self.id = str(uuid.uuid4())
        
        self.task = random.choices(['task1', 'task2', 'task3'], weights=(10, 30, 50), k=1)[0]
        self.url = "/apis/{}.{}/veta/namespaces/default/{}s/{}".format(self.user, self.environment, self.event_type, self.event_name)
        
        subject = {}
        subject['id'] = self.id
        subject['type'] = self.event_type
        
        content = {}
        content['task'] = self.task 
        content['url'] = self.url
        subject['content'] = content
        
        return subject
        
    def create_pipeline_run(self):
        '''
        Input: None
        Returns: None
        
        Function Overview:
            This function will populate the `"pipelineRun": {...}` section of a CDEvent with the standard pipelineRun format
            from CDEvents (see `PipelineRun.py`)
        '''

        self.pipelineRun = PipelineRun(self)
        self.subject['content']['pipelineRun'] = self.pipelineRun.entry 
        
        return
    
    def create_task_run(self):
        '''
        Input: None
        Returns: None
        
        Function Overview:
            This function will populate the `"taskRun": {...}` section of a CDEvent with the standard taskRun format
            from CDEvents (see `TaskRun.py`)
        '''

        self.taskRun = TaskRun(self)
        self.subject['content']['taskRun'] = self.taskRun.entry 
        
        return
        
    
    def to_string(self):
        '''
        Function Overview:
            Prints a JSON-style representation of a CDEvent entry.  This is intended for debugging and visualization purposes.
        '''
        
        print(json.dumps(self.entry, indent=4, default=str))
        
        return
