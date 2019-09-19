import json
import abc
import uuid
from .exceptions import TaskNameError
from .brokera import Broker


class BackgroundTask(abc.ABC):
    '''Base Task needed to send task to background'''
    task_name=None

    def __init__(self):
        if not self.task_name:
            raise TaskNameError('Task name is necessary.')
        if " " in self.task_name:
            raise TaskNameError('Task name cannot have space in it.')
        self.broker=Broker()
    
    @abc.abstractmethod
    def run(self,*args,**kwargs):
        raise NotImplementedError("Task `run` method must be implemented.")
    
    def delay(self,verbose:bool=False,*args,**kwargs):
        try:
            task_id=str(uuid.uuid4())
            recieved_task = {"task_id": task_id, "args": args, "kwargs": kwargs}
            serialized_task = json.dumps(recieved_task)
            self.broker.enqueue(queue_name=self.task_name, item=serialized_task)
            if verbose:
                print("Task: {} queued succesfully".format(task_id))
            return task_id
        except Exception:
            raise Exception("Unable to send task to the broker.")   