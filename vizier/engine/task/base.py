# Copyright (C) 2017-2019 New York University,
#                         University at Buffalo,
#                         Illinois Institute of Technology.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Defines the base classes for representing executable tasks and the context
in which task are executed. The interface for task execution engines (i.e.,
processors) is defined in the processor module.
"""

from typing import Optional, Dict, Any


from vizier.engine.controller import WorkflowController
from vizier.datastore.base import Datastore
from vizier.filestore.base import Filestore
from vizier.datastore.artifact import ArtifactDescriptor
from vizier.datastore.dataset import DatasetHandle

class TaskContext(object):
    """The task context contains references to the datastore and filestore that
    are associated with the project that executes the task. The context also
    contains the current database state against which a task is executed.

    The database state is represented as a mapping of user-defined dataset names
    to unique dataset identifier.

    Attributes
    ----------
    datasets: dict
        Identifier for datasets in the database state agains which a task is
        executed (keyed by user-provided name)
    datastore: vizier.datastore.base.Datastore
        Datastore for the project that execute the task
    filestore: vizier.filestore.Filestore
        Filestore for the project that executes the task
    """
    def __init__(self, 
            project_id: str,
            datastore: Datastore, 
            filestore: Filestore, 
            artifacts: Dict[str, ArtifactDescriptor], 
            resources: Dict[str, Any] = dict()):
        """Initialize the components of the task context.

        Parameters
        ----------
        project_id: string
            project_id of the task
        datastore: vizier.datastore.base.Datastore
            Datastore for the project that execute the task
        filestore: vizier.filestore.Filestore
            Filestore for the project that executes the task
        datasets: dict, optional
            Identifier for datasets in the database state agains which a task
            is executed (keyed by user-provided name)
        resources: dict, optional
            Optional information about resources that were generated during a
            previous execution of the command
        """
        self.project_id = project_id
        self.datastore = datastore
        self.filestore = filestore
        self.datasets = { name: artifacts[name] for name in artifacts if artifacts[name].is_dataset }
        self.resources = resources
        self.dataobjects = { name: artifacts[name] for name in artifacts if not artifacts[name].is_dataset }

    def get_dataset(self, 
            name: str
        ) -> DatasetHandle:
        """Get the handle for the dataset with the given name. Raises ValueError
        if the dataset does not exist.

        Parameters
        ----------
        name: string
            Dataset name

        Returns
        -------
        vizier.datastore.dataset.DatasetHandle
        """
        if name in self.datasets:
            dataset = self.datastore.get_dataset(self.datasets[name].identifier)
            if not dataset is None:
                return dataset
        raise ValueError('unknown dataset \'' + str(name) + '\'')
    
    def get_dataobject(self, name):
        """Get the handle for the dataset with the given name. Raises ValueError
        if the dataset does not exist.

        Parameters
        ----------
        name: string
            Dataset name

        Returns
        -------
        vizier.datastore.dataset.DatasetHandle
        """
        if name in self.dataobjects:
            dataobj = self.datastore.get_object(self.dataobjects[name].identifier)
            if not dataobj is None:
                return dataobj
        raise ValueError('unknown data object \'' + str(name) + '\'')


class TaskHandle(object):
    """A task is uniquely identified by a task identifier. The identifier is
    used as a key between the controlling workflow engine and the excuting
    processor to signal state changes.

    The given controller is used to signal state changes to the controlling
    workflow engine.
    """
    def __init__(self, 
            task_id: str, 
            project_id: str, 
            controller: Optional[WorkflowController]=None):
        """Initialize the components of the task handle.

        Parameters
        ----------
        task_id: string
            Unique task identifier
        project_id: string
            Unique project identifier
        controller: vizier.engine.controller.WorkflowController, optional
            Controller for associates workflow engine
        """
        self.task_id = task_id
        self.project_id = project_id
        self.controller = controller
