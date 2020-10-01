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

"""Vizier DB Workflow API - Viztrails.

Base classes for workflows and viztrails. Workflow are sequences of modules.
Viztrails are collections of workflows (branches). A viztrail maintains not
only the different workflows but also the history for each of them.
"""

from typing import cast, Optional, List, Dict
from datetime import datetime


from vizier.core.util import init_value
from vizier.core.timestamp import get_current_time
from vizier.viztrail.module.base import ModuleState, MODULE_SUCCESS, ModuleHandle
from vizier.datastore.dataset import ArtifactDescriptor, DatasetDescriptor


"""Workflow modification action identifier."""
ACTION_APPEND = 'apd'
ACTION_CREATE = 'cre'
ACTION_DELETE = 'del'
ACTION_INSERT = 'ins'
ACTION_REPLACE = 'upd'


class WorkflowDescriptor(object):
    """Simple workflow provenance object that contains the command that created
    a workflow version and the timestamp of workflow creation.

    Attributes
    ----------
    action: string
        Identifier of the action that created the workflow version (create,
        insert, delete, or replace)
    command_id: string
        Identifier of the module command
    create_at: datetime.datetime, optional
        Timestamp of workflow creation (UTC)
    identifier: string
        Unique workflow identifier
    package_id: string
        Identifier of the package the module command is from
    """
    def __init__(self, 
            identifier: str, 
            action: str, 
            package_id: Optional[str] = None, 
            command_id: Optional[str] = None, 
            created_at: Optional[datetime] = None
        ):
        """Initialize the descriptor. If action is not the branch create action
        the package_id and command_id are expected to not be None.

        Parameters
        ----------
        identifier: string
            Unique workflow identifier
        action: string
            Identifier of the action that created the workflow version (create,
            insert, delete, or replace)
        package_id: string
            Identifier of the package the module command is from
        command_id: string
            Identifier of the module command
        create_at: datetime.datetime
            Timestamp of workflow creation (UTC)
        """
        if action != ACTION_CREATE and (package_id is None or command_id is None):
            raise ValueError('invalid workflow provenance information')
        self.identifier = identifier
        self.action = action
        self.package_id = package_id
        self.command_id = command_id
        self.created_at = init_value(created_at, get_current_time())


class WorkflowHandle(object):
    """Handle for a data curation workflow. Workflows are sequences of modules.
    Each workflow has a unique identifier and belongs to a branch in the
    viztrail.

    Attributes
    ----------
    branch_id: string
        Unique identifier of the branch the workflow is associated with
    descriptor: vizier.viztrail.workflow.WorkflowDescriptor
        Provenance information for the workflow version
    identifier: string
        Unique workflow identifier
    modules: list(vizier.workflow.module.ModuleHandle)
        Sequence of modules that make up the workflow and result in the current
        state of the database after successful workflow execution.
    """
    def __init__(self, 
            identifier: str, 
            branch_id: str, 
            modules: List[ModuleHandle], 
            descriptor: WorkflowDescriptor
        ):
        """Initialize the workflow handle.

        Parameters
        ----------
        identifier: string
            Unique workflow identifier
        branch_id: string
            Unique identifier of the branch the workflow is associated with
        modules : list(vizier.workflow.module.ModuleHandle)
            Sequence of modules that make up the workflow.
        descriptor: vizier.viztrail.workflow.WorkflowDescriptor
            Provenance information for the workflow version
        """
        self.identifier = identifier
        self.branch_id = branch_id
        self.modules = modules
        self.descriptor = descriptor

    @property
    def created_at(self):
        """Shortcut to get the timestamp of creation from the associated
        descriptor.

        Returns
        -------
        datetime.datetime
        """
        return self.descriptor.created_at

    def get_state(self) -> ModuleState:
        """The workflow state is either SUCCESS or the state of the first module
        that is not in SUCCESS state.

        Returns
        -------
        vizier.viztrail.module.ModuleState
        """
        for m in self.modules:
            if not m.is_success:
                return ModuleState(m.state)
        return ModuleState(MODULE_SUCCESS)

    @property
    def is_active(self):
        """True if the workflow is in an active state. This is indicated by the
        state of the last module in the workflow.

        Returns
        -------
        bool
        """
        if len(self.modules) > 0:
            return self.modules[-1].is_active
        else:
            return False

    @property
    def tail_artifacts(self) -> Dict[str, ArtifactDescriptor]:
        """Retrieve a list of dataset mappings at the tail of this
        workflow.  

        Returns a map from dataset name to dataset identifier.
        """
        datasets:Dict[str, ArtifactDescriptor] = dict()
        for m in self.modules:
            if m.provenance.write is not None:
                for artifact_name in m.provenance.write:
                    artifact = m.provenance.write[artifact_name]
                    datasets[artifact_name] = artifact
        return datasets

    @property
    def tail_datasets(self) -> Dict[str, DatasetDescriptor]:
        """Retrieve a list of dataset mappings at the tail of this
        workflow.  

        Returns a map from dataset name to dataset identifier.
        """
        datasets:Dict[str, DatasetDescriptor] = dict()
        for m in self.modules:
            if m.provenance.write is not None:
                for artifact_name in m.provenance.write:
                    artifact = m.provenance.write[artifact_name]
                    if artifact.is_dataset:
                        datasets[artifact_name] = cast(DatasetDescriptor, artifact)
        return datasets
