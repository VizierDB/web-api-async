# Copyright (C) 2018 New York University
#                    University at Buffalo,
#                    Illinois Institute of Technology.
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

from vizier.core.timestamp import get_current_time, to_datetime

"""Identifier of the default master branch for all viztrails."""
DEFAULT_BRANCH = 'master'

"""Default name for the master branch."""
DEFAULT_BRANCH_NAME = 'Default'

"""Workflow modification action identifier."""
ACTION_CREATE = 'cre'
ACTION_DELETE = 'del'
ACTION_INSERT = 'ins'
ACTION_REPLACE = 'upd'


class VersionDescriptor(object):
    """Simple workflow descriptor that contains the workflow version and the
    time of creation.

    Attributes
    ----------
    version: int
        Workflow version identifier
    create_at: datetime.datetime, optional
        Timestamp of workflow creation (UTC)
    """
    def __init__(self, version, action=None, package_id=None, command_id=None, created_at=None):
        """Initialize the descriptor.

        Parameters
        ----------
        version: int
            Workflow version identifier
        actions: string
            Identifier of the action that created the workflow version (create,
            insert, delete, or replace)
        package_id: string
            Identifier of the package the module command is from
        command_id: string
            Identifier of the module command
        create_at: datetime.datetime
            Timestamp of workflow creation (UTC)
        """
        self.version = version
        self.action = action
        self.package_id = package_id
        self.command_id = command_id
        self.created_at = created_at if not created_at is None else get_current_time()

    @staticmethod
    def from_dict(obj):
        """Create descriptor instance from dictionary serialization.

        Returns
        -------
        vizier.workflow.base.VersionDescriptor
        """
        return VersionDescriptor(
            obj['version'],
            action=obj['action'] if 'action' in obj else None,
            package_id=obj['packageId'] if 'packageId' in obj else None,
            command_id=obj['commandId'] if 'commandId' in obj else None,
            created_at=to_datetime(obj['createdAt'])
        )

    def to_dict(self):
        """Create dictionary serialization for the object.

        Returns
        -------
        dict
        """
        return {
            'version': self.version,
            'action': self.action,
            'packageId': self.package_id,
            'commandId': self.command_id,
            'createdAt': self.created_at.isoformat()
        }


class WorkflowHandle(object):
    """Handle for a data curation workflow. Workflows are sequences of modules
    that contain (i) the command specification, and (ii) outputs for STDOUT and
    STDERR generated during module execution. Each workflow belongs to a branch
    in the viztrail. Workflows have unique version numbers.

    Attributes
    ----------
    branch_id: string
        Unique identifier of the branch the workflow is associated with
    version: int
        Unique version number for the workflow
    created_at : datetime.datetime
        Timestamp of viztrail creation (UTC)
    modules: list(vizier.workflow.module.ModuleHandle)
        Sequence of modules that make up the workflow and result in the current
        state of the database after successful workflow execution.
    """
    def __init__(self, branch_id, version, created_at, modules):
        """Initialize the workflow handle.

        Parameters
        ----------
        branch_id: string
            Unique identifier of the branch the workflow is associated with
        version : int
            Unique version number for the workflow
        created_at : datetime.datetime
            Timestamp of viztrail creation (UTC)
        modules : list(ModuleHandle)
            Sequence of modules that make up the workflow.
        """
        self.branch_id = branch_id
        self.version = version
        self.created_at = created_at
        self.modules = modules

    @property
    def has_error(self):
        """Flag indicating whether there was an error during workflow execution.
        Currently, only the existience of output to STDERR in at least one of
        the modules is used as error indicator.

        Returns
        -------
        bool
        """
        for m in self.modules:
            if m.has_error:
                return True
        return False
