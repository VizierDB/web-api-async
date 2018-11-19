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


class WorkflowProvenance(object):
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
    package_id: string
        Identifier of the package the module command is from
    """
    def __init__(self, action=None, package_id=None, command_id=None, created_at=None):
        """Initialize the descriptor. If action is None package_id and
        command_id are expected to be None as well.

        Parameters
        ----------
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
        if action is None and not (package_id is None and command_id is None):
            raise ValueError('invalid workflow provenance information')
        self.action = action if not action is None else ACTION_CREATE
        self.package_id = package_id
        self.command_id = command_id
        self.created_at = created_at if not created_at is None else get_current_time()


class WorkflowHandle(object):
    """Handle for a data curation workflow. Workflows are sequences of modules.
    Each workflow has a unique identifier and belongs to a branch in the
    viztrail.

    Attributes
    ----------
    branch_id: string
        Unique identifier of the branch the workflow is associated with
    descriptor: vizier.viztrail.workflow.WorkflowProvenance
        Provenance information for the workflow version
    identifier: string
        Unique workflow identifier
    modules: list(vizier.workflow.module.ModuleHandle)
        Sequence of modules that make up the workflow and result in the current
        state of the database after successful workflow execution.
    """
    def __init__(self, identifier, branch_id, modules, descriptor=None):
        """Initialize the workflow handle.

        Parameters
        ----------
        identifier: string
            Unique workflow identifier
        branch_id: string
            Unique identifier of the branch the workflow is associated with
        modules : list(vizier.workflow.module.ModuleHandle)
            Sequence of modules that make up the workflow.
        """
        self.identifier = identifier
        self.branch_id = branch_id
        self.modules = modules
        self.descriptor = descriptor if not descriptor is None else WorkflowProvenance()
