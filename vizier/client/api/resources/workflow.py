# Copyright (C) 2018 New York University,
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

"""Resource object representing a workflow from a project branch that is
available at a remote vizier instance."""

from vizier.client.api.resources.module import ModuleResource
from vizier.core.timestamp import to_datetime

import vizier.engine.packages.pycell.base as pycell
import vizier.viztrail.workflow as wf


PACKAGES = {
    pycell.PACKAGE_PYTHON: pycell.PYTHON_COMMANDS
}


class WorkflowResource(object):
    """A workflow in a remote vizier instance."""
    def __init__(self, identifier, action, command, created_at, modules=None):
        """Initialize the branch attributes."""
        self.identifier = identifier
        self.action = action
        self.command = command
        self.created_at = created_at
        self.modules = modules

    @staticmethod
    def from_dict(obj):
        """Get a workflow resource instance from the dictionary representation
        returned by the vizier web service.

        Parameters
        ----------
        obj: dict()
            Dictionary serialization of a workflow descriptor or handle

        Returns
        -------
        vizier.client.api.resources.workflow.WorkflowResource
        """
        # Get the action name
        action = to_external_form(obj['action'])
        # Get the command name
        command = None
        package = PACKAGES[obj['packageId']]['command']
        for cmd in package:
            if cmd['id'] == obj['commandId']:
                command = cmd['name']
        modules = None
        if 'modules' in obj:
            modules = [ModuleResource.from_dict(m) for m in obj['modules']]
        return WorkflowResource(
            identifier=obj['id'],
            action=action,
            command=command,
            created_at=to_datetime(obj['createdAt']),
            modules=modules
        )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def to_external_form(action):
    """Convert a given action identifier to its external form.

    Parameters
    ----------
    action: string
         Unique action identifier

    Returns
    -------
    string
    """
    if action is None:
        return 'null'
    elif action == wf.ACTION_APPEND:
        return 'append'
    elif action == wf.ACTION_CREATE:
        return 'create'
    elif action == wf.ACTION_DELETE:
        return 'delete'
    elif action == wf.ACTION_INSERT:
        return 'insert'
    elif action == wf.ACTION_REPLACE:
        return 'replace'
    else:
        return 'unknown'
