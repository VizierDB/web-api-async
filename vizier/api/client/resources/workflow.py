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

"""Resource object representing a workflow from a project branch that is
available at a remote vizier instance."""

from vizier.api.client.resources.dataset import DatasetDescriptor
from vizier.api.client.resources.module import ModuleResource
from vizier.core.timestamp import to_datetime

import vizier.api.serialize.deserialize as deserialize
import vizier.engine.packages.plot.base as plot
import vizier.engine.packages.pycell.base as pycell
import vizier.engine.packages.vizual.base as vizual
import vizier.viztrail.workflow as wf


PACKAGES = {
    plot.PACKAGE_PLOT: plot.PLOT_COMMANDS,
    pycell.PACKAGE_PYTHON: pycell.PYTHON_COMMANDS,
    vizual.PACKAGE_VIZUAL: vizual.VIZUAL_COMMANDS
}


class WorkflowResource(object):
    """A workflow in a remote vizier instance."""
    def __init__(
        self, identifier, action, command, created_at, modules=None,
        datasets=None, links=None
    ):
        """Initialize the branch attributes."""
        self.identifier = identifier
        self.action = action
        self.command = command
        self.created_at = created_at
        self.modules = modules
        self.datasets = datasets
        self.links = links

    @property
    def is_empty(self):
        """True if the notebook is empty (indicated by an action, command, or
        created at timestamp that is None).

        Returns
        -------
        bool
        """
        return self.action is None or self.command is None or self.created_at is None

    @staticmethod
    def from_dict(obj):
        """Get a workflow resource instance from the dictionary representation
        returned by the vizier web service.

        Parameters
        ----------
        obj: dict
            Dictionary serialization of a workflow descriptor or handle

        Returns
        -------
        vizier.api.client.resources.workflow.WorkflowResource
        """
        # Get the action name
        action = None
        command = None
        created_at = None
        if 'action' in obj:
            action = to_external_form(obj['action'])
            created_at = to_datetime(obj['createdAt'])
            # Get the command name
            package_id = obj['packageId']
            if not package_id is None:
                package = PACKAGES[package_id]['command']
                for cmd in package:
                    if cmd['id'] == obj['commandId']:
                        command = cmd['name']
            else:
                command = 'Create Branch'
        modules = None
        if 'modules' in obj:
            modules = [ModuleResource.from_dict(m) for m in obj['modules']]
        datasets = None
        if 'datasets' in obj:
            datasets = {
                ds['id']: DatasetDescriptor.from_dict(ds)
                    for ds in obj['datasets']
                }
        links = None
        if 'links' in obj:
            links = deserialize.HATEOAS(links=obj['links'])
        return WorkflowResource(
            identifier=obj['id'],
            action=action,
            command=command,
            created_at=created_at,
            modules=modules,
            datasets=datasets,
            links=links
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
