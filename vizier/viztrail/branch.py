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

"""Abstract class for viztrail branches. Each branch has a unique identifier
and an optional name. Each branch maintains a sequence of workflow versions
that represent the history of the branch.
"""

from abc import abstractmethod

from vizier.core.timestamp import get_current_time
from vizier.viztrail.base import NamedObject


"""Initial name for the default branch."""
DEFAULT_BRANCH = 'Default'


class BranchProvenance(object):
    """Simple object that contains provenance information for each branch. The
    provenance for a branch includes the source branch identifier, the
    identifier for the workflow, and the identifier for the module in the source
    workflow from which the branch was created. The module identifier defines
    the last module in the source workflow that was copied too the new branch.

    If a branch is not created from a previous branch but from scratch, e.g.,
    the first branch in a viztrail, all arguments are expected to be None. If
    the source-branch is not None the workflow and module identifier are also
    expected not to be None.

    Attributes
    ----------
    created_at: datetime.datetime
        Timestamp of branch creation (UTC)
    module_id: string
        Identifier of module at which the new branch started
    source_branch : string
        Unique identifier of source branch
    workflow_id: string
        Identifier of source workflow

    """
    def __init__(self, source_branch=None, workflow_id=None, module_id=None, created_at=None):
        """Initialize the provenance object.

        Raises ValueError if at least one but not all arguments are None.

        Parameters
        ----------
        source_branch : string
            Unique identifier of source branch
        workflow_id: string
            Identifier of source workflow
        module_id: string
            Identifier of module at which the new branch started
        created_at: datetime.datetime, optional
            Timestamp of branch creation (UTC)
        """
        # Raise an exception if one argument is None but not all of them
        if not source_branch is None and not workflow_id is None and not module_id is None:
            pass
        elif source_branch is None and workflow_id is None and  module_id is None:
            pass
        else:
            raise ValueError('invalid arguments for branch provenance')
        self.source_branch = source_branch
        self.workflow_id = workflow_id
        self.module_id = module_id
        self.created_at = created_at if not created_at is None else get_current_time()


class BranchHandle(NamedObject):
    """Branch in a viztrail. Each branch has a unique identifier, a set of user-
    defined properties, and a provenance object that defines the branch source.

    Each branch is a list of workflow versions. The sequence of workflows define
    the history of the branch, i.e., all the previous states and the current
    state of the branch. The last workflow version in the branch represents the
    head of the branch and therefore the current state of the branch.

    Attributes
    ----------
    identifier: string
        Unique branch identifier
    properties: vizier.core.properties.ObjectPropertiesHandler
        User-defined properties for the branch
    provenance: vizier.viztrail.base.BranchProvenance
        Provenance information for this branch
    """
    def __init__(self, identifier, properties, provenance):
        """Initialize the viztrail branch.

        Parameters
        ----------
        identifier: string
            Unique branch identifier
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Handler for user-defined properties
        provenance: vizier.viztrail.base.BranchProvenance
            Provenance information for this branch
        """
        super(BranchHandle, self).__init__(properties=properties)
        self.identifier = identifier
        self.provenance = provenance

    @abstractmethod
    def append_workflow(self, modules, action, command, pending_modules=None):
        """Append a workflow as the new head of the branch. The new workflow may
        contain modules that have not been persisted prevoiusly (pending
        modules). These modules are persisted as part of the workflow being
        created.

        Parameters
        ----------
        modules: list(vizier.viztrail.module.ModuleHandle
            List of modules in the workflow that are completed
        action: string
            Identifier of the action that created the workflow
        command: vizier.viztrail.module.ModuleCommand
            Specification of the executed command that created the workflow
        pending_modules: list(vizier.viztrail.module.ModuleHandle, optional
            List of modules in the workflow that need to be materialized

        Returns
        -------
        vizier.viztrail.workflow.base.WorkflowHandle
        """
        raise NotImplementedError

    @property
    def created_at(self):
        """Shortcut to get the created_at timestamp from the associated
        provenance object.

        Returns
        -------
        datatime.datatime
        """
        return self.provenance.created_at

    def get_head(self):
        """Shortcut the get the workflow at the head of the branch. The result
        is None if the branch is empty.

        Returns
        -------
        vizier.viztrail.workflow.base.WorkflowHandle
        """
        return self.get_workflow(workflow_id=None)

    @abstractmethod
    def get_history(self):
        """Get the list of descriptors for the workflows in the branch history.

        Returns
        -------
        list(vizier.viztrail.workflow.base.WorkflowDescriptor)
        """
        raise NotImplementedError

    @abstractmethod
    def get_workflow(self, workflow_id=None):
        """Get the workflow with the given identifier. If the identifier is
        none the head of the branch is returned. The result is None if the
        branch is empty.

        Parameters
        ----------
        workflow_id: string, optional
            Unique workflow identifier

        Returns
        -------
        vizier.viztrail.workflow.base.WorkflowHandle
        """
        raise NotImplementedError

    @property
    def last_modified_at(self):
        """The timestamp of last modification is either the time when the
        brnach was created or when the branch head was modified.

        Returns
        -------
        datatime.datatime
        """
        ts = self.provenance.created_at
        head = self.get_head()
        if not head is None:
            if ts < head.descriptor.created_at:
                ts = head.descriptor.created_at
        return ts
