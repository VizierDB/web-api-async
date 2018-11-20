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

"""Abstract class for viztrail branches. Each branch has a unique identifier
and an optional name. Each branch maintains a sequence of workflow versions
that represent the history of the branch.
"""

from abc import abstractmethod

from vizier.core.timestamp import get_current_time
from vizier.viztrail.base import NamedObject


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
    def __init__(self, identifier, properties, provenance, workflows=None):
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
        self.workflows = workflows if not workflows is None else list()
        # Maintain an index of workflow positions for fast access
        self.index = dict()
        for i in range(len(self.workflows)):
            self.index[self.workflows[i].identifier] = i

    @abstractmethod
    def append_workflow(self, workflow):
        """Append the given workflow handle to the branch. The workflow becomes
        the new head of the branch.

        Parameters
        ----------
        workflow: vizier.viztrail.workflow.WorkflowHandle
            New branch head
        """
        raise NotImplementedError

    def get_head(self):
        """Shortcut the get the workflow at the head of the branch. The result
        is None if the branch is empty.

        Returns
        -------
        vizier.viztrail.workflow.base.WorkflowHandle
        """
        return self.get_workflow(workflow_id=None)

    def get_history(self):
        """Get the list of workflows for the branch that define the branch
        history. The result includes the current state of the branch as the
        last element in the list.

        Returns
        -------
        list(vizier.viztrail.workflow.base.WorkflowHandle)
        """
        return list(self.workflows)

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
        if len(self.workflows) == 0:
            return None
        if workflow_id is None:
            return self.worlflows[-1]
        elif workflow_id in self.index:
            return self.workflows[self.index[workflow_id]]
        else:
            return None
