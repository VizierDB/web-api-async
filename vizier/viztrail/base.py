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

"""Abstract class for viztrails. Viztrails are Vizier's adoption of VisTrails.
Defines and implements the base classes for viztrail objects.

A viztrail is a set of branches. Each branch has a unique identifier and an
optional name. Each branch is a sequence of workflow versions.
"""

from abc import abstractmethod
from typing import Optional, List, Dict, Any
from datetime import datetime

from vizier.core.timestamp import get_current_time
from vizier.core.annotation.base import ObjectAnnotationSet
from vizier.viztrail.branch import BranchHandle, BranchProvenance
from vizier.viztrail.named_object import NamedObject
from vizier.viztrail.module.base import ModuleHandle, MODULE_PENDING
from vizier.viztrail.command import ModuleCommand
from vizier.viztrail.module.output import ModuleOutputs
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp


# ------------------------------------------------------------------------------
# Viztrails
# ------------------------------------------------------------------------------

class ViztrailHandle(NamedObject):
    """Handle for a viztrail. The viztrail handle provides access to all the
    vviztrail information and branches.

    Each viztrail has a unique viztrail identifier, timestamp information for
    viztrail creation and the last modification, and the viztrail properties.

    The viztrail properties contain the optional name. If the respective key is
    not set the viztrail name is None.

    Attributes
    ----------
    identifier : string
        Unique viztrail identifier
    created_at : datetime.datetime
        Timestamp of viztrail creation (UTC)
    last_modified_at : datetime.datetime
        Timestamp when viztrail was last modified (UTC). This does not include
        changes to the viztrail properties but only to branches and workflows.
    name: string
        Human readable viztrail name
    properties: dict(string, ANY)
        Set of user-defined properties that are associated with this viztrail
    """
    def __init__(self, 
            identifier: str, 
            properties: ObjectAnnotationSet, 
            branches: Optional[List[BranchHandle]]=None, 
            default_branch: Optional[BranchHandle] = None, 
            created_at: datetime = get_current_time()
    ):
        """Initialize the viztrail descriptor.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        properties: dict(string, any), optional
            Handler for user-defined properties
        branches: list(vizier.viztrail.branch.BranchHandle), optional
            List of branches in the viztrail
        default_branch: vizier.viztrail.branch.BranchHandle, optional
            Default branch for the viztrail
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        """
        super(ViztrailHandle, self).__init__(
            properties=properties
        )
        self.identifier = identifier
        self.branches:Dict[str,BranchHandle] = dict()
        # Initialize the branch index from the given list (if present)
        if not branches is None:
            for b in branches:
                self.branches[b.identifier] = b
        self.default_branch = default_branch
        # If created_at timestamp is None the viztrail is expected to be a newly
        # created viztrail.
        self.created_at = created_at if not created_at is None else get_current_time()

    @abstractmethod
    def create_branch(self, 
            provenance: Optional[BranchProvenance] = None, 
            properties: Optional[Dict[str, Any]] = None, 
            modules: Optional[List[str]] = None,
            identifier: Optional[str] = None
        ) -> BranchHandle:
        """Create a new branch. If the list of workflow modules is given this
        defins the branch head. Otherwise, the branch is empty.

        Parameters
        ----------
        provenance: vizier.viztrail.base.BranchProvenance
            Provenance information for the new branch
        properties: dict, optional
            Set of properties for the new branch
        modules: list(string), optional
            List of module identifier for the modules in the workflow at the
            head of the branch

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_branch(self, branch_id: str) -> bool:
        """Delete branch with the given identifier. Returns True if the branch
        existed and False otherwise.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    def get_default_branch(self) -> Optional[BranchHandle]:
        """Get the handle for the default branch of the viztrail.

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        return self.default_branch

    def get_branch(self, branch_id: str) -> Optional[BranchHandle]:
        """Get handle for the branch with the given identifier. Returns None if
        no branch with the given identifier exists.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        if branch_id in self.branches:
            return self.branches[branch_id]
        else:
            return None

    def has_branch(self, branch_id):
        """Returns True if a branch with the given identifier exists.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier

        Returns
        -------
        bool
        """
        return branch_id in self.branches

    def is_default_branch(self, branch_id: str) -> bool:
        """Test if a given branch is the default branch.

        Returns
        -------
        bool
        """
        if self.default_branch is None:
            return False
        else:
            return self.default_branch.identifier == branch_id

    @property
    def last_modified_at(self):
        """The timestamp of last modification is either the time when the
        viztrail was created or when any of the viztrail branches was modified.

        Returns
        -------
        datatime.datatime
        """
        ts = self.created_at
        for branch in list(self.branches.values()):
            branch_ts = branch.last_modified_at
            if ts < branch_ts:
                ts = branch_ts
        return ts

    def list_branches(self) -> List[BranchHandle]:
        """Get a list of branches that are currently defined for the viztrail.

        Returns
        -------
        list(vizier.viztrail.branch.BranchHandle)
        """
        return list(self.branches.values())

    @abstractmethod
    def set_default_branch(self, branch_id: str) -> BranchHandle:
        """Set the branch with the given identifier as the default branch.
        Raises ValueError if no branch with the given identifier exists.

        Returns the branch handle for the new default.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        raise NotImplementedError()

    @abstractmethod
    def create_module(self, 
        command: ModuleCommand,
        external_form: str,
        state: int = MODULE_PENDING,
        timestamp: ModuleTimestamp = ModuleTimestamp(),
        outputs: ModuleOutputs = ModuleOutputs(), 
        provenance: ModuleProvenance = ModuleProvenance(),
        identifier: Optional[str] = None,
    ) -> ModuleHandle:
        """
        Create a module handle in a format native to this repository.  
        If the repository is persisent, this should also persist the 
        module.
        """
        raise NotImplementedError
