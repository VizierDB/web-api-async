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

from vizier.core.annotation.base import ObjectAnnotationSet
from vizier.core.timestamp import get_current_time


"""Key's for default viztrail properties."""
# Human readable object name for viztrails and viztrail branches
PROPERTY_NAME = 'name'


class NamedObject(object):
    """Viztrails and branches are named objects. A named object maintains a set
    of user-defined annotations. The annotations with the key defined in
    PROPERTY_NAME is interpreted as the human-readable object name.

    This base class provides getter and setter methods to access and manipulate
    the human-readable object name.

    Attributes
    ----------
    name: string
        Human readable viztrail name
    properties: vizier.core.annotation.base.ObjectAnnotationSet
        Set of user-defined properties that are associated with this viztrail
    """
    def __init__(self, properties):
        """Initialize the object's properties set.

        Parameters
        ----------
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Handler for user-defined properties
        """
        self.properties = properties

    @property
    def name(self):
        """Get the value of the object property with key 'name'. The result is
        None if no such property exists.

        Returns
        -------
        string
        """
        return self.properties.find_one(
            key=PROPERTY_NAME,
            default_value=None,
            raise_error_on_multi_value=False
        )

    @name.setter
    def name(self, value):
        """Set the value of the object property with key 'name'.

        Parameters
        ----------
        name: string
            Human-readable name for the viztrail
        """
        return self.properties.replace(key=PROPERTY_NAME, value=str(value))


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
    properties: vizier.core.annotation.base.ObjectAnnotationSet
        Set of user-defined properties that are associated with this viztrail
    """
    def __init__(
        self, identifier, properties=None, branches=None, default_branch=None, created_at=None
    ):
        """Initialize the viztrail descriptor.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        properties: vizier.core.annotation.base.ObjectAnnotationSet, optional
            Handler for user-defined properties
        branches: list(vizier.viztrail.branch.BranchHandle), optional
            List of branches in the viztrail
        default_branch: vizier.viztrail.branch.BranchHandle, optional
            Default branch for the viztrail
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        """
        super(ViztrailHandle, self).__init__(
            properties=properties if not properties is None else ObjectAnnotationSet()
        )
        self.identifier = identifier
        self.branches = dict()
        # Initialize the branch index from the given list (if present)
        if not branches is None:
            for b in branches:
                self.branches[b.identifier] = b
        self.default_branch = default_branch
        # If created_at timestamp is None the viztrail is expected to be a newly
        # created viztrail.
        self.created_at = created_at if not created_at is None else get_current_time()

    @abstractmethod
    def create_branch(self, provenance=None, properties=None, modules=None):
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
        raise NotImplementedError

    @abstractmethod
    def delete_branch(self, branch_id):
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
        raise NotImplementedError

    def get_default_branch(self):
        """Get the handle for the default branch of the viztrail.

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        return self.default_branch

    def get_branch(self, branch_id):
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

    def is_default_branch(self, branch_id):
        """Test if a given branch is the default branch.

        Returns
        -------
        bool
        """
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
        for branch in self.branches.values():
            branch_ts = branch.last_modified_at
            if ts < branch_ts:
                ts = branch_ts
        return ts

    def list_branches(self):
        """Get a list of branches that are currently defined for the viztrail.

        Returns
        -------
        list(vizier.viztrail.branch.BranchHandle)
        """
        return self.branches.values()

    @abstractmethod
    def set_default_branch(self, branch_id):
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
        raise NotImplementedError
