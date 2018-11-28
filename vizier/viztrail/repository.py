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

"""Abstract class for viztrail repositories. The repository maintains a set of
viztrails. It provides methods to create, delete, and access viztrails.
"""

from abc import abstractmethod

from vizier.core.system import VizierSystemComponent


class ViztrailRepository(VizierSystemComponent):
    """Repository for viztrails. This is an abstract class that defines all
    necessary methods to maintain and manipulate viztrails.
    """
    def __init__(self, build, viztrails=None):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        build : dict()
            Build information
        viztrails: list(vizier.viztrail.base.ViztrailHandle)
            List of viztrails that are maintained by the repository
        """
        super(ViztrailRepository, self).__init__(build)
        self.viztrails = dict()
        # Initialize the viztrail index from the given list
        if not viztrails is None:
            for vt in viztrails:
                self.viztrails[vt.identifier] = vt

    @abstractmethod
    def create_viztrail(self, properties=None):
        """Create a new viztrail. The initial set of properties is an optional
        dictionary of (key,value)-pairs where all values are expected to either
        be scalar values or a list of scalar values.

        Parameters
        ----------
        properties: dict, optional
            Set of properties for the new viztrail

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def delete_viztrail(self, viztrail_id):
        """Delete the viztrail with given identifier. The result is True if a
        viztrail with the given identifier existed, False otherwise.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    def get_branch(self, viztrail_id, branch_id):
        """This is a shortcut to retrieve a particular branch from a viztrail.
        The result is None if either the viztrail or the branch does not exist.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        vt = self.get_viztrail(viztrail_id)
        if vt is None:
            return None
        return vt.get_branch(branch_id)

    def get_viztrail(self, viztrail_id):
        """Retrieve the viztrail with the given identifier. The result is None
        if no viztrail with given identifier exists.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        if viztrail_id in self.viztrails:
            return self.viztrails[viztrail_id]
        else:
            return None

    def get_workflow(self, viztrail_id, branch_id, workflow_id):
        """This is a shortcut to retrieve a particular workflow version from a
        viztrail branch. The result is None if either of the identified objects
        does not exist.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier
        workflow_id: string, optional
            Unique workflow identifier

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        vt = self.get_viztrail(viztrail_id)
        if vt is None:
            return None
        branch = vt.get_branch(branch_id)
        if branch is None:
            return None
        return branch.get_workflow(workflow_id)

    def list_viztrails(self):
        """List handles for all viztrails in the repository.

        Returns
        -------
        list(vizier.viztrail.base.ViztrailHandle)
        """
        return self.viztrails.values()
