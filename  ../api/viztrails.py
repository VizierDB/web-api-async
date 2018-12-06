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

"""Vizier API - Viztrail repository - Implements the methods of the API that
directly interact with the viztrail repository component of the Vizier instance.
"""

from vizier.viztrail.base import PROPERTY_NAME
from vizier.viztrail.branch import BranchProvenance


class ViztrailRepositoryApi(object):
    """API wrapper around the viztrails repository object that manages viztrails
    for the Vizier instance. Note that viztrails were originally referred to as
    projects in the API.
    """
    def __init__(self, viztrails_repository):
        """Initialize the object that manages viztrails for the Vizier instance.

        Parameters
        ----------
        viztrail_repository : vizier.viztrail.ViztrailRepository
            Repository for viztrails
        """
        self.repository = viztrails_repository

    # --------------------------------------------------------------------------
    # Projects
    # --------------------------------------------------------------------------
    def create_project(self, properties):
        """Create a new project. All the information about a project is
        currently stored as part of the viztrail.

        Raises ValueError if no valid project name is included in the given
        properties dictionary.

        Parameters
        ----------
        properties : dict
            Dictionary of user-defined project properties

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        if PROPERTY_NAME in properties:
            name = properties[PROPERTY_NAME]
            if name is None or name == '':
                raise ValueError('not a valid project name')
        else:
            raise ValueError('missing project name')
        return self.repository.create_viztrail(properties=properties)

    def delete_project(self, project_id):
        """Delete the project with given identifier. Deletes the viztrail that
        represents the project.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        bool
        """
        return self.repository.delete_viztrail(viztrail_id=project_id)

    def get_project(self, project_id):
        """Get the viztrail handle that represents the project with the given
        identifier.

        Returns None if no project with the given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        return self.repository.get_viztrail(viztrail_id=project_id)

    def list_projects(self):
        """Returns a list of viztrail handles for all projects that are
        currently maintained by the viztrail repository.

        Returns
        ------
        list(vizier.viztrail.base.ViztrailHandle)
        """
        return self.repository.list_viztrails()

    def update_project_properties(self, project_id, properties):
        """Update the set of user-defined properties for a project with given
        identifier.

        The (key, value)-pairs in the properties dictionary define the update
        operations. Values are expected to be either None, a scalar value (i.e.,
        int, float, or string) or a list of scalar values. If the value is None
        the corresponding project property is deleted. Otherwise, the
        corresponding property will be replaced by the value or the values in a
        given list of values.

        Returns None if no project with given identifier exists. Raises a
        ValueError if the name property of the viztrail is deleted or updated to
        an empty string.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        properties : dict
            Dictionary representing property update statements

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.repository.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Ensure that the project name is not set to an empty string
        if PROPERTY_NAME in properties:
            name = properties[PROPERTY_NAME]
            if name is None or name == '':
                raise ValueError('not a valid project name')
        viztrail.properties.update(properties)
        return viztrail

    # --------------------------------------------------------------------------
    # Branches
    # --------------------------------------------------------------------------
    def create_branch(self, project_id, branch_id=None, workflow_id=None, module_id=None, properties=None):
        """Create a new branch for a given project. The branch_id, workflow_id,
        and module_id specify the branch point. The values are either all None,
        in which case an empty branch is created, or all not None.

        Returns None if the specified project does not exist. Raises ValueError
        if the specified branch point does not exists or if the combination of
        values is invalid. Raises ValueError if no valid branch name is given in
        the properties dictionary.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: int
            Unique branch identifier
        workflow_id: int
            Workflow identifier in branch history
        module_id: int
            Module identifier in given workflow version
        properties: dict()
            Properties for new workflow branch

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        # Ensure that the branch point specified properly
        if branch_id is None and (not workflow_id is None or not module_id is None):
            raise ValueError('invalid branch point specification')
        elif not branch_id is None and (workflow_id is None or module_id is None):
            raise ValueError('invalid branch point specification')
        # Raise error if branch name is missing or invalid
        if PROPERTY_NAME in properties:
            name = properties[PROPERTY_NAME]
            if name is None or name == '':
                raise ValueError('not a valid project name')
        else:
            raise ValueError('missing project name')
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.repository.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        if branch_id is None:
            # Create branch and return handle
            return viztrail.create_branch(properties=properties)
        else:
            # Retrieve the branch, workflow and module. If either is None we
            # raise an exception.
            branch = viztrail.get_branch(branch_id)
            if branch is None:
                raise ValueError('unknown branch \'' + str(branch_id) + '\'')
            workflow = branch.get_workflow(workflow_id)
            if workflow is None:
                raise ValueError('unknown workflow \'' + str(workflow_id) + '\'')
            # Create list of modules for the new branch. The branch will include
            # all modules up until (including) the specified module. If the
            # module is not found we raise an exception
            modules = list()
            for m in workflow.modules:
                modules.append(m)
                if m.identifier == module_id:
                    break
            if len(modules) == 0 or modules[-1].identifier != module_id:
                raise ValueError('unknown module \'' + str(module_id) + '\'')
            # Create branch and return handle
            return viztrail.create_branch(
                provenance=BranchProvenance(
                    source_branch=branch_id,
                    workflow_id=workflow_id,
                    module_id=module_id
                ),
                properties=properties,
                modules=modules
            )

    def delete_branch(self, project_id, branch_id):
        """Delete the branch with the given identifier from the given project.

        Returns True if the branch existed and False if the project or branch
        are unknown.

        Raises ValueError if an attempt is made to delete the default branch.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: int
            Unique branch identifier

        Returns
        -------
        bool
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.repository.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Delete viztrail branch. The result is True if the branch existed. This
        # will raise an exception if an attempt is made to delete the default
        # branch.
        return viztrail.delete_branch(branch_id=branch_id)

    def get_branch(self, project_id, branch_id):
        """Retrieve a branch from a given project.

        Returns None if the project or the branch do not exist.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.repository.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        return viztrail.get_branch(branch_id)

    def list_branches(self, project_id):
        """Get a list of all branches for a given project. The result is a list
        of branch handles or None, if the specified project does not exist.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        list(vizier.viztrail.branch.BranchHandle)
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.repository.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        return viztrail.list_branches()

    def set_default_branch(self, project_id, branch_id):
        """Set the default branch for a given project.

        Returns the branch handle for the new default. Raises ValueError if no
        branch with the given identifier exists.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        # Retrieve project viztrail from repository to ensure that it exists.
        viztrail = self.repository.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        return viztrail.set_default_branch(branch_id=branch_id)

    def update_branch_properties(self, project_id, branch_id, properties):
        """Update properties for a given project workflow branch. Returns the
        handle for the modified branch or None if the project or branch do not
        exist.

        The (key, value)-pairs in the properties dictionary define the update
        operations. Values are expected to be either None, a scalar value (i.e.,
        int, float, or string) or a list of scalar values. If the value is None
        the corresponding project property is deleted. Otherwise, the
        corresponding property will be replaced by the value or the values in a
        given list of values.

        Raises a ValueError if the name property of the viztrail is deleted or
        updated to an empty string.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        properties: dict()
            Properties that are being updated. A None value for a property
            indicates that the property is to be deleted.

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        # Retrieve project viztrail and branch to ensure that both exist
        viztrail = self.repository.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        branch = viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Ensure that the branch name is not set to an empty string
        if PROPERTY_NAME in properties:
            name = properties[PROPERTY_NAME]
            if name is None or name == '':
                raise ValueError('not a valid branch name')
        branch.properties.update(properties)
        return branch
