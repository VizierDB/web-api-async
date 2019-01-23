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

"""Vizier Branch API - Implements all methods of the API to interact with
project branches.
"""

from vizier.api.base import validate_name
from vizier.viztrail.branch import BranchProvenance

import vizier.api.serialize.branch as serialize
import vizier.api.serialize.labels as labels


class VizierBranchApi(object):
    """The Vizier branch API implements the methods that correspond to
    requests that access and manipulate project branches.
    """
    def __init__(self, projects, urls):
        """Initialize the API components.

        Parameters
        ----------
        projects: vizier.engine.project.cache.base.ProjectCache
            Cache for project handles
        urls: vizier.api.routes.base.UrlFactory
            Factory for resource urls
        """
        self.projects = projects
        self.urls = urls

    def create_branch(
        self, project_id, branch_id=None, workflow_id=None, module_id=None,
        properties=None
    ):
        """Create a new branch for a given project. The branch point is
        specified by the branch_id, workflow_id, and module_id parameters. If
        all values are None an empty branch is created.

        The properties for the new branch are set from the given properties
        dictionary.

        Returns None if the specified project does not exist. Raises ValueError
        if the specified branch point does not exists.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string, optional
            Unique branch identifier
        workflow_id: string, optional
            Unique workflow identifier
        module_id: string, optional
            Unique module identifier
        properties: dict, optional
            Properties for new workflow branch

        Returns
        -------
        dict
        """
        # Retrieve the project from the repository to ensure that it exists
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        if branch_id is None and workflow_id is None and module_id is None:
            # Create an empty branch if the branch point is not specified
            branch = project.viztrail.create_branch(properties=properties)
        else:
            # Ensure that the branch point exist and get the index position of
            # the source module
            source_branch = project.viztrail.get_branch(branch_id)
            if source_branch is None:
                raise ValueError('unknown source branch \'' + str(branch_id) + '\'')
            workflow = source_branch.get_workflow(workflow_id)
            if workflow is None:
                    raise ValueError('unknown workflow \'' + str(workflow_id) + '\'')
            module_index = -1
            for i in range(len(workflow.modules)):
                module = workflow.modules[i]
                if module.identifier == module_id:
                    module_index = i
                    break
            if module_index == -1:
                raise ValueError('unknown module \'' + str(module_id) + '\'')
            modules = [m.identifier for m in workflow.modules[:module_index+1]]
            # Create a new branch that contains all source modules including
            # the specified one.
            branch = project.viztrail.create_branch(
                provenance=BranchProvenance(
                    source_branch=source_branch.identifier,
                    workflow_id=workflow_id,
                    module_id=module_id
                ),
                properties=properties,
                modules=modules
            )
        return serialize.BRANCH_DESCRIPTOR(
            branch=branch,
            project=project,
            urls=self.urls
        )

    def delete_branch(self, project_id, branch_id):
        """Delete the branch with the given identifier from the given
        project.

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
        # Retrieve the project from the repository to ensure that it exists
        project = self.projects.get_project(project_id)
        if project is None:
            return False
        # Delete viztrail branch. The result indicates if the branch existed
        # or not.
        return project.viztrail.delete_branch(branch_id=branch_id)

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
        dict
            Serialization of the project workflow
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist.
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Return serialization of the branch handle
        return serialize.BRANCH_HANDLE(
            project=project,
            branch=branch,
            urls=self.urls
        )

    def update_branch(self, project_id, branch_id, properties):
        """Update properties for a given project workflow branch. Returns the
        handle for the modified workflow or None if the project or branch do not
        exist.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique workflow branch identifier
        properties: dict
            Properties that are being updated. A None value for a property
            indicates that the property is to be deleted.

        Returns
        -------
        dict
        """
        # Retrieve the project and branch from the repository to ensure that
        # they exist.
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        # Update branch properties and return branch descriptor. Ensure that
        # the branch name is not set to an invalid value.
        validate_name(properties, message='not a valid branch name')
        branch.properties.update(properties)
        return serialize.BRANCH_DESCRIPTOR(
            branch=branch,
            project=project,
            urls=self.urls
        )
