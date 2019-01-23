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

"""The common project cache solely uses the functionality of a provided
viztrails repository to manipulate the cached objects.
"""

from vizier.engine.project.base import ProjectHandle
from vizier.engine.project.cache.base import ProjectCache


class CommonProjectCache(ProjectCache):
    """The common project cache is a simple wrapper around a viztrail
    repository, a datastore factory, and a filestore factory.
    """
    def __init__(self, datastores, filestores, viztrails):
        """Initialize the cache components and load all projects in the given
        viztrails repository. Maintains all projects in an dictionary keyed by
        their identifier.

        Parameters
        ----------
        datastores: vizier.datastore.factory.DatastoreFactory
            Factory for project datastores
        filestores: vizier.filestore.factory.FilestoreFactory
            Factory for project filestores
        viztrails: vizier.vizual.repository.ViztrailRepository
            Repository for viztrails
        """
        self.datastores = datastores
        self.filestores = filestores
        self.viztrails = viztrails
        # Create index of project handles from existing viztrails
        self.projects = dict()
        for viztrail in self.viztrails.list_viztrails():
            identifier = viztrail.identifier
            project = ProjectHandle(
                viztrail=viztrail,
                datastore=self.datastores.get_datastore(identifier),
                filestore=self.filestores.get_filestore(identifier)
            )
            self.projects[viztrail.identifier] = project

    def create_project(self, properties=None):
        """Create a new project. Will create a viztrail in the underlying
        viztrail repository. The initial set of properties is an optional
        dictionary of (key,value)-pairs where all values are expected to either
        be scalar values or a list of scalar values. The properties are passed
        to the create method for the viztrail repository.

        Parameters
        ----------
        properties: dict, optional
            Set of properties for the new viztrail

        Returns
        -------
        vizier.engine.project.base.ProjectHandle
        """
        viztrail = self.viztrails.create_viztrail(properties=properties)
        datastore = self.datastores.get_datastore(viztrail.identifier)
        filestore = self.filestores.get_filestore(viztrail.identifier)
        project = ProjectHandle(
            viztrail=viztrail,
            datastore=datastore,
            filestore=filestore
        )
        self.projects[project.identifier] = project
        return project

    def delete_project(self, project_id):
        """Delete all resources that are associated with the given project.
        Returns True if the project existed and False otherwise.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        bool
        """
        if project_id in self.projects:
            viztrail = self.projects[project_id].viztrail
            self.viztrails.delete_viztrail(viztrail.identifier)
            self.datastores.delete_datastore(viztrail.identifier)
            self.filestores.delete_filestore(viztrail.identifier)
            del self.projects[project_id]
            return True
        return False

    def get_branch(self, project_id, branch_id):
        """Get the branch with the given identifier for the specified project.
        The result is None if the project of branch does not exist.

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
        # If the project is not in the internal cache it does not exist
        if not project_id in self.projects:
            return None
        # Return the handle for the specified branch
        return self.projects[project_id].viztrail.get_branch(branch_id)

    def get_project(self, project_id):
        """Get the handle for project. Returns None if the project does not
        exist.

        Returns
        -------
        vizier.engine.project.base.ProjectHandle
        """
        if project_id in self.projects:
            return self.projects[project_id]
        return None

    def list_projects(self):
        """Get a list of handles for all projects.

        Returns
        -------
        list(vizier.engine.project.base.ProjectHandle)
        """
        return self.projects.values()
