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

"""The default vizier engine is used for configurations where the viztrails
repository as well as the datastores and filestores for all projects are
accessible via a central interface. This configuration is assumed to be the
default, oposed to configurations where each project is maintained in a separate
container of vitrual environment and the datastore and filestore is only
accessible via a specific interface.
"""

from vizier.engine.base import VizierEngine


class DefaultVizierEngine(VizierEngine):
    """Default vizier engines for configurations where viztrails, datastores,
    and filestores are maintained and accessible via a single interface.
    """
    def __init__(
        self, name, version, datastores, filestores, viztrails, backend,
        packages
    ):
        """Initialize the engine from a dictionary. Expects a dictionary with
        three elements: 'datastore', 'filestore', and 'viztrails'. The value
        for each of these components is a class loader dictionary.

        The datastore dictionary is expected to load an instance of the
        datastore factory class. The filestore dictionary is expected to load
        and instance of the filestore factory. The viztrail instance is expected
        to load an instance of an viztrails repository. This additional step of
        indirection is added to aviod that the class is fully instantiated
        multiple times when loades as part of a Flask application.

        Parameters
        ----------
        name: string
            Descriptive name for the configuration
        version: string
            Version information for the configurations
        datastores: vizier.datastore.factory.DatastoreFactory
            Factory for project datastores
        filestores: vizier.filestore.factory.FilestoreFactory
            Factory for project filestores
        viztrails: vizier.vizual.repository.ViztrailRepository
            Repository for viztrails
        backend: vizier.engine.backend.base.VizierBackend
            Backend for workflow execution
        packages: dict(vizier.engine.packages.base.PackageIndex)
            Index of loaded packages
        """
        super(DefaultVizierEngine, self).__init__(
            name=name,
            version=version,
            backend=backend,
            packages=packages
        )
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
                filestore=self.filestores.get_filestore(identifier),
                backend=self.backend,
                packages=self.packages
            )
            self.projects[project.identifier] = project

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
        vizier.viztrail.engine.project.ProjectHandle
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
        return self.projects[project_id].get_branch(branch_id)

    def get_project(self, project_id):
        """Get the handle for the given project. Returns None if the project
        does not exist.

        Returns
        -------
        vizier.viztrail.engine.project.ProjectHandle
        """
        if project_id in self.projects:
            return self.projects[project_id]
        return None

    def list_projects(self):
        """Get a list of all project handles.

        Returns
        -------
        list(vizier.viztrail.engine.project.ProjectHandle)
        """
        return self.projects.values()
