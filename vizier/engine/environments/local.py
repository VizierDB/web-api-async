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

"""Implements vizier execution environments for local installation of vizier.
All of these configurations assume that the typical use-case is a single user
that has full controll over the machine. All package will have access to all
local files and resources that the user can access. The user is expected to work
promarily on a single object with limited number of modules being executed in
parallel.
"""

from vizier.core.loader import ClassLoader
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.base import VizierEngine
from vizier.engine.project import ProjectHandle


"""Element names for the properties dictionary."""
PROPERTY_DATASTORE = 'datastore'
PROPERTY_FILESTORE = 'filestore'
PROPERTY_VIZTRAILS = 'viztrails'

PROPERTIES = [PROPERTY_DATASTORE, PROPERTY_FILESTORE, PROPERTY_VIZTRAILS]


class DefaultLocalEngine(VizierEngine):
    """The default local engine uses a multi-process backend. Expects datastore
    and filestore factories to create instances of the respective types for all
    projects.
    """
    def __init__(self, properties, packages):
        """Initialize the engine from a dictionary. Expects a dictionary with
        three elements: 'datastore', 'filestore', and 'viztrails'. The value
        for each of these components is a class loader dictionary.

        The datastore dictionary is expected to load an instance of the
        datastore factory class. The filestore dictionary is expected to load
        and instance of the filestore factory. The viztrail instance is expected
        to load an instance of an viztrails repository.

        Raises a ValueError if any of the mandatory properties is missing.

        Parameters
        ----------
        properties: dict()
            Configuration object for the local engine
        packages: dict(vizier.engine.packages.base.PackageIndex)
            Index of loaded packages
        """
        super(DefaultLocalEngine, self).__init__(
            name='Default Local Engine',
            version='0.1.0',
            packages=packages
        )
        for key in PROPERTIES:
            if not key in properties:
                raise ValueError('missing element \'' + key + '\' in properties')
        # This part is a bit hacky. Initialize the datastore factory, filestore
        # factory and the viztrails repository with class loaders. These loaders
        # will be replaced with class instances when the .init() method is
        # called.
        self.datastores = ClassLoader(values=properties[PROPERTY_DATASTORE])
        self.filestores = ClassLoader(values=properties[PROPERTY_FILESTORE])
        self.viztrails = ClassLoader(values=properties[PROPERTY_VIZTRAILS])

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
        backend = MultiProcessBackend(
            datastore=datastore,
            filestore=filestore,
            processors=self.processors
        )
        project = ProjectHandle(
            viztrail=viztrail,
            datastore=datastore,
            filestore=filestore,
            backend=backend,
            packages=self.packages
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

    def init(self):
        """Initialize the engine by replacing (i) the initialized class loaders
        with class instances, and (ii) the package indexes with package task
        engines.
        """
        self.datastores = self.datastores.get_instance()
        self.filestores = self.filestores.get_instance()
        self.viztrails = self.viztrails.get_instance()
        self.processors = dict()
        for key in self.packages:
            loader = ClassLoader(self.packages[key].engine)
            self.processors[key] = loader.get_instance()
        # Create index of project handles from existing viztrails
        self.projects = dict()
        for viztrail in self.viztrails.list_viztrails():
            identifier = viztrail.identifier
            datastore = self.datastores.get_datastore(identifier)
            filestore = self.filestores.get_filestore(identifier)
            backend = MultiProcessBackend(
                datastore=datastore,
                filestore=filestore,
                processors=self.processors
            )
            project = ProjectHandle(
                viztrail=viztrail,
                datastore=datastore,
                filestore=filestore,
                backend=backend,
                packages=self.packages
            )
            self.projects[project.identifier] = project

    def list_projects(self):
        """Get a list of all project handles.

        Returns
        -------
        list(vizier.viztrail.engine.project.ProjectHandle)
        """
        return self.projects.values()
