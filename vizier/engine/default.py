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

"""The vizier engine defines the interface that is used by the API for creating,
deleting, and manipulating projects. Different instantiations of the engine
will use different implementations for datasores, filestores, vitrails
repositories and backends.

The idea is to have a set of pre-defined 'configurations' for vizier engines
that can be used in different environments (i.e., when running vizier on a local
machine, with a set of celery workers, running each project in its own
container, etc). The configuration that is used by a vizier instance is
specified in the configuration file and loaded when the instance is started.
"""

from vizier.engine.base import VizierEngine


class VizierEngine(VizierEngine):
    """Engine that is used by the API to create and manipulate projects. The
    engine is a wrapper around the datastore, filestore, and viztrails factories
    and repositories, as well as the execution backend. Different configurations
    for a vizier instance will use different classes for the wrapped objects.
    Each configuration should have a descriptive name and version information.

    The engine maintains the viztrail repository.

    When the engine is initialized it expects class loaders for the wrapped
    objects. It is assumed that the init() method is called by the web service
    before the first resource is accessed. The init() method will replace the
    class loaders with instances of the repsective classes.
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
        datastores: vizier.core.loader.ClassLoader
            Class loader for datastore factory
        filestores: vizier.core.loader.ClassLoader
            Class loader for filestore factory
        viztrails: vizier.core.loader.ClassLoader
            Class loader for viztrails repository
        backend: vizier.core.loader.ClassLoader
            Class loader for execution backend
        packages: dict(vizier.engine.packages.base.PackageIndex)
            Index of loaded packages
        """
        self.name = name
        self.version = version
        self.datastores = datastores
        self.filestores = filestores
        self.viztrails = viztrails
        self.backend = backend
        self.packages = packages

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
        with class instances, and (ii) loading the existing project into the
        memory cache.
        """
        self.datastores = self.datastores.get_instance()
        self.filestores = self.filestores.get_instance()
        self.viztrails = self.viztrails.get_instance()
        self.backend = self.backend.get_instance()
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

    def list_projects(self):
        """Get a list of all project handles.

        Returns
        -------
        list(vizier.viztrail.engine.project.ProjectHandle)
        """
        return self.projects.values()
