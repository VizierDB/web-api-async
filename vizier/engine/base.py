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
deleting, and manipulating projects. Different implementations of the engine
will use different implementations for datasores, filestores, vitrails
repositories and backends.

The idea is to have a set of pre-defined 'configurations' for vizier engines
that can be used in different environments (i.e., when running vizier on a local
machine, with a set of celery workers, running each project in its own
container, etc. The environment that is used by a vizier instance is specified
in the configuration file and loaded when the instance is started.
"""

from abc import abstractmethod


class VizierEngine(object):
    """Interface that is used by the API to create and manipulate objects. Each
    implementation should have a descriptive name and version information.

    The engine maintains the viztrail repository.

    When the engine is created it will receive an implementation-specific
    dictionary of properties.
    """
    def __init__(self, name, version, packages):
        """initialize the engine name and version information.

        Note that an implementation of this class should provide a constructor
        that expects as the only argument a dictionary. The content of the
        dictionary is implementation-specific and can be used to configure the
        inital state of the engine. For the webservice the vizier configuration
        file will contain the referecne to the used engine and the configuration
        dictionary.

        Parameters
        ----------
        name: string
            Descriptive name for an engine configuration.
        version: string
            Version information
        packages: dict(vizier.engine.packages.base.PackageIndex)
            Index of loaded packages
        """
        self.name = name
        self.version = version
        # Keep a reference for the package index.
        self.packages = packages

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def delete_project(self, project_id):
        """Delete all resources that are associated with the given project.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def get_project(self, project_id):
        """Get the handle for the given project. Returns None if the project
        does not exist.

        Returns
        -------
        vizier.viztrail.engine.project.ProjectHandle
        """
        raise NotImplementedError

    @abstractmethod
    def init(self):
        """Initialization method that is called before the first call to any of
        the other engine methods.
        """
        raise NotImplementedError

    @abstractmethod
    def list_projects(self):
        """Get a list of all project handles.

        Returns
        -------
        list(vizier.viztrail.engine.project.ProjectHandle)
        """
        raise NotImplementedError
