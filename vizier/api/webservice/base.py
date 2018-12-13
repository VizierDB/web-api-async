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

"""Vizier API - Implements all methods of the API to interact with a running
Vizier instance.

The API orchestrates the interplay
between different components such as the viztrail repository that manages
viztrails and the workflow engine that executes modules in viztrail workflow
versions.

Internally the API is further divided into four parts that deal with the file
store, data store, viztrail repository and the workflow execution engine.
"""

from vizier.api.webservice.routes import UrlFactory
from vizier.api.serialize.base import HATEOAS
from vizier.api.serialize.project import PROJECT_DESCRIPTOR, PROJECT_HANDLE
from vizier.core import VERSION_INFO
from vizier.core.timestamp import get_current_time



class VizierApi(object):
    """The Vizier API implements the methods that correspond to requests that
    are supported by the Vizier Web Service. the API, however, can also be used
    in a stand-alone manner, e.g., via the command line interpreter tool.

    This class is a wrapper around the different components of the Vizier system
    that are necessary for the Web Service, i.e., file store, data store,
    viztrail repository, and workflow execution engine.
    """
    def __init__(self, config):
        """Initialize the API components.

        Parameters
        ----------
        filestore: vizier.filestore.base.Filestore
            Backend store for uploaded files
        viztrails_repository: vizier.viztrail.repository.ViztrailRepository
            Viztrail manager for the API instance
        """
        self.engine = config.get_api_engine()
        self.urls = UrlFactory(config)
        self.service_descriptor = {
            'name': config.webservice.name,
            'version': VERSION_INFO,
            'startedAt': get_current_time().isoformat(),
            'defaults': {
                'maxFileSize': config.webservice.defaults.max_file_size
            },
            'environment': {
                'name': self.engine.name,
                'version': self.engine.version
            },
            'links': HATEOAS({
                'self': self.urls.service_descriptor(),
                'doc': config.webservice.doc_url,
                'projects:create': self.urls.create_project(),
                'projects:list': self.urls.list_projects()
            })
        }

    def init(self):
        """Initialize the API before the first request."""
        self.engine.init()

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------
    def service_overview(self):
        """Returns a dictionary containing essential information about the web
        service including HATEOAS links to access resources and interact with
        the service.

        Returns
        -------
        dict
        """
        return self.service_descriptor

    # --------------------------------------------------------------------------
    # Projects
    # --------------------------------------------------------------------------
    def create_project(self, properties):
        """Create a new project. All the information about a project is
        currently stored as part of the viztrail.

        Parameters
        ----------
        properties : dict
            Dictionary of user-defined project properties

        Returns
        -------
        dict
        """
        # Create a new project and return the serialized descriptor
        project = self.engine.create_project(properties=properties)
        return PROJECT_DESCRIPTOR(project, self.urls)

    def delete_project(self, project_id):
        """Delete the project with given identifier. Deletes all resources that
        are associated with the project.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        bool
            True, if project existed and False otherwise
        """
        # Delete entry in repository. The result indicates whether the project
        # existed or not.
        return self.engine.delete_project(project_id)

    def get_project(self, project_id):
        """Get comprehensive information for the project with the given
        identifier.

        Returns None if no project with the given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier

        Returns
        -------
        dict
        """
        # Retrieve the project from the repository to ensure that it exists.
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        # Get serialization for project handle.
        return PROJECT_HANDLE(project, self.urls)

    def list_projects(self):
        """Returns a list of descriptors for all projects that are currently
        contained in the project repository.

        Returns
        ------
        dict
        """
        return {
            'projects': [
                PROJECT_DESCRIPTOR(project, self.urls)
                    for project in self.engine.list_projects()
            ],
            'links': HATEOAS({
                'self': self.urls.list_projects(),
                'projects:create': self.urls.create_project()
            })
        }

    def update_project(self, project_id, properties):
        """Update the set of user-defined properties for a project with given
        identifier.

        Returns None if no project with given identifier exists.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        properties : dict
            Dictionary representing property update statements

        Returns
        -------
        dict
            Serialization of the project handle
        """
        # Retrieve the project from the repository to ensure that it exists.
        project = self.engine.get_project(project_id)
        if project is None:
            return None
        # Update properties that are associated with the viztrail. Make sure
        # that a new project name, if given, is not the empty string.
        if 'name' in properties:
            project_name = properties['name']
            if not project_name is None:
                if project_name == '':
                    raise ValueError('not a valid project name')
        project.viztrail.properties.update(properties)
        # Return serialization for project handle.
        return PROJECT_DESCRIPTOR(project, self.urls)

    # --------------------------------------------------------------------------
    # Branches
    # --------------------------------------------------------------------------
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
        # Delete viztrail branch. The result is None if either the viztrail or
        # the branch does not exist.
        viztrail = self.viztrails.delete_branch(
            viztrail_id=project_id,
            branch_id=branch_id
        )
        return not viztrail is None

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
        # Get viztrail to ensure that it exist.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Return serialization if branch does exist, otherwise None
        if branch_id in viztrail.branches:
            branch = viztrail.branches[branch_id]
            return serialize.BRANCH_HANDLE(viztrail, branch, self.urls)

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
        properties: dict()
            Properties that are being updated. A None value for a property
            indicates that the property is to be deleted.

        Returns
        -------
        dict
        """
        # Get the viztrail to ensure that it exists
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
            return None
        # Get the specified branch
        if not branch_id in viztrail.branches:
            return None
        # Update properties that are associated with the workflow
        viztrail.branches[branch_id].properties.update_properties(properties)
        return serialize.BRANCH_HANDLE(
            viztrail,
            viztrail.branches[branch_id],
            urls=self.urls
        )
