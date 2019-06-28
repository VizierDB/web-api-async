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

"""Vizier Project API - Implements all methods of the API to interact with
vizier projects.
"""

from vizier.api.base import validate_name

import vizier.api.serialize.base as serialize
import vizier.api.serialize.hateoas as ref
import vizier.api.serialize.labels as labels
import vizier.api.serialize.project as serialpr


class VizierProjectApi(object):
    """The Vizier project API implements the methods that correspond to
    requests that access and manipulate projects.
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
        project = self.projects.create_project(properties=properties)
        return serialpr.PROJECT_DESCRIPTOR(project=project, urls=self.urls)

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
        return self.projects.delete_project(project_id)

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
        # Retrieve the project from the repository to ensure that it exists
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        # Get serialization for project handle.
        return serialpr.PROJECT_HANDLE(
            project=project,
            urls=self.urls
        )

    def list_projects(self):
        """Returns a list of descriptors for all projects that are currently
        contained in the project repository.

        Returns
        ------
        dict
        """
        return {
            'projects': [
                serialpr.PROJECT_DESCRIPTOR(project=project, urls=self.urls)
                    for project in self.projects.list_projects()
            ],
            labels.LINKS: serialize.HATEOAS({
                ref.SELF: self.urls.list_projects(),
                ref.PROJECT_CREATE: self.urls.create_project(),
                ref.PROJECT_IMPORT: self.urls.import_project()
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
        # Retrieve the project from the repository to ensure that it exists
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        # Update properties that are associated with the viztrail. Make sure
        # that a new project name, if given, is not the empty string.
        validate_name(properties, message='not a valid project name')
        project.viztrail.properties.update(properties)
        # Return serialization for project handle.
        return serialpr.PROJECT_DESCRIPTOR(project=project, urls=self.urls)
