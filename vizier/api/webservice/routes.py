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

"""The url factory is used to generate url for all resources (routes) that are
accessible via the web service.
"""


class UrlFactory:
    """Factory to create urls for all routes that the webservice supports."""

    def __init__(self, config):
        """Intialize the base url for the web service.

        Parameters
        ----------
        config: vizier.config.AppConfig
            Application configuration parameters
        """
        # Construct base Url from server url, port, and application path.
        self.base_url = config.app_base_url
        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------

    def service_descriptor(self):
        """Base Url for the webservice. Provides access to the service
        descriptor.

        Returns
        -------
        string
        """
        return self.base_url

    # --------------------------------------------------------------------------
    # Projects
    # --------------------------------------------------------------------------
    def create_project(self):
        """Url to create a new project.

        Returns
        -------
        string
        """
        return self.list_projects()

    def delete_project(self, project_id):
        """Url to delete the project with the given identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id)

    def get_project(self, project_id):
        """Url to retrieve the project with the given identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.list_projects() + '/' + project_id

    def list_projects(self):
        """Url to retrieve the list of active projects.

        Returns
        -------
        string
        """
        return self.base_url + '/projects'

    def update_project(self, project_id):
        """Url to update properties for the project with the given identifier.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id)
