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


"""Pagination query parameter."""
PAGE_LIMIT = 'limit'
PAGE_OFFSET = 'offset'


class UrlFactory:
    """Factory to create urls for all routes that the webservice supports."""

    def __init__(self, base_url, api_doc_url=None):
        """Intialize the base url for the web service.

        Parameters
        ----------
        config: vizier.config.app.AppConfig
            Application configuration parameters
        """
        self.base_url = base_url
        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    def set_task_state(self, project_id, task_id):
        """Url to modify the state of a given task.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        task_id: string
            Unique task identifier

        Returns
        -------
        string
        """
        return self.get_project(project_id) + '/tasks/' + task_id
