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
# limitations under the License.

"""The url factory is used to generate the url that can be used by external
workers to update the status of a task.
"""

from vizier.api.routes.base import PROPERTIES_BASEURL


class TaskUrlFactory(object):
    """Factory to create urls for route that is used update workflow status."""
    def __init__(self, base_url=None, properties=None):
        """Intialize the internal components. The factory maintains the base url
        of the web service. A given properties dictionary overrides a given url.
        The dictionary is expected to contain the base url property. If no
        base url is given a ValueError is raised.

        Parameters
        ----------
        base_url: string
            Base url for web service
        properties: dict, optional
            Initialize the factory from a dictionary.
        """
        self.base_url = base_url
        if not properties is None:
            if PROPERTIES_BASEURL in properties:
                self.base_url = properties[PROPERTIES_BASEURL]
        # Raise ValueError if the base url is not set
        if self.base_url is None:
            raise ValueError('missing base url argument')
        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    def set_task_state(self, task_id):
        """Url to modify the state of a given task.

        Parameters
        ----------
        task_id: string
            Unique task identifier

        Returns
        -------
        string
        """
        return self.base_url + '/tasks/' + task_id
