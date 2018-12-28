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

"""The url factory is used to generate the url that can be used by external
workers to update the status of a task.
"""

from vizier.core.loader import ClassLoader


class TaskUrlFactory:
    """Factory to create urls for route that is used update workflow status."""

    def __init__(self, urls=None, properties=None):
        """Intialize the internal components. The factory has a reference to a
        url factory to be able to generate project urls. Initialization can be
        done via a properties dictionary. A given properties dictionary
        overrides a given url factory object. The dictionary is expected to
        contain a class loader definition. If both parameters are None a
        ValueError is raised.

        Parameters
        ----------
        urls: vizier.api.routes.base.UrlFactory, optional
            Factory for project urls
        properties: dict, optional
            Initialize the factory from a dictionary.
        """
        # Raise value error if both arguments are None
        if urls is None and properties is None:
            raise ValueError('invalid arguments')
        self.urls = urls
        if not properties is None:
            # The properties are expected to contain a class loader definition
            self.urls = ClassLoader(values=properties).get_instance()

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
        return self.urls.get_project(project_id) + '/tasks/' + task_id
