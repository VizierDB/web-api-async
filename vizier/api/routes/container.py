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

"""Url factories for web services when running a configuration in which each
project wrapped inside an individual container.
"""

from vizier.api.routes.base import PAGE_LIMIT, PAGE_OFFSET, UrlFactory


class ContainerApiUrlFactory(object):
    """Factory to create urls for all routes that are supported by a vizier
    API running in a separate container serving a single project.
    """
    def __init__(self, base_url=None, api_doc_url=None):
        """Intialize the base url for the web service. A ValueError is raised
        if the base url is None.

        Parameters
        ----------
        base_url: string, optional
            Prefix for all urls
        api_doc_url: string, optional
            Url for the API documentation
        """
        self.base_url = base_url
        self.api_doc_url = api_doc_url
        # Raise ValueError if the base url is not set
        if self.base_url is None:
            raise ValueError("missing base url argument")
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

    def api_doc(self):
        """Url to the service API documentation.

        Returns
        -------
        string
        """
        return self.api_doc_url

    # --------------------------------------------------------------------------
    # Tasks
    # --------------------------------------------------------------------------
    def cancel_exec(self, task_id):
        """Url to request cancelation of a running task.

        Parameters
        ----------
        task_id: string
            Unique task identifier

        Returns
        -------
        string
        """
        return self.base_url + '/tasks/' + task_id + '/cancel'

    def execute_task(self):
        """Url to request execution of a task.

        Returns
        -------
        string
        """
        return self.base_url + '/tasks/execute'

    # --------------------------------------------------------------------------
    # Datasets
    # --------------------------------------------------------------------------
    def get_dataset(self, project_id, dataset_id):
        """Url to retrieve dataset rows.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.base_url + '/datasets/' + dataset_id

    def get_dataset_descriptor(self, project_id, dataset_id):
        """Url to retrieve dataset descriptor.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.get_dataset(project_id, dataset_id) + '/descriptor'

    def dataset_pagination(self, project_id, dataset_id, offset=0, limit=None):
        """Get Url for dataset row pagination.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id : string
            Unique dataset identifier
        offset: int, optional
            Pagination offset. The returned Url always includes an offset
            parameter
        limit: int, optional
            Dataset row limit. Only included if not None

        Returns
        -------
        string
        """
        query = PAGE_OFFSET + '=' + str(offset)
        if not limit is None:
            query += '&' + PAGE_LIMIT + '=' + str(limit)
        return self.get_dataset(project_id, dataset_id) + '?' + query

    def download_dataset(self, project_id, dataset_id):
        """Url to download a dataset in csv format.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.get_dataset(project_id, dataset_id) + '/csv'

    def get_dataset_annotations(self, project_id, dataset_id):
        """Url to retrieve dataset annotations.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.get_dataset(project_id, dataset_id) + '/annotations'

    def update_dataset_annotations(self, project_id, dataset_id):
        """Url to update dataset annotations.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.get_dataset_annotations(project_id, dataset_id)

    # --------------------------------------------------------------------------
    # Files
    # --------------------------------------------------------------------------
    def download_file(self, project_id, file_id):
        """File download url.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        string
        """
        return self.base_url + '/files/' + file_id

    def upload_file(self, project_id):
        """File upload url for the given project.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        return self.base_url + '/files'


class ContainerEngineUrlFactory(UrlFactory):
    """Url factory for the web service API when running a configuration where
    every project is maintained in a separate container. Overrides all methods
    that create urls for resoures that are accessible directly via the project
    container API.
    """
    def __init__(self, base_url, projects, api_doc_url=None):
        """Intialize the base url for the web service and the cache for
        projects. Each project is expected to maintain a separate factory for
        resources that are accessible via the project container API.

        Parameters
        ----------
        base_url: string
            Prefix for all urls
        projects: vizier.engine.project.cache.base.ProjectCache
            Cache for projects (only used for container engine)
        api_doc_url: string, optional
            Url for the API documentation
        """
        super(ContainerEngineUrlFactory, self).__init__(
            base_url=base_url,
            api_doc_url=api_doc_url
        )
        self.projects = projects

    # --------------------------------------------------------------------------
    # Datasets
    # --------------------------------------------------------------------------
    def get_dataset(self, project_id, dataset_id):
        """Url to retrieve dataset rows.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        project = self.projects.get_project(project_id)
        return project.urls.get_dataset(project_id, dataset_id)

    # --------------------------------------------------------------------------
    # Files
    # --------------------------------------------------------------------------
    def upload_file(self, project_id):
        """File upload url for the given project.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        string
        """
        project = self.projects.get_project(project_id)
        return project.urls.upload_file(project_id)
