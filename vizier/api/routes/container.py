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
from typing import Optional, TYPE_CHECKING

from vizier.api.routes.base import PAGE_LIMIT, PAGE_OFFSET, UrlFactory, FORCE_PROFILER
if TYPE_CHECKING:
    from vizier.engine.project.cache.container import ContainerProjectCache


class ContainerApiUrlFactory(UrlFactory):
    """Factory to create urls for all routes that are supported by a vizier
    API running in a separate container serving a single project.
    """
    def __init__(self, 
            base_url: str, 
            api_doc_url: Optional[str] = None):
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
        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------

    def service_descriptor(self) -> str:
        """Base Url for the webservice. Provides access to the service
        descriptor.

        Returns
        -------
        string
        """
        return self.base_url

    def api_doc(self) -> Optional[str]:
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
    def get_dataset(self, project_id: str, dataset_id: str, force_profiler: Optional[bool] = None) -> str:
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
        url = self.base_url + '/datasets/' + dataset_id
        if force_profiler is not None and force_profiler:
            url += "?{}=true".format(FORCE_PROFILER)
        return url

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

    def download_dataset(self, project_id: str, dataset_id: str) -> str:
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

    def get_dataset_caveats(self, project_id, dataset_id):
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

    def get_dataset_profiling(self, project_id, dataset_id):
        """Url to retrieve dataset profiling results.

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
        return self.get_dataset(project_id, dataset_id) + '/profiling'

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

    def upload_file(self, project_id: str) -> str:
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
    def __init__(self, 
            base_url: str, 
            projects: "ContainerProjectCache", 
            api_doc_url: Optional[str] = None):
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
    def get_dataset(self, project_id: str, dataset_id: str, force_profiler: Optional[bool] = None) -> str:
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
        return project.urls.get_dataset(project_id, dataset_id, force_profiler = force_profiler)

    # --------------------------------------------------------------------------
    # Files
    # --------------------------------------------------------------------------
    def upload_file(self, project_id: str) -> str:
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
