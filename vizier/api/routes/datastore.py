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

"""Url factory for clients that access the datasore of a viztrail project. The
factory os a wrapper around a UrlFactory.
"""

from vizier.api.routes.base import PAGE_LIMIT


class DatastoreClientUrlFactory(object):
    """Url factory for API client datastores that access and manipulate datasets
    for a vizier project. This class is a wrapper around a UrlFactory and the
    project identifier.
    """
    def __init__(self, urls, project_id):
        """Intialize the url factory and the project identifier.

        Parameters
        ----------
        urls: vizier.api.routes.base.UrlFactory
            the default url factory
        project_id: string
            Identifier of the project whoses datasets are accessed
        """
        self.urls = urls
        self.project_id = project_id

    # --------------------------------------------------------------------------
    # Datasets
    # --------------------------------------------------------------------------
    def create_dataset(self):
        """Url to create a new dataset.

        Returns
        -------
        string
        """
        return self.urls.create_dataset(self.project_id)

    def get_dataset(self, dataset_id):
        """Url to retrieve dataset rows.

        Parameters
        ----------
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        url = self.urls.get_dataset(self.project_id, dataset_id)
        return url + '?' + PAGE_LIMIT + '=-1'

    def get_dataset_annotations(self, dataset_id):
        """Url to retrieve dataset annotations.

        Parameters
        ----------
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.urls.get_dataset_annotations(self.project_id, dataset_id)

    def get_dataset_descriptor(self, dataset_id):
        """Url to retrieve a dataset descriptor.

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
        return self.urls.get_dataset_descriptor(self.project_id, dataset_id)

    def update_dataset_annotations(self, dataset_id):
        """Url to update dataset annotations.

        Parameters
        ----------
        dataset_id: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return self.urls.update_dataset_annotations(self.project_id, dataset_id)
