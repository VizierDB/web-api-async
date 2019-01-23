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

"""Implementation of the datastore factory interface for remote datasore
clients.
"""

from vizier.api.client.datastore.base import DatastoreClient
from vizier.api.routes.base import UrlFactory
from vizier.api.routes.datastore import DatastoreClientUrlFactory
from vizier.datastore.factory import DatastoreFactory


class DatastoreClientFactory(DatastoreFactory):
    """Create datastore instances that access remote datastores via the web
    service API.
    """
    def __init__(self, base_url):
        """Initialize the base url of the web service API.

        Parameters
        ----------
        base_url: string
            Base url of the web service API
        """
        self.webservice_url = base_url

    def delete_datastore(self, identifier):
        """Delete a datastore. This method is normally called when the project
        with which the datastore is associated is deleted.

        The remote datastore cannot delete itself via the web service API at
        this point.

        Parameters
        ----------
        identifier: string
            Unique identifier for datastore
        """
        pass

    def get_datastore(self, identifier):
        """Get the datastore client for the project with the given identifier.

        Paramaters
        ----------
        identifier: string
            Unique identifier for datastore

        Returns
        -------
        vizier.api.client.datastore.base.DatastoreClient
        """
        return DatastoreClient(
            urls=DatastoreClientUrlFactory(
                urls=UrlFactory(base_url=self.webservice_url),
                project_id=identifier
            )
        )
