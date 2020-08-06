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

"""Datastore that uses the webs ervice API to create and retrieve datasets. This
datastore is for example used by python cell processors that do not have access
to a shared file system, e.g., processors that are sandboxed in containers.
"""

from typing import List, Dict, Any, Optional
import json
import requests

from vizier.api.client.datastore.dataset import RemoteDatasetHandle
from vizier.datastore.base import Datastore
from vizier.datastore.dataset import DatasetColumn, DatasetRow, DatasetHandle, DatasetDescriptor
from vizier.datastore.annotation.base import DatasetCaveat
from vizier.api.routes.datastore import DatastoreClientUrlFactory

import vizier.api.serialize.dataset as serialize
import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.labels as labels

class DatastoreClient(Datastore):
    """Datastore that is a client to a vizier web service API. Datasets are
    retireved from the API and stored at the API via HTTP requests.

    The datastore only allows access to datasets for a single project.
    """
    def __init__(self, urls: DatastoreClientUrlFactory):
        """Initialize the url factory to retireve and manipulate API resources.

        Parameters
        ----------
        urls: vizier.api.routes.datastore.DatastoreClientUrlFactory
            Factory for urls to access and manipulate datasets
        """
        self.urls = urls

    def create_dataset(self, 
            columns: List[DatasetColumn], 
            rows: List[DatasetRow], 
            properties: Optional[Dict[str, Any]] =None
        ) -> DatasetDescriptor:
        """Create a new dataset in the project datastore using the API. Expects
        a list of columns and the rows for the dataset. All columns and rows
        should have unique non-negative identifier (although this may not
        be enforced by all backend datastores). Depending on the backend
        datastore the given identifier may change. They are, however, required
        to reference the given properties properly.

        Raises ValueError if the backend datastore rejects the given dataset.

        Parameters
        ----------
        columns: list(vizier.datastore.dataset.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.dataset.DatasetRow)
            List of dataset rows.
        properties: dict, optional
            Properties for dataset components

        Returns
        -------
        vizier.datastore.dataset.DatasetDescriptor
        """
        url = self.urls.create_dataset()
        data:Dict[str, Any] = {
            labels.COLUMNS: [serialize.DATASET_COLUMN(col) for col in columns],
            labels.ROWS: [serialize.DATASET_ROW(row) for row in rows]
        }
        if not properties is None:
            data[labels.PROPERTIES] = properties
        # Send request. Raise exception if status code indicates that the
        # request was not successful.
        r = requests.post(url, json=data)
        r.raise_for_status()
        obj = json.loads(r.text)
        return deserialize.DATASET_DESCRIPTOR(obj)

    def get_dataset(self, identifier: str) -> Optional[DatasetHandle]:
        """Get the handle for the dataset with given identifier from the data
        store. Returns None if no dataset with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        url = self.urls.get_dataset(identifier)
        r = requests.get(url)
        if r.status_code == 404:
            return None
        elif r.status_code != 200:
            r.raise_for_status()
        # The result is the workflow handle
        obj = json.loads(r.text)
        ds = deserialize.DATASET_DESCRIPTOR(obj)
        return RemoteDatasetHandle(
            identifier=ds.identifier,
            columns=ds.columns,
            rows=[deserialize.DATASET_ROW(row) for row in obj[labels.ROWS]],
            store=self
        )

    def get_caveats(self, 
            identifier: str, 
            column_id: Optional[int] = None, 
            row_id: Optional[str] = None
        ) -> List[DatasetCaveat]:
        """Get all dataset annotations.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.annotation.dataset.DatasetMetadata
        """
        url = self.urls.get_dataset_caveats(identifier, column_id, row_id)
        r = requests.get(url)
        if r.status_code == 404:
            return list()
        elif r.status_code != 200:
            r.raise_for_status()
        # The result is the workflow handle
        return [deserialize.CAVEAT(obj) for obj in json.loads(r.text)]

    def get_descriptor(self, identifier):
        """Get the descriptor for the dataset with given identifier from the
        data store. Returns None if no dataset with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.DatasetDescriptor
        """
        url = self.urls.get_dataset_descriptor(identifier)
        r = requests.get(url)
        if r.status_code == 404:
            return None
        elif r.status_code != 200:
            r.raise_for_status()
        # The result is the workflow handle
        obj = json.loads(r.text)
        return deserialize.DATASET_DESCRIPTOR(obj)