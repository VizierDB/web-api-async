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

from typing import List, Dict, Any, Optional, TYPE_CHECKING

from vizier.datastore.dataset import DatasetHandle
from vizier.datastore.reader import InMemDatasetReader
from vizier.datastore.annotation.base import DatasetCaveat
from vizier.datastore.dataset import DatasetRow, DatasetColumn
if TYPE_CHECKING:
    from vizier.api.client.datastore.base import DatastoreClient

class RemoteDatasetHandle(DatasetHandle):
    """Handle for dataset that has been downloaded from a (remote) API."""
    def __init__(self, 
            identifier: str, 
            columns: List[DatasetColumn], 
            rows: List[DatasetRow], 
            store: "DatastoreClient", 
            properties: Dict[str, Any] = dict()):
        """Initialize the dataset handle. The list of rows is a list of
        dictionaries in the default serialization format.

        Parameters
        ----------
        identifier: string, optional
            Unique dataset identifier.
        columns: list(DatasetColumn), optional
            List of columns. It is expected that each column has a unique
            identifier.
        row_count: int, optional
            Number of rows in the dataset
        rows: list(vizier.datastore.dataset.DatasetRow)
            Dataset in default serialization format
        store: vizier.api.client.datastore.base.DatastoreClient
            Reference to the datastore (required to retrieve annotations)
        """
        super(RemoteDatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns
        )
        self.rows = rows
        self.store = store
        self.properties = properties

    def get_caveats(self, 
            column_id: Optional[int] = None, 
            row_id: Optional[str] = None
        ) -> List[DatasetCaveat]:
        """Get all annotations for a given dataset resource. If both identifier
        are None all dataset annotations are returned.

        Parameters
        ----------
        column_id: int, optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier

        Returns
        -------
        list(vizier.datastpre.annotation.base.DatasetAnnotation)
        """
        return self.store.get_caveats(self.identifier, column_id, row_id)


    def reader(self, offset=0, limit=None):
        """Get reader for the dataset to access the dataset rows. The optional
        offset amd limit parameters are used to retrieve only a subset of
        rows.

        Parameters
        ----------
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.

        Returns
        -------
        vizier.datastore.reader.DatasetReader
        """
        if offset == 0 and limit is None:
            return InMemDatasetReader(self.rows)
        elif limit is not None:
            return InMemDatasetReader(self.rows[offset:offset+limit])
        else:
            return InMemDatasetReader(self.rows[offset:])

    def get_properties(self) -> Dict[str, Any]:
        return self.properties

    def get_row_count(self) -> int:
        return len(self.rows)