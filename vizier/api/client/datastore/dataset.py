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

from vizier.datastore.dataset import DatasetHandle
from vizier.datastore.reader import InMemDatasetReader


class RemoteDatasetHandle(DatasetHandle):
    """Handle for dataset that has been downloaded from a (remote) API."""
    def __init__(self, identifier, columns, row_count, rows, store):
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
            columns=columns,
            row_count=row_count
        )
        self.rows = rows
        self.store = store

    def get_annotations(self, column_id=None, row_id=None):
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
        annotations = self.store.get_annotations(self.identifier)
        if column_id is None and row_id is None:
            return annotations.values
        elif row_id is None:
            return annotations.for_column(row_id)
        elif column_id is None:
            return annotations.for_row(row_id)
        else:
            return annotations.for_cell(column_id=column_id, row_id=row_id)


    def reader(self, offset=0, limit=-1):
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
        if offset == 0 and limit == -1:
            return InMemDatasetReader(self.rows)
        elif limit > 0:
            return InMemDatasetReader(self.rows[offset:offset+limit])
        else:
            return InMemDatasetReader(self.rows[offset:])
