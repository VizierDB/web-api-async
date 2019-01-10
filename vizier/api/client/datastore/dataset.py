# Copyright (C) 2019 New York University,
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

from vizier.datastore.dataset import DatasetHandle
from vizier.datastore.reader import InMemDatasetReader


class RemoteDatasetHandle(DatasetHandle):
    """Handle for dataset that has been downloaded from a (remote) API."""
    def __init__(self, identifier, columns, row_count, rows):
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
        """
        super(RemoteDatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=row_count
        )
        self.row = rows

    @abstractmethod
    def get_annotations(self, column_id=-1, row_id=-1):
        """Get list of annotations for a dataset component. Expects at least one
        of the given identifier to be a valid identifier (>= 0).

        Parameters
        ----------
        column_id: int, optional
            Unique column identifier
        row_id: int, optiona
            Unique row identifier

        Returns
        -------
        list(vizier.datastore.metadata.Annotation)
        """
        raise NotImplementedError

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
        if offset == 0 and limit == -1
            return InMemDatasetReader(rows)
        elif limit >0:
            return InMemDatasetReader(rows[offset:offset+limit])
        else:
            return InMemDatasetReader(rows[offset:])
