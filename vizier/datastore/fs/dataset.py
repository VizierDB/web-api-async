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

"""Implementation of the abstract dataset handle class for the default file
system datastore. Each dataset is maintained in a separate folder containing
two files: the dataset descriptor and the data itself.

The dataset descriptor is stored in a file in Json format that contains the
schema, row count, and the counters for column and row identifier.

The data file is also in Json format containing one an array of rows where each
row is an object with id and an array of values, one for each of the columns in
the dataset schema.
"""

import json

from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.datastore.dataset import DatasetColumn, DatasetHandle
from vizier.datastore.reader import DefaultJsonDatasetReader


"""Json element labels for dataset serialization."""
KEY_IDENTIFIER = 'id'
KEY_COLUMN_ID = 'id'
KEY_COLUMN_NAME = 'name'
KEY_COLUMN_TYPE = 'type'
KEY_COLUMNS = 'columns'
KEY_ROWCOUNT = 'rowCount'
KEY_MAXROWID = 'maxRowId'


class FileSystemDatasetHandle(DatasetHandle):
    """Handle for a dataset that is stored on the file system.

    The dataset handle keeps counters for columns and rows id's to generate
    unique unique identifier.

    The dataset rows are stored in a separate file. The file format is JSON with
    the following structure:
        {
            'rows': [
                {'id': int, 'values': [...]}
            ]
        }
    """
    def __init__(
        self, identifier, columns, max_row_id, data_file, row_count=0,
        annotations=None
    ):
        """Initialize the dataset handle.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.
        columns: list(vizier.datastore.base.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        data_file: string
            Path to the file that contains the dataset rows. The data is stored
            in Json format.
        rows: int, optional
            Number of rows in the dataset
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata, optional
            Annotations for dataset components
        """
        super(FileSystemDatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=row_count,
            annotations=annotations
        )
        self.data_file = data_file
        if max_row_id is None:
            raise ValueError('invalid max')
        self._max_row_id = max_row_id

    @staticmethod
    def from_file(descriptor_file, data_file, annotations=None):
        """Read dataset descriptor from file and return a new instance of the
        dataset handle.

        Parameters
        ----------
        descriptor_file: string
            Path to the file containing the dataset descriptor
        data_file: string
            Path to the file that contains the dataset rows.
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata, optional
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.fs.dataset.FileSystemDatasetHandle
        """
        with open(descriptor_file, 'r') as f:
            doc = json.loads(f.read())
        return FileSystemDatasetHandle(
            identifier=doc[KEY_IDENTIFIER],
            columns=[
                DatasetColumn(
                    identifier=col[KEY_COLUMN_ID],
                    name=col[KEY_COLUMN_NAME],
                    data_type=col[KEY_COLUMN_TYPE]
                ) for col in doc[KEY_COLUMNS]
            ],
            data_file=data_file,
            row_count=doc[KEY_ROWCOUNT],
            max_row_id=doc[KEY_MAXROWID],
            annotations=annotations
        )

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
        if column_id is None and row_id is None:
            return self.annotations.values
        elif row_id is None:
            return self.annotations.for_column(row_id)
        elif column_id is None:
            return self.annotations.for_row(row_id)
        else:
            return self.annotations.for_cell(column_id=column_id, row_id=row_id)

    def max_row_id(self):
        """Get maximum identifier for all rows in the dataset. If the dataset
        is empty the result is -1.

        Returns
        -------
        int
        """
        return self._max_row_id

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
        vizier.datastore.reader.DefaultJsonDatasetReader
        """
        return DefaultJsonDatasetReader(
            self.data_file,
            columns=self.columns,
            offset=offset,
            limit=limit
        )

    def to_file(self, descriptor_file):
        """Write dataset descriptor to file. The default serialization format is
        Json.

        Parameters
        ----------
        descriptor_file: string
            Path to the file containing the dataset descriptor
        """
        doc = {
            KEY_IDENTIFIER: self.identifier,
            KEY_COLUMNS: [{
                    KEY_COLUMN_ID: col.identifier,
                    KEY_COLUMN_NAME: col.name,
                    KEY_COLUMN_TYPE: col.data_type
                } for col in self.columns],
            KEY_ROWCOUNT: self.row_count,
            KEY_MAXROWID: self._max_row_id
        }
        with open(descriptor_file, 'w') as f:
            json.dump(doc, f)
