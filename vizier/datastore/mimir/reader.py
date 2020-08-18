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

"""Implements reader for datasets that are stored in the Mimir backend."""
from typing import List
from vizier.datastore.dataset import DatasetRow
from vizier.datastore.reader import DatasetReader

import vizier.mimir as mimir
import vizier.datastore.mimir.base as base


class MimirDatasetReader(DatasetReader):
    """Dataset reader for Mimir datasets."""
    def __init__(
        self, table_name, columns,
        offset=0, limit=None, rowid=None
    ):
        """Initialize information about the delimited file and the file format.

        Parameters
        ----------
        table_name: string
            Name of table or view in database that contains the dataset
        columns: vizier.datastore.mimir.MimirDatasetColumn
            List of descriptors for columns in the database
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components
        """
        self.table_name = table_name
        self.columns = columns
        if offset < 0:
            raise Exception("Invalid Offset: {}".format(offset))
        self.offset = offset
        if limit is not None and limit < 0:
            raise Exception("Invalid Limit: {}".format(limit))
        self.limit = limit
        self.rowid = rowid
        # Convert row id list into row position index. Depending on whether
        # offset or limit parameters are given we also limit the entries in the
        # dictionary. The internal flag .is_range_query keeps track of whether
        # offset or limit where given (True) or not (False). This information is
        # later used to generate the query for the database.
        if offset > 0 or limit is not None:
            self.is_range_query = True
        else:
            self.is_range_query = False
        # Keep an in-memory copy of the dataset rows when open
        self.is_open = False
        self.read_index = None
        self.rows = None

    def close(self):
        """Close any open files and set the is_open flag to False."""
        self.rows = None
        self.read_index = None
        self.is_open = False

    def __next__(self):
        """Return the next row in the dataset iterator. Raises StopIteration if
        end of row list is reached.

        Automatically closes the reader when end of iteration is reached for
        the first time.

        Returns
        -------
        vizier.datastore.base.DatasetRow
        """
        if self.is_open:
            if self.read_index < len(self.rows):
                row = self.rows[self.read_index]
                self.read_index += 1
                return row
            self.close()
        raise StopIteration

    def open(self) -> "MimirDatasetReader":
        """Setup the reader by querying the database and creating an in-memory
        copy of the dataset rows.

        Returns
        -------
        vizier.datastore.reader.MimirDatasetReader
        """
        # Query the database to retrieve dataset rows if reader is not already
        # open
        if not self.is_open:
            # Query the database to get the list of rows. Sort rows according to
            # order in row_ids and return a InMemReader
            rs = mimir.getTable(
                    table = self.table_name,
                    columns = [col.name_in_rdb for col in self.columns],
                    offset_to_rowid = self.rowid,
                    limit = self.limit if self.is_range_query else None,
                    offset = self.offset if self.is_range_query else None,
                    include_uncertainty = True
                )

            #self.row_ids = rs['prov']
            # Initialize mapping of column rdb names to index positions in
            # dataset rows
            rs_rows = rs['data']
            row_ids = rs['prov']
            annotation_flags = rs['colTaint']
            self.rows = list()
            for row_index in range(len(rs_rows)):
                row = rs_rows[row_index]
                row_annotation_flags = annotation_flags[row_index]  
                row_id = str(row_ids[row_index])
                values = [None] * len(self.columns)
                annotation_flag_values: List[bool] = [False] * len(self.columns)
                for i in range(len(self.columns)):
                    col = self.columns[i]
                    values[i] = base.mimir_value_to_python(row[i], col)
                    annotation_flag_values[i] = not row_annotation_flags[i]
                self.rows.append(DatasetRow(row_id, values, annotation_flag_values))
            self.read_index = 0
            self.is_open = True
        return self

    
