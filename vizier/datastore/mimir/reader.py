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

import json

from vizier.datastore.dataset import DatasetRow
from vizier.datastore.reader import DatasetReader

import vizier.mimir as mimir
import vizier.datastore.mimir.base as base


class MimirDatasetReader(DatasetReader):
    """Dataset reader for Mimir datasets."""
    def __init__(
        self, table_name, columns, row_idxs, row_ids, rowid_column_numeric=True,
        offset=0, limit=-1, rowid=None
    ):
        """Initialize information about the delimited file and the file format.

        Parameters
        ----------
        table_name: string
            Name of table or view in database that contains the dataset
        columns: vizier.datastore.mimir.MimirDatasetColumn
            List of descriptors for columns in the database
        row_ids: list(int)
            Sort order for rows in the dataset
        rowid_column_numeric: bool, optional
            Flag indicating if the ROW ID column is numeric (necessary when
            generating the WHERE clause for pagination queries).
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components
        """
        self.table_name = table_name
        self.columns = columns
        self.rowid_column_numeric = rowid_column_numeric
        self.offset = offset
        self.limit = limit
        self.rowid = rowid
        # Convert row id list into row position index. Depending on whether
        # offset or limit parameters are given we also limit the entries in the
        # dictionary. The internal flag .is_range_query keeps track of whether
        # offset or limit where given (True) or not (False). This information is
        # later used to generate the query for the database.
        self.row_ids = row_ids
        self.row_idxs = row_idxs
        if offset > 0 or limit > 0:
            self.is_range_query = True
        else:
            self.is_range_query = False
        # Keep an in-memory copy of the dataset rows when open
        self.is_open = False
        self.read_index = None
        self.rows = None
        # Index position of columns in dataset rows
        self.col_map = None

    def close(self):
        """Close any open files and set the is_open flag to False."""
        self.rows = None
        self.read_index = None
        self.col_map = None
        self.is_open = False

    def next(self):
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

    def open(self):
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
            sql = base.get_select_query(self.table_name, columns=self.columns)
            if self.rowid != None:
                sql += ' WHERE ROWID() = ' + str(self.rowid)
            if self.is_range_query:
                if self.limit > 0:
                    sql +=  ' LIMIT ' + str(self.limit)
                if self.offset > 0:
                    sql += ' OFFSET ' + str(self.offset) 
            rs = mimir.vistrailsQueryMimirJson(sql+ ';', True, False)
            #self.row_ids = rs['prov']
            # Initialize mapping of column rdb names to index positions in
            # dataset rows
            self.col_map = dict()
            for i in range(len(rs['schema'])):
                col = rs['schema'][i]
                self.col_map[base.sanitize_column_name(col['name'])] = i
            # Initialize rows (make sure to sort them according to order in
            # row_ids list), read index and open flag
            #rowid_idx = self.col_map[base.ROW_ID]
            # Filter rows if this is a range query (needed until IN works)
            rs_rows = rs['data']
            row_ids = rs['prov']
            annotation_flags = rs['colTaint']
            self.rows = list()
            for row_index in range(len(rs_rows)):
                row = rs_rows[row_index]
                row_annotation_flags = annotation_flags[row_index]  
                row_id = str(row_ids[row_index])
                values = [None] * len(self.columns)
                annotation_flag_values = [None] * len(self.columns)
                for i in range(len(self.columns)):
                    col = self.columns[i]
                    col_index = self.col_map[col.name_in_rdb]
                    values[i] = base.mimir_value_to_python(row[col_index], col)
                    annotation_flag_values[i] = row_annotation_flags[col_index]
                self.rows.append(DatasetRow(base.convertrowid(row_id, row_index), values, annotation_flag_values))
            self.rows.sort(key=lambda row: self.sortbyrowid(row.identifier))
            #self.rows.sort(key=lambda row: self.row_idxs[int(row.identifier)])
            self.read_index = 0
            self.is_open = True
        return self

    def sortbyrowid(self, s):
        try:
            return int(s)
        except ValueError:
            pass
        try:
            return int(s.replace("'", ""))
        except ValueError:
            pass
        try:
            return int(s.split('|')[0])
        except:
            return 0
