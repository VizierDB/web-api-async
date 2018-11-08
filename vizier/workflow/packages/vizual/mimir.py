# Copyright (C) 2018 New York University
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

"""VizUAL API - Defines and interface to a (persistent) data store engine for
the VizUAL data curation language. The engine allows manipulation of datasets
via VizUAL commands.
"""

from abc import abstractmethod
import csv
import gzip
import json

import vistrails.packages.mimir.init as mimir

from vizier.core.system import build_info
from vizier.core.util import is_valid_name
from vizier.datastore.base import get_index_for_column
from vizier.datastore.mimir import MimirDatasetColumn
from vizier.datastore.mimir import COL_PREFIX, ROW_ID
from vizier.workflow.vizual.base import DefaultVizualEngine


class MimirVizualEngine(DefaultVizualEngine):
    """Implementation for VizUAL DB Engine unsing Mimir. Translates most VizUAL
    commands into SQL views.
    """
    def __init__(self, datastore, fileserver):
        """Initialize the Mimir datastore that is used to retrieve and update
        datasets and the file server managing CSV files.

        Parameters
        ----------
        datastore: vizier.datastore.mimir.MimirDataStore
            Datastore to retireve and update datasets.
        fileserver:  vizier.filestore.base.FileSever
            File server to access uploaded  CSV files
        """
        super(MimirVizualEngine, self).__init__(
            datastore,
            fileserver,
            build=build_info('MimirVizualEngine')
        )

    def delete_column(self, identifier, column):
        """Delete a column in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column: int
            Unique column identifier

        Returns
        -------
        int, string
            Number of deleted columns (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Get the index of the specified column that is to be deleted.
        col_index = get_index_for_column(dataset, column)
        # Delete column from schema
        schema = list(dataset.columns)
        del schema[col_index]
        # Create a view for the modified schema
        col_list = [ROW_ID]
        for col in schema:
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = self.datastore.register_dataset(
            table_name=view_name,
            columns=schema,
            row_ids=dataset.row_ids,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return 1, ds.identifier

    def delete_row(self, identifier, row):
        """Delete a row in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row is not within the range of the dataset.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        row : int
            Row index for deleted row

        Returns
        -------
        int, string
            Number of deleted rows (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that row refers a valid row in the dataset
        if row < 0 or row >= len(dataset.row_ids):
            raise ValueError('invalid row index \'' + str(row) + '\'')
        # Get the id of the row that is being deleted and modify the row id list
        # for the resulting dataset
        rows = list(dataset.row_ids)
        row_id = rows[row]
        del rows[row]
        # Create a view for the modified dataset
        col_list = [ROW_ID]
        for col in dataset.columns:
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        sql += ' WHERE ' + ROW_ID + ' <> ' + dataset.rowid_column.to_sql_value(row_id)
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = self.datastore.register_dataset(
            table_name=view_name,
            columns=dataset.columns,
            row_ids=rows,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return 1, ds.identifier

    def filter_columns(self, identifier, columns, names):
        """Dataset projection operator. Returns a copy of the dataset with the
        given identifier that contains only those columns listed in columns.
        The list of names contains optional new names for the filtered columns.
        A value of None in names indicates that the name of the corresponding
        column is not changed.

        Returns the number of rows in the dataset and the identifier of the
        projected dataset.

        Raises ValueError if no dataset with given identifier exists or if any
        of the filter columns are unknown.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(int)
            List of column identifier for columns in the result.
        names: list(string)
            Optional new names for filtered columns.

        Returns
        -------
        int, string
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # The schema of the new dataset only contains the columns in the given
        # list. A column might need to be renamed.
        schema = list()
        col_list = [ROW_ID]
        for i in range(len(columns)):
            col_idx = get_index_for_column(dataset, columns[i])
            col = dataset.columns[col_idx]
            if not names[i] is None:
                schema.append(
                    MimirDatasetColumn(
                        identifier=col.identifier,
                        name_in_dataset=names[i],
                        name_in_rdb=col.name_in_rdb
                    )
                )
            else:
                schema.append(col)
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = self.datastore.register_dataset(
            table_name=view_name,
            columns=schema,
            row_ids=dataset.row_ids,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations.filter_columns(columns)
        )
        return len(dataset.row_ids), ds.identifier

    def insert_column(self, identifier, position, name):
        """Insert column with given name at given position in dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column position is outside of the current schema bounds, or if
        the column name is invalid.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        position : int
            Index position at which the column will be inserted
        name : string
            New column name

        Returns
        -------
        int, string
            Number of inserted columns (i.e., 1) and identifier of resulting
            dataset
        """
        # Raise ValueError if given colum name is invalid
        if not is_valid_name(name):
            raise ValueError('invalid column name \'' + name + '\'')
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(dataset.columns):
            raise ValueError('invalid column index \'' + str(position) + '\'')
        # Get name for new column
        col_id = dataset.column_counter
        dataset.column_counter += 1
        # Insert new column into schema
        schema = list(dataset.columns)
        new_column = MimirDatasetColumn(col_id, name, COL_PREFIX + str(col_id))
        schema.insert(position, new_column)
        # Create a view for the modified schema
        col_list = [ROW_ID]
        for col in schema:
            if col.identifier == new_column.identifier:
                # Note: By no (April 2018) this requires Mimir to run with the
                # XNULL option. Otherwise, in some scenarios setting the all
                # values in the new column to NULL may cause an exception.
                col_list.append('NULL ' + col.name_in_rdb)
            else:
                col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = self.datastore.register_dataset(
            table_name=view_name,
            columns=schema,
            row_ids=dataset.row_ids,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return 1, ds.identifier

    def insert_row(self, identifier, position):
        """Insert row at given position in a dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row psotion isoutside the dataset bounds.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        position : int
            Index position at which the row will be inserted

        Returns
        -------
        int, string
            Number of inserted rows (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > len(dataset.row_ids):
            raise ValueError('invalid row index \'' + str(position) + '\'')
        # Get unique id for new row
        row_id = dataset.row_counter
        dataset.row_counter += 1
        row_ids = list(dataset.row_ids)
        row_ids.insert(position, row_id)
        # Create a view for the modified schema
        col_list = [ROW_ID]
        for col in dataset.columns:
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        union_list = [dataset.rowid_column.to_sql_value(row_id) + ' AS ' + ROW_ID]
        for col in dataset.columns:
            union_list.append('NULL AS ' + col.name_in_rdb)
        sql = '(' + sql + ') UNION ALL (SELECT ' + ','.join(union_list) + ')'
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = self.datastore.register_dataset(
            table_name=view_name,
            columns=dataset.columns,
            row_ids=row_ids,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return 1, ds.identifier

    def move_column(self, identifier, column, position):
        """Move a column within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown or the target position invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column: int
            Unique column identifier
        position: int
            Target position for the column

        Returns
        -------
        int, string
            Number of moved columns (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(dataset.columns):
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # Get index position of column that is being moved
        source_idx = get_index_for_column(dataset, column)
        # No need to do anything if source position equals target position
        if source_idx != position:
            # There are no changes to the underlying database. We only need to
            # change the column information in the dataset schema.
            schema = list(dataset.columns)
            schema.insert(position, schema.pop(source_idx))
            # Store updated dataset to get new identifier
            ds = self.datastore.register_dataset(
                table_name=dataset.table_name,
                columns=schema,
                row_ids=dataset.row_ids,
                column_counter=dataset.column_counter,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations
            )
            return 1, ds.identifier
        else:
            return 0, identifier

    def move_row(self, identifier, row, position):
        """Move a row within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row or position is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row: int
            Row index for deleted row
        position: int
            Target position for the row

        Returns
        -------
        int, string
            Number of moved rows (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that row is within dataset bounds
        if row < 0 or row >= dataset.row_count:
            raise ValueError('invalid source row \'' + str(row) + '\'')
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > dataset.row_count:
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # No need to do anything if source position equals target position
        if row != position:
            dataset.row_ids.insert(position, dataset.row_ids.pop(row))
            # Store updated dataset to get new identifier
            ds = self.datastore.register_dataset(
                table_name=dataset.table_name,
                columns=dataset.columns,
                row_ids=dataset.row_ids,
                column_counter=dataset.column_counter,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations
            )
            return 1, ds.identifier
        else:
            return 0, identifier

    def rename_column(self, identifier, column, name):
        """Rename column in a given dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column is unknown, or if the given column name is invalid.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column : int
            Unique column identifier
        name : string
            New column name

        Returns
        -------
        int, string
            Number of renamed columns (i.e., 1) and identifier of resulting
            dataset
        """
        # Raise ValueError if given colum name is invalid
        if not is_valid_name(name):
            raise ValueError('invalid column name \'' + name + '\'')
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Get the specified column that is to be renamed and set the column name
        # to the new name
        schema = list(dataset.columns)
        col = schema[get_index_for_column(dataset, column)]
        # No need to do anything if the name hasn't changed
        if col.name.lower() != name.lower():
            # There are no changes to the underlying database. We only need to
            # change the column information in the dataset schema.
            col.name = name
            # Store updated dataset to get new identifier
            ds = self.datastore.register_dataset(
                table_name=dataset.table_name,
                columns=schema,
                row_ids=dataset.row_ids,
                column_counter=dataset.column_counter,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations
            )
            return 1, ds.identifier
        else:
            return 0, identifier

    def sort_dataset(self, identifier, columns, reversed):
        """Sort the dataset with the given identifier according to the order by
        statement. The order by statement is a pair of lists. The first list
        contains the identifier of columns to sort on. The second list contains
        boolean flags, one for each entry in columns, indicating whether sort
        order is revered for the corresponding column or not.

        Returns the number of rows in the database and the identifier of the
        sorted dataset.

        Raises ValueError if no dataset with given identifier exists or if any
        of the columns in the order by clause are unknown.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(int)
            List of column identifier for sort columns.
        reversed: list(bool)
            Flags indicating whether the sort order of the corresponding column
            is reveresed.

        Returns
        -------
        int, string
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Create order by clause based on columns and reversed flags
        order_by_clause = list()
        for i in range(len(columns)):
            col_id = columns[i]
            stmt = dataset.column_by_id(col_id).name_in_rdb
            if reversed[i]:
                stmt += ' DESC'
            order_by_clause.append(stmt)
        # Query the row ids in the database sorted by the given order by clause
        sql = 'SELECT ' + ROW_ID + ' FROM ' + dataset.table_name + ' ORDER BY '
        sql += ','.join(order_by_clause)
        rs = json.loads(
            mimir._mimir.vistrailsQueryMimirJson(sql, True, False)
        )
        # The result contains the sorted list of row ids
        rows = list()
        for row in rs['data']:
            rows.append(row[0])
        # Register new dataset with only a modified list of row identifier
        ds = self.datastore.register_dataset(
            table_name=dataset.table_name,
            columns=dataset.columns,
            row_ids=rows,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return len(rows), ds.identifier

    def update_cell(self, identifier, column, row, value):
        """Update a cell in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified cell is outside of the current dataset ranges.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column : int
            Unique column identifier for updated cell (starting at 0)
        row : int
            Row index for updated cell (starting at 0)
        value : string
            New cell value

        Returns
        -------
        int, string
            Number of updated rows (i.e., 1) and identifier of resulting
            dataset
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that row refers a valid row in the dataset
        if row < 0 or row >= dataset.row_count:
            raise ValueError('invalid cell [' + str(column) + ', ' + str(row) + ']')
        # Get the index of the specified cell column
        col_index = get_index_for_column(dataset, column)
        # Get id of the cell row
        row_id = dataset.row_ids[row]
        # Create a view for the modified dataset
        col_list = [ROW_ID]
        for i in range(len(dataset.columns)):
            col = dataset.columns[i]
            if i == col_index:
                try:
                    val_stmt = col.to_sql_value(value)
                    col_sql = val_stmt + ' ELSE ' + col.name_in_rdb + ' END '
                except ValueError:
                    col_sql = '\'' + str(value) + '\' ELSE CAST({{input}}.' + col.name_in_rdb  + ' AS varchar) END '
                rid_sql = dataset.rowid_column.to_sql_value(row_id)
                stmt = 'CASE WHEN ' + ROW_ID + ' = ' + rid_sql + ' THEN '
                stmt += col_sql
                stmt += 'AS ' + col.name_in_rdb
                col_list.append(stmt)
            else:
                col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        view_name = mimir._mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = self.datastore.register_dataset(
            table_name=view_name,
            columns=dataset.columns,
            row_ids=dataset.row_ids,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return 1, ds.identifier
