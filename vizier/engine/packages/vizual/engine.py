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

from vizier.core.util import is_valid_name
from vizier.core.system import build_info, component_descriptor
from vizier.core.system import VizierSystemComponent
from vizier.datastore.base import DatasetColumn, DatasetRow
from vizier.datastore.base import get_index_for_column


class VizualEngine(VizierSystemComponent):
    """Abstract interface to Vizual engine that allows manipulation of datasets
    via VizUAL commands. There may be various implementations of this interface
    for different storage backends.
    """
    def __init__(self, build):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        build : dict()
            Build information
        """
        super(VizualEngine, self).__init__(build)

    def components(self):
        """List containing component descriptor.

        Returns
        -------
        list
        """
        return [component_descriptor('vizual', self.system_build())]

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def delete_row(self, identifier, row):
        """Delete a row in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row: int
            Row index for deleted row

        Returns
        -------
        int, string
            Number of deleted rows (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def insert_column(self, identifier, position, name=None):
        """Insert column with given name at given position in dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column position is outside of the current schema bounds, or if
        the column name is invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        position: int
            Index position at which the column will be inserted
        name: string, optional
            New column name

        Returns
        -------
        int, string
            Number of inserted columns (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def insert_row(self, identifier, position):
        """Insert row at given position in a dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row psotion isoutside the dataset bounds.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        position: int
            Index position at which the row will be inserted

        Returns
        -------
        int, string
            Number of inserted rows (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def load_dataset(self, dataset_name, file_id):
        """Create (or load) a new dataset from a given Uri. The format of the
        Uri and the method to resolve the Uri and retireve the data are all
        implementation dependent.

        Reaise ValueError if (1) the Uri is invalid, or (2) the Uri references
        a non-existing resource.

        Parameters
        ----------
        file_id: string
            Identifier of the file on the file server from which the dataset is
            being created

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def rename_column(self, identifier, column, name):
        """Rename column in a given dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column is unknown, or if the given column name is invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column: int
            Unique column identifier
        name: string
            New column name

        Returns
        -------
        int, string
            Number of renamed columns (i.e., 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError

    @abstractmethod
    def sort_dataset(self, identifier, columns, reversed):
        """Sort the dataset with the given identifier according to the order by
        statement. The order by statement is a pair of lists. The first list
        contains the identifier of columns to sort on. The second list contains
        boolean flags, one for each entry in columns, indicating whether sort
        order is revered for the corresponding column or not.

        Returns the number of rows in the dataset and the identifier of the
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
        raise NotImplementedError

    @abstractmethod
    def update_cell(self, identifier, column, row, value):
        """Update a cell in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified cell is outside of the current dataset ranges.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column: int
            Unique column identifier for updated cell
        row: int
            Row index for updated cell (starting at 0)
        value: string
            New cell value

        Returns
        -------
        int, string
            Number of updated rows (i.e., 0 or 1) and identifier of resulting
            dataset
        """
        raise NotImplementedError


class DefaultVizualEngine(VizualEngine):
    """Default implementation for VizUAL DB Engine. Manipulates datasets in
    memory and uses a datastore object to store changes.
    """
    def __init__(self, datastore, fileserver, build=None):
        """Initialize the datastore that is used to retrieve and update
        datasets and the file server managing CSV files.

        Parameters
        ----------
        datastore : vizier.datastore.base.Datastore
            Datastore to retireve and update datasets.
        fileserver:  vizier.filestore.base.FileSever
            File server to access uploaded  CSV files
        """
        if build is None:
            build = build_info('DefaultVizualEngine')
        super(DefaultVizualEngine, self).__init__(build)
        self.datastore = datastore
        self.fileserver = fileserver

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
        columns = list(dataset.columns)
        del columns[col_index]
        # Delete all value for the deleted column
        rows = dataset.fetch_rows()
        for row in rows:
            del row.values[col_index]
        # Store updated dataset to get new identifier
        ds = self.datastore.create_dataset(
            columns=columns,
            rows=rows,
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
        rows = dataset.fetch_rows()
        if row < 0 or row >= len(rows):
            raise ValueError('invalid row index \'' + str(row) + '\'')
        # Delete the row at the given position
        del rows[row]
        # Store updated dataset to get new identifier
        ds = self.datastore.create_dataset(
            columns=dataset.columns,
            rows=rows,
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
        # list. Keep track of their index positions to filter values.
        schema = list()
        val_filter = list()
        for i in range(len(columns)):
            col_idx = get_index_for_column(dataset, columns[i])
            col = dataset.columns[col_idx]
            if not names[i] is None:
                schema.append(
                    DatasetColumn(identifier=col.identifier, name=names[i])
                )
            else:
                schema.append(col)
            val_filter.append(col_idx)
        # Create a list of projected rows
        rows = list()
        for row in dataset.fetch_rows():
            values = list()
            for v_idx in val_filter:
                values.append(row.values[v_idx])
            rows.append(DatasetRow(identifier=row.identifier, values=values))
        # Store updated dataset to get new identifier
        ds = self.datastore.create_dataset(
            columns=schema,
            rows=rows,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations.filter_columns(columns)
        )
        return len(rows), ds.identifier

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
        # Insert new column into dataset
        columns = list(dataset.columns)
        rows = dataset.fetch_rows()
        columns.insert(position, DatasetColumn(dataset.column_counter, name))
         # Add a null value to each row for the new column
        for row in rows:
            row.values.insert(position, None)
        # Store updated dataset to get new identifier
        ds = self.datastore.create_dataset(
            columns=columns,
            rows=rows,
            column_counter=dataset.column_counter + 1,
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
        rows = dataset.fetch_rows()
        if position < 0 or position > len(rows):
            raise ValueError('invalid row index \'' + str(position) + '\'')
        # Create empty set of values
        row = DatasetRow(dataset.row_counter, [None] * len(dataset.columns))
        rows.insert(position, row)
        # Store updated dataset to get new identifier
        ds = self.datastore.create_dataset(
            columns=dataset.columns,
            rows=rows,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter + 1,
            annotations=dataset.annotations
        )
        return 1, ds.identifier

    def load_dataset(self, file_id):
        """Create (or load) a new dataset from a given Uri. The format of the
        Uri and the method to resolve the Uri and retireve the data are all
        implementation dependent.

        Reaise ValueError if (1) the Uri is invalid, or (2) the Uri references
        a non-existing resource.

        Parameters
        ----------
        file_id: string
            Identifier of the file on the file server from which the dataset is
            being created

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        # Ensure that file name references a previously uploaded file.
        f_handle = self.fileserver.get_file(file_id)
        if f_handle is None:
            raise ValueError('unknown file \'' + file_id + '\'')
        # Create dataset and return handle
        return self.datastore.load_dataset(f_handle)

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
            columns = list(dataset.columns)
            columns.insert(position, columns.pop(source_idx))
            rows = dataset.fetch_rows()
            for row in rows:
                row.values.insert(position, row.values.pop(source_idx))
            # Store updated dataset to get new identifier
            ds = self.datastore.create_dataset(
                columns=columns,
                rows=rows,
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
        rows = dataset.fetch_rows()
        if row < 0 or row >= len(rows):
            raise ValueError('invalid source row \'' + str(row) + '\'')
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > len(rows):
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # No need to do anything if source position equals target position
        if row != position:
            rows.insert(position, rows.pop(row))
            # Store updated dataset to get new identifier
            ds = self.datastore.create_dataset(
                columns=dataset.columns,
                rows=rows,
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
        identifier: string
            Unique dataset identifier
        column: int
            Unique column identifier
        name: string
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
        col_idx = get_index_for_column(dataset, column)
        # Nothing needs to be changed if name does not differ from column name
        if dataset.columns[col_idx].name.lower() != name.lower():
            columns = list(dataset.columns)
            columns[col_idx] = DatasetColumn(
                columns[col_idx].identifier,
                name
            )
            # Store updated dataset to get new identifier
            ds = self.datastore.create_dataset(
                columns=columns,
                rows=dataset.fetch_rows(),
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
        # Fetch the full set of rows
        rows = dataset.fetch_rows()
        # Sort multiple times, ones for each of the sort columns (in reverse
        # order of appearance in the order by clause)
        for i in range(len(columns)):
            l_idx = len(columns) - (i + 1)
            col_id = columns[l_idx]
            col_idx = get_index_for_column(dataset, col_id)
            reverse = reversed[l_idx]
            rows.sort(key=lambda row: row.values[col_idx], reverse=reverse)
        # Store updated dataset to get new identifier
        ds = self.datastore.create_dataset(
            columns=dataset.columns,
            rows=rows,
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
            Unique column identifier
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
        # Get column index forst in case it raises an exception
        col_idx = get_index_for_column(dataset, column)
        # Make sure that row refers a valid row in the dataset
        rows = dataset.fetch_rows()
        if row < 0 or row >= len(rows):
            raise ValueError('invalid cell [' + str(column) + ', ' + str(row) + ']')
        # Update the specified cell in the given data array
        r = rows[row]
        values = list(r.values)
        values[col_idx] = value
        rows[row] = DatasetRow(r.identifier, values)
        # Store updated dataset to get new identifier
        ds = self.datastore.create_dataset(
            columns=dataset.columns,
            rows=rows,
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return 1, ds.identifier
