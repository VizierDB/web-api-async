# Copyright (C) 2018 New York University,
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

"""Default implementation of the vizual API. Uses the file system based
filestore and datastore to persist files and datasets.
"""

from vizier.engine.packages.base import FILE_ID
from vizier.engine.packages.vizual.api.base import VizualApi, VizualApiResult


class DefaultVizualApi(VizualApi):
    """Default implementation of the vizual API. Manipulates datasets in memory.
    Expects an instance of the vizier.datastore.fs.base.FileSystemDatastore to
    persist datasets.
    """
    def __init__(self, datastore, filestore):
        """Initialize the datastore that is used to retrieve and update
        datasets and the filestore that maintains downloaded files.

        Parameters
        ----------
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets
        filestore:  vizier.filestore.base.Filestore
            Filestore for downloaded files
        """
        self.datastore = datastore
        self.filestore = filestore

    def delete_column(self, identifier, column_id):
        """Delete a column in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column_id: int
            Unique column identifier

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            return VizualApiResult(
                stderr=['unknown dataset \'' + identifier + '\'']
            )
        # Get the index of the specified column that is to be deleted.
        col_index = dataset.get_index(column_id)
        if col_index is None:
            return VizualApiResult(
                stderr=['unknown column identifier \'' + str(column_id) + '\'']
            )
        # Delete column from schema. Keep track of the column name for the
        # result output.
        columns = list(dataset.columns)
        name = columns[col_index].name
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
        return VizualApiResult(
            dataset=ds,
            stdout=['Column \'' + name + '\' deleted.']
        )

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

    def load_dataset(
        self, file_id=None, uri=None, username=None, password=None,
        resources=None, reload=False
    ):
        """Create (or load) a new dataset from a given file or Uri. It is
        guaranteed that either the file identifier or the uri are not None but
        one of them will be None. The user name and password may only be given
        if an uri is given.

        If the uri is given it is checked whether a fileId exists in the
        resources. If yes this indicates that the file has been downloaded
        previously. In this case the file will only be downloaded if the reload
        flag is True.

        If an uri is provided without a file id being set in the resource the
        file is loaded to the assocuated file store and the resource dictionary
        in the result will contain the identifier for the local copy of the
        downloaded file.

        Parameters
        ----------
        file_id: string, optional
            Identifier for a file in an associated filestore
        uri: string, optional
            Identifier for a web resource
        username: string, optional
            User name for authentication when accessing restricted resources
        password: string, optional
            Password for authentication when accessing restricted resources
        resources: dict, optional
            Dictionary of additional resources (i.e., key,value pairs) that were
            generated during a previous execution of the associated module
        reload: bool, optional
            Flag to force download of a remote resource even if it was
            downloaded previously

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # If the uri is given we have to decide whether the file needs to be
        # downloaded or not. Otherwise we get the handle from the filestore.
        if not uri is None:
            if not reload and not resources is None and FILE_ID in resources:
                # File was downloaded previously.
                f_handle = self.fileserver.get_file(resources[FILE_ID])
            else:
                f_handle = self.filestore.download_file(
                    uri=uri,
                    username=username,
                    password=password
                )
        else:
            f_handle = self.fileserver.get_file(file_id)
        # Ensure that file identifier references an existing file
        if f_handle is None:
            return VizualApiResult(
                stderr=['unknown file']
            )
        # Create dataset and generate the execution result output.
        dataset = self.datastore.load_dataset(f_handle)
        output = list()
        return VizualApiResult(
            dataset=dataset,
            stdout=output,
            resources={FILE_ID: f_handle.identifier} if not uri is None else None
        )

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
