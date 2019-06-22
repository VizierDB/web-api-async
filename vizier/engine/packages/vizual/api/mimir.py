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

"""Implementation of the vizual API for configurations that use Mimir as the
storage backend.
"""

import json

from vizier.core.util import is_valid_name
from vizier.datastore.base import get_index_for_column
from vizier.datastore.mimir.base import ROW_ID
from vizier.datastore.mimir.dataset import MimirDatasetColumn
from vizier.engine.packages.vizual.api.base import VizualApi, VizualApiResult

import vizier.engine.packages.vizual.api.base as base
import vizier.mimir as mimir


class MimirVizualApi(VizualApi):
    """Implementation for VizUAL DB Engine using Mimir. Translates most VizUAL
    commands into SQL queries.
    """
    def delete_column(self, identifier, column_id, datastore):
        """Delete a column in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column_id: int
            Unique column identifier
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Get the index of the specified column that is to be deleted.
        col_index = get_index_for_column(dataset, column_id)
        # Delete column from schema
        schema = list(dataset.columns)
        del schema[col_index]
        # Create a view for the modified schema
        col_list = [ROW_ID]
        for col in schema:
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name + ';'
        view_name = mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = datastore.register_dataset(
            table_name=view_name,
            columns=schema,
            row_idxs=dataset.row_idxs,
            row_ids=dataset.row_ids,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

    def delete_row(self, identifier, row_index, datastore):
        """Delete a row in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row_index: int
            Row index for deleted row
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that row refers a valid row in the dataset
        if row_index < 0 or row_index >= len(dataset.row_ids):
            raise ValueError('invalid row index \'' + str(row_index) + '\'')
        # Get the id of the row that is being deleted and modify the row id list
        # for the resulting dataset
        rows = list(dataset.row_idxs)
        rows_ids = list(dataset.row_ids)
        row_id = rows[row_index]
        del rows[row_index]
        del rows_ids[row_index]
        # Create a view for the modified dataset
        col_list = [ROW_ID]
        for col in dataset.columns:
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        sql += ' WHERE ' + ROW_ID + ' <> ' + dataset.rowid_column.to_sql_value(row_id) + ';'
        view_name = mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = datastore.register_dataset(
            table_name=view_name,
            columns=dataset.columns,
            row_idxs=rows,
            row_ids=rows_ids,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

    def filter_columns(self, identifier, columns, names, datastore):
        """Dataset projection operator. Returns a copy of the dataset with the
        given identifier that contains only those columns listed in columns.
        The list of names contains optional new names for the filtered columns.
        A value of None in names indicates that the name of the corresponding
        column is not changed.

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
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
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
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name + ';'
        view_name = mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = datastore.register_dataset(
            table_name=view_name,
            columns=schema,
            row_idxs=dataset.row_idxs,
            row_ids=dataset.row_ids,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations.filter(
                columns=columns,
                rows=dataset.row_ids
            )
        )
        return VizualApiResult(ds)

    def insert_column(self, identifier, position, name, datastore):
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
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Raise ValueError if given colum name is invalid
        if not is_valid_name(name):
            raise ValueError('invalid column name \'' + str(name) + '\'')
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(dataset.columns):
            raise ValueError('invalid column index \'' + str(position) + '\'')
        # Get identifier for new column
        col_id = dataset.max_column_id() + 1
        # Insert new column into schema
        schema = list(dataset.columns)
        new_column = MimirDatasetColumn(col_id, name, name)
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
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name + ';'
        view_name = mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = datastore.register_dataset(
            table_name=view_name,
            columns=schema,
            row_idxs=dataset.row_idxs,
            row_ids=dataset.row_ids,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

    def insert_row(self, identifier, position, datastore):
        """Insert row at given position in a dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row psotion isoutside the dataset bounds.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        position: int
            Index position at which the row will be inserted
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > len(dataset.row_ids):
            raise ValueError('invalid row index \'' + str(position) + '\'')
        # Get unique id for new row
        row_idx = dataset.row_counter
        dataset.row_counter += 1
        row_ids = list(dataset.row_ids)
        row_ids.insert(position, str(row_idx))
        row_idxs = list(dataset.row_idxs)
        row_idxs.append(row_idx)
        # Create a view for the modified schema
        col_list = [ROW_ID]
        for col in dataset.columns:
            col_list.append(col.name_in_rdb)
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name
        mimirSchema = mimir.getSchema(sql)
        union_list = [dataset.rowid_column.to_sql_value(row_id) + ' AS ' + ROW_ID]
        for col in mimirSchema[1:]:
            union_list.append('CAST(NULL AS '+col['baseType']+') AS ' + col['name'])
        sql = '(' + sql + ') UNION ALL (SELECT ' + ','.join(union_list) + ');'
        view_name = mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = datastore.register_dataset(
            table_name=view_name,
            columns=dataset.columns,
            row_idxs=row_idxs,
            row_ids=row_ids,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

    def load_dataset(
        self, datastore, filestore, file_id=None, url=None, detect_headers=True,
        infer_types=True, load_format='csv', options=[], username=None,
        password=None, resources=None, reload=False, human_readable_name = None
    ):
        """Create (or load) a new dataset from a given file or Uri. It is
        guaranteed that either the file identifier or the url are not None but
        one of them will be None. The user name and password may only be given
        if an url is given.

        The resources refer to any resoures (e.g., file identifier) that have
        been generated by a previous execution of the respective task. This
        allows to associate an identifier with a downloaded file to avoid future
        downloads (unless the reload flag is True).

        Parameters
        ----------
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets
        filestore: vizier.filestore.Filestore
            Filestore to retrieve uploaded datasets
        file_id: string, optional
            Identifier for a file in an associated filestore
        url: string, optional
            Identifier for a web resource
        detect_headers: bool, optional
            Detect column names in loaded file if True
        infer_types: bool, optional
            Infer column types for loaded dataset if True
        load_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command
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
        dataset = None
        f_handle = None
        result_resources = dict()
        if not url is None:
            # If the same url has been previously used to generate a dataset
            # we do not need to download the file and re-create the dataset.
            if not reload and not resources is None and base.RESOURCE_URL in resources and base.RESOURCE_DATASET in resources:
                # Check if the previous download matches the given Uri
                if resources[base.RESOURCE_URL] == url:
                    ds_id = resources[base.RESOURCE_DATASET]
                    dataset = datastore.get_dataset(ds_id)
            result_resources[base.RESOURCE_URL] = url
        elif not file_id is None:
            # If the same file has been previously used to generate a dataset
            # we do not need to re-create it.
            if not resources is None and base.RESOURCE_FILEID in resources and base.RESOURCE_DATASET in resources:
                if resources[base.RESOURCE_FILEID] == file_id:
                    ds_id = resources[base.RESOURCE_DATASET]
                    dataset = datastore.get_dataset(ds_id)
            # If the dataset is None we will load the dataset from an uploaded
            # file. Need to get the file handle for the file here.
            if dataset is None:
                f_handle = filestore.get_file(file_id)
            result_resources[base.RESOURCE_FILEID] = file_id
        else:
            raise ValueError('no source identifier given for load')
        # If the dataset is still None at this point we need to call the
        # load_dataset method of the datastore to load it.
        if dataset is None:
            dataset = datastore.load_dataset(
                f_handle=f_handle,
                url=url,
                detect_headers=detect_headers,
                infer_types=infer_types,
                load_format=load_format,
                human_readable_name = human_readable_name,
                options=options
            )
        result_resources[base.RESOURCE_DATASET] = dataset.identifier
        return VizualApiResult(
            dataset=dataset,
            resources=result_resources
        )

        # Ensure that file name references a previously uploaded file.
        f_handle = self.fileserver.get_file(file_id)
        if f_handle is None:
            raise ValueError('unknown file \'' + file_id + '\'')
        # Create dataset and return handle

    def move_column(self, identifier, column_id, position, datastore):
        """Move a column within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown or the target position invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column_id: int
            Unique column identifier
        position: int
            Target position for the column
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(dataset.columns):
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # Get index position of column that is being moved
        source_idx = get_index_for_column(dataset, column_id)
        # No need to do anything if source position equals target position
        if source_idx != position:
            # There are no changes to the underlying database. We only need to
            # change the column information in the dataset schema.
            schema = list(dataset.columns)
            schema.insert(position, schema.pop(source_idx))
            # Store updated dataset to get new identifier
            ds = datastore.register_dataset(
                table_name=dataset.table_name,
                columns=schema,
                row_idxs=dataset.row_idxs,
                row_ids=dataset.row_ids,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations
            )
            return VizualApiResult(ds)
        else:
            return VizualApiResult(dataset)

    def move_row(self, identifier, row_index, position, datastore):
        """Move a row within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row or position is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row_index: int
            Row index for deleted row
        position: int
            Target position for the row
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that row is within dataset bounds
        if row_index < 0 or row_index >= dataset.row_count:
            raise ValueError('invalid source row \'' + str(row_index) + '\'')
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > dataset.row_count:
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # No need to do anything if source position equals target position
        if row_index != position:
            dataset.row_ids.insert(position, dataset.row_ids.pop(row_index))
            # Store updated dataset to get new identifier
            ds = datastore.register_dataset(
                table_name=dataset.table_name,
                columns=dataset.columns,
                row_idxs=dataset.row_idxs,
                row_ids=dataset.row_ids,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations
            )
            return VizualApiResult(ds)
        else:
            return VizualApiResult(dataset)

    def rename_column(self, identifier, column_id, name, datastore):
        """Rename column in a given dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column is unknown, or if the given column name is invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column_id: int
            Unique column identifier
        name: string
            New column name
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Raise ValueError if given colum name is invalid
        if not is_valid_name(name):
            raise ValueError('invalid column name \'' + str(name) + '\'')
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Get the specified column that is to be renamed and set the column name
        # to the new name
        schema = list(dataset.columns)
        col = schema[get_index_for_column(dataset, column_id)]
        # No need to do anything if the name hasn't changed
        if col.name.lower() != name.lower():
            # There are no changes to the underlying database. We only need to
            # change the column information in the dataset schema.
            col.name = name
            # Store updated dataset to get new identifier
            ds = datastore.register_dataset(
                table_name=dataset.table_name,
                columns=schema,
                row_idxs=dataset.row_idxs,
                row_ids=dataset.row_ids,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations
            )
            return VizualApiResult(ds)
        else:
            return VizualApiResult(dataset)

    def sort_dataset(self, identifier, columns, reversed, datastore):
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
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
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
        sql = 'SELECT * FROM {{input}} ORDER BY '
        sql += ','.join(order_by_clause) + ';'
        view_name = mimir.createView(dataset.table_name, sql)
        # Query the row ids in the database sorted by the given order by clause
        sql = 'SELECT 1 AS NOP FROM ' + view_name + ';'
        rs = mimir.vistrailsQueryMimirJson(sql, True, False)
        # The result contains the sorted list of row ids
        rows = rs['prov']
        rows_idxs = range(len(rows))
        # Register new dataset with only a modified list of row identifier
        ds = datastore.register_dataset(
            table_name=view_name,
            columns=dataset.columns,
            row_idxs=row_idxs,
            row_ids=rows,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

    def update_cell(self, identifier, column_id, row_id, value, datastore):
        """Update a cell in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified cell is outside of the current dataset ranges.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column_id: int
            Unique column identifier for updated cell
        row_id: int
            Unique row identifier
        value: string
            New cell value
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Get the index of the specified cell column
        col_index = get_index_for_column(dataset, column_id)
        # Raise exception if row id is not valid
        if not row_id in dataset.row_ids:
            print dataset.row_ids
            raise ValueError('invalid row id \'' + str(row_id) + '\'')
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
        sql = 'SELECT ' + ','.join(col_list) + ' FROM ' + dataset.table_name + ';'
        view_name = mimir.createView(dataset.table_name, sql)
        # Store updated dataset information with new identifier
        ds = datastore.register_dataset(
            table_name=view_name,
            columns=dataset.columns,
            row_idxs=dataset.row_idxs,
            row_ids=dataset.row_ids,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)
