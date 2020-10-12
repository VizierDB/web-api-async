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

"""Default implementation of the vizual API. Uses the file system based
filestore and datastore to persist files and datasets.
"""
from typing import cast, Optional, List, Tuple, Dict, Any

from vizier.core.util import is_valid_name, get_unique_identifier
from vizier.datastore.dataset import DatasetColumn, DatasetRow, DatasetDescriptor
from vizier.engine.packages.vizual.api.base import VizualApi, VizualApiResult
from vizier.datastore.base import Datastore
from vizier.filestore.base import Filestore
from vizier.datastore.fs.base import FileSystemDatastore, FileSystemDatasetHandle
from vizier.filestore.fs.base import FileSystemFilestore

import vizier.engine.packages.vizual.api.base as base


class DefaultVizualApi(VizualApi):
    """Default implementation of the vizual API. Manipulates datasets in memory.
    Expects an instance of the vizier.datastore.fs.base.FileSystemDatastore to
    persist datasets.
    """
    def delete_column(self, 
        identifier: str, 
        column_id: int, 
        datastore: Datastore
    ) -> VizualApiResult:
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
        col_index = dataset.get_index(column_id)
        if col_index is None:
            raise ValueError('unknown column identifier \'' + str(column_id) + '\'')
        # Delete column from schema. Keep track of the column name for the
        # result output.
        columns = list(dataset.columns)
        del columns[col_index]
        # Delete all value for the deleted column
        rows = dataset.fetch_rows()
        for row in rows:
            del row.values[col_index]
        # Store updated dataset to get new identifier
        ds = datastore.create_dataset(
            columns=columns,
            rows=rows,
            properties={}
        )
        return VizualApiResult(ds)

    def delete_row(self, 
        identifier: str, 
        row_index: str, 
        datastore: Datastore
    ) -> VizualApiResult:
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
        if int(row_index) < 0 or int(row_index) >= dataset.row_count:
            raise ValueError('invalid row index \'' + str(row_index) + '\'')
        # Delete the row at the given index position
        rows = dataset.fetch_rows()
        del rows[int(row_index)]
        # Store updated dataset to get new identifier
        ds = datastore.create_dataset(
            columns=dataset.columns,
            rows=rows,
            properties={}
        )
        return VizualApiResult(ds)

    def filter_columns(self, 
        identifier: str, 
        columns: List[int], 
        names: List[str], 
        datastore: Datastore
    ) -> VizualApiResult:
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
        # list. Keep track of their index positions to filter values.
        schema = list()
        val_filter = list()
        for i in range(len(columns)):
            col_idx = dataset.get_index(columns[i])
            if col_idx is None:
                raise ValueError('unknown column identifier \'' + str(columns[i]) + '\'')
            col = dataset.columns[col_idx]
            if not names[i] is None:
                schema.append(
                    DatasetColumn(
                        identifier=col.identifier,
                        name=names[i],
                        data_type=col.data_type
                    )
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
        ds = datastore.create_dataset(
            columns=schema,
            rows=rows,
            properties={}
        )
        return VizualApiResult(ds)

    def insert_column(self, 
        identifier: str, 
        position: int, 
        name: str, 
        datastore: Datastore
    ) -> VizualApiResult:
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
        name: string
            New column name
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Raise ValueError if given colum name is invalid
        if not name is None and not is_valid_name(name):
            raise ValueError('invalid column name \'' + name + '\'')
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(dataset.columns):
            raise ValueError('invalid column index \'' + str(position) + '\'')
        # Insert new column into dataset
        columns = list(dataset.columns)
        rows = dataset.fetch_rows()
        columns.insert(
            position,
            DatasetColumn(
                identifier=dataset.max_column_id() + 1,
                name=name if not name is None else ''
            )
        )
        # Add a null value to each row for the new column
        for row in rows:
            row.values.insert(position, None)
        # Store updated dataset to get new identifier
        ds = datastore.create_dataset(
            columns=columns,
            rows=rows,
            properties={}
        )
        return VizualApiResult(ds)

    def insert_row(self, 
        identifier: str, 
        position: int, 
        datastore: Datastore
    ) -> VizualApiResult:
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
        print('---------------' + str(dataset.__class__.__name__))
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        assert(isinstance(dataset, FileSystemDatasetHandle))
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > dataset.row_count:
            raise ValueError('invalid row index \'' + str(position) + '\'')
        # Create empty set of values
        rows = dataset.fetch_rows()
        rows.insert(
            position,
            DatasetRow(
                identifier=str(dataset.max_row_id() + 1),
                values=[None] * len(dataset.columns)
            )
        )
        # Store updated dataset to get new identifier
        ds = datastore.create_dataset(
            columns=dataset.columns,
            rows=rows,
            properties={}
        )
        return VizualApiResult(ds)

    def import_dataset(self, 
        datastore: Datastore, 
        project_id: str, 
        dataset_id: str
    ) -> VizualApiResult:
        raise Exception("Import not (yet) supported in the FS backend")


    def load_dataset(self, 
        datastore: Datastore, 
        filestore: Filestore, 
        file_id: Optional[str] = None, 
        url: Optional[str] = None, 
        detect_headers: bool = True,
        infer_types: bool = True, 
        load_format: str = 'csv', 
        options: List[Dict[str,str]] = [], 
        username: str = None,
        password: str = None, 
        resources: Optional[Dict[str, Any]] = None, 
        reload: bool = False, 
        human_readable_name: Optional[str] = None,
        proposed_schema: List[Tuple[str,str]] = []
    ) -> VizualApiResult:
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
        result_resources = dict()
        if url is not None:
            # If the same url has been previously used to generate a dataset
            # we do not need to download the file and re-create the dataset.
            if not reload and not resources is None and base.RESOURCE_URL in resources and base.RESOURCE_DATASET in resources:
                # Check if the previous download matches the given Uri
                if resources[base.RESOURCE_URL] == url:
                    ds_id = resources[base.RESOURCE_DATASET]
                    dataset = datastore.get_dataset(ds_id)
            # If dataset is still None we need to create a new dataset by
            # downloading the given Uri
            if dataset is None:
                assert(isinstance(datastore, FileSystemDatastore))
                dataset = datastore.download_dataset(
                    url=url,
                    username=username,
                    password=password,
                )
            result_resources[base.RESOURCE_URL] = url
        else:
            # either url or file_id must not be None
            assert(file_id is not None)
            
            # If the same file has been previously used to generate a dataset
            # we do not need to re-create it.
            if not resources is None and base.RESOURCE_FILEID in resources and base.RESOURCE_DATASET in resources:
                if resources[base.RESOURCE_FILEID] == file_id:
                    ds_id = resources[base.RESOURCE_DATASET]
                    dataset = datastore.get_dataset(ds_id)
            # If dataset is still None we need to create a new dataset from the
            # specified file
            if dataset is None:
                dataset = datastore.load_dataset(
                    f_handle=filestore.get_file(file_id),
                    proposed_schema=proposed_schema
                )
            result_resources[base.RESOURCE_FILEID] = file_id
        # Ensure that the dataset is not None at this point
        if dataset is None:
            raise ValueError('unknown file or resource')
        result_resources[base.RESOURCE_DATASET] = dataset.identifier
        return VizualApiResult(
            dataset=dataset,
            resources=result_resources
        )

    def unload_dataset(self, 
        dataset: DatasetDescriptor, 
        datastore: Datastore, 
        filestore: Filestore, 
        unload_format='csv', 
        options=[], 
        resources=None
    ):
        """Export (or unload) a dataset to a given file format. 

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
        unload_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command
        resources: dict, optional
            Dictionary of additional resources (i.e., key,value pairs) that were
            generated during a previous execution of the associated module
        
        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        f_handles = None
        result_resources = dict()
        assert(isinstance(filestore, FileSystemFilestore))
        if dataset is not None:
            f_handles = datastore.unload_dataset(
                filepath=filestore.get_file_dir(get_unique_identifier() ) ,
                dataset_name=dataset.identifier,
                format=unload_format,
                options=options
            )
        result_resources[base.RESOURCE_FILEID] = f_handles
        return VizualApiResult(
            dataset=dataset,
            resources=result_resources
        )

    def move_column(self, 
        identifier: str, 
        column_id: int, 
        position: int, 
        datastore: Datastore
    ) -> VizualApiResult:
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
        source_idx = dataset.get_index(column_id)
        if source_idx is None:
            raise ValueError('unknown column identifier \'' + str(column_id) + '\'')
        # No need to do anything if source position equals target position
        if source_idx != position:
            columns = list(dataset.columns)
            columns.insert(position, columns.pop(source_idx))
            rows = dataset.fetch_rows()
            for row in rows:
                row.values.insert(position, row.values.pop(source_idx))
            # Store updated dataset to get new identifier
            ds = datastore.create_dataset(
                columns=columns,
                rows=rows,
                properties={}
            )
            return VizualApiResult(ds)
        else:
            return VizualApiResult(dataset)

    def move_row(self, 
        identifier: str, 
        row_id: str, 
        position: int, 
        datastore: Datastore
    ):
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
        if int(row_id) < 0 or int(row_id) >= dataset.row_count:
            raise ValueError('invalid source row \'' + str(row_id) + '\'')
        # Make sure that position is a valid row index in the new dataset
        if position < 0 or position > dataset.row_count:
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # No need to do anything if source position equals target position
        if row_id != position:
            rows = dataset.fetch_rows()
            rows.insert(position, rows.pop(int(row_id)))
            # Store updated dataset to get new identifier
            ds = datastore.create_dataset(
                columns=dataset.columns,
                rows=rows,
                properties={}
            )
            return VizualApiResult(ds)
        else:
            return VizualApiResult(dataset)

    def rename_column(self, 
        identifier: str, 
        column_id: int, 
        name: str, 
        datastore: Datastore
    ) -> VizualApiResult:
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
            raise ValueError('invalid column name \'' + name + '\'')
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Get the specified column that is to be renamed and set the column name
        # to the new name
        col_idx = dataset.get_index(column_id)
        if col_idx is None:
            raise ValueError('unknown column identifier \'' + str(column_id) + '\'')
        # Nothing needs to be changed if name does not differ from column name
        if dataset.columns[col_idx].name.lower() != name.lower():
            columns = list(dataset.columns)
            col = columns[col_idx]
            columns[col_idx] = DatasetColumn(
                identifier=col.identifier,
                name=name,
                data_type=col.data_type
            )
            # Store updated dataset to get new identifier
            ds = datastore.create_dataset(
                columns=columns,
                rows=dataset.fetch_rows(),
                properties={}
            )
            return VizualApiResult(ds)
        else:
            return VizualApiResult(dataset)

    def sort_dataset(self, 
        identifier: str, 
        columns: List[int], 
        reversed: List[bool], 
        datastore: Datastore
    ) -> VizualApiResult:
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
        # Fetch the full set of rows
        rows = dataset.fetch_rows()
        # Sort multiple times, ones for each of the sort columns (in reverse
        # order of appearance in the order by clause)
        for i in range(len(columns)):
            l_idx = len(columns) - (i + 1)
            col_id = columns[l_idx]
            col_idx = dataset.get_index(col_id)
            # print("SORT: {}".format(col_idx))
            # print("\n".join(", ".join("'{}':{}".format(v, type(v)) for v in row.values) for row in rows))
            reverse = reversed[l_idx]
            if col_idx is None:
                raise ValueError('unknown column identifier \'' + str(col_id) + '\'')
            else:
                rows.sort(key=lambda row: row.values[cast(int, col_idx)], reverse=reverse)
        # Store updated dataset to get new identifier
        ds = datastore.create_dataset(
            columns=dataset.columns,
            rows=rows,
            properties={}
        )
        return VizualApiResult(ds)

    def update_cell(self, 
        identifier: str, 
        column_id: int, 
        row_id: str, 
        value: str, 
        datastore: Datastore
    ) -> VizualApiResult:
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
        # Get column index forst in case it raises an exception
        col_idx = dataset.get_index(column_id)
        if col_idx is None:
            raise ValueError('unknown column identifier \'' + str(column_id) + '\'')
        # Update the specified cell in the given data array
        rows = dataset.fetch_rows()
        row_index = -1
        for i in range(len(rows)):
            if int(rows[i].identifier) == int(row_id):
                row_index = i
                break
        # Make sure that row refers a valid row in the dataset
        if row_index < 0:
            raise ValueError('invalid row identifier \'' + str(row_id) + '\'')
        r = rows[row_index]
        values = list(r.values)
        values[col_idx] = value
        rows[row_index] = DatasetRow(identifier=r.identifier, values=values)
        # Store updated dataset to get new identifier
        ds = datastore.create_dataset(
            columns=dataset.columns,
            rows=rows,
            properties={}
        )
        return VizualApiResult(ds)
    
    def materialize_dataset(self, 
            identifier: str, 
            datastore: Datastore
        ) -> VizualApiResult:
        """Create a materialized snapshot of the dataset for faster
        execution.
    
        This is a no-op for the FS Backend
        """
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        return VizualApiResult(dataset)
        