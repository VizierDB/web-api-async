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
from typing import cast, List, Dict, Any, Optional, Tuple

from vizier.core.util import is_valid_name, get_unique_identifier
from vizier.datastore.base import get_index_for_column, Datastore
from vizier.datastore.dataset import DatasetDescriptor, DatasetRow
from vizier.datastore.mimir.dataset import MimirDatasetColumn, MimirDatasetHandle
from vizier.datastore.mimir.store import MimirDatastore
from vizier.engine.packages.vizual.api.base import VizualApi, VizualApiResult
from vizier.filestore.base import Filestore
from vizier.filestore.fs.base import FileSystemFilestore

import vizier.engine.packages.vizual.api.base as base
import vizier.mimir as mimir
from vizier import debug_is_on


class MimirVizualApi(VizualApi):
    """Implementation for VizUAL DB Engine using Mimir. Translates most VizUAL
    commands into SQL queries.
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
        # Confirm that the column actually exists and convert the column_identifier to
        # a position in the schema (usually ==, but not guaranteed)
        col_index = get_index_for_column(dataset, column_id)
        command = {
            "id" : "deleteColumn",
            "column" : col_index
        }
        response = mimir.vizualScript(dataset.identifier, command)
        return VizualApiResult.from_mimir(response, identifier)

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
        assert(isinstance(dataset, MimirDatasetHandle))
        
        # Create a view for the modified dataset
        col_list = []
        for col in dataset.columns:
            assert(isinstance(col, MimirDatasetColumn))
            col_list.append(col.name_in_rdb)
        command = {
            "id" : "deleteRow",
            "row" : int(row_index)
        }
        response = mimir.vizualScript(dataset.identifier, command)
        return VizualApiResult.from_mimir(response)

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
        # list. A column might need to be renamed.
        schema = list()
        column_mapping = list()
        col_list = []
        for i in range(len(columns)):
            col_idx = get_index_for_column(dataset, columns[i])
            col = dataset.columns[col_idx]
            if not names[i] is None:
                if not is_valid_name(names[i]):
                    raise ValueError('invalid column name \'' + str(names[i]) + '\'')
                schema.append(
                    MimirDatasetColumn(
                        identifier=col.identifier,
                        name_in_dataset=names[i],
                        name_in_rdb=names[i]
                    )
                )
            else:
                schema.append(col)
            column_mapping.append({
                "columns_column" : col_idx,
                "columns_name" : schema[-1].name
            })
            col_list.append(col.name_in_rdb)
        command = {
            "id" : "projection",
            "columns" : column_mapping
        }
        response = mimir.vizualScript(dataset.identifier, command)
        return VizualApiResult.from_mimir(response)

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
        command = {
            "id" : "insertColumn",
            "name" : name,
            "position" : position
        }
        response = mimir.vizualScript(dataset.identifier, command)
        return VizualApiResult.from_mimir(response)

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
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')

        command = {
            "id" : "insertRow",
            "position" : position
        }
        response = mimir.vizualScript(dataset.identifier, command)
        return VizualApiResult.from_mimir(response)

    def import_dataset(self, 
        datastore: Datastore, 
        project_id: str, 
        dataset_id: str
    ) -> VizualApiResult:
        from vizier.api.webservice.server import api
        # Mimir doesn't actually need to use the project ID (yet), but let's check the
        # URL for safety anyway
        project = api.projects.projects.get_project(project_id)
        if project is None: 
            raise Exception("No Such Project: {}".format(project_id))
        # Get the actual dataset
        dataset = datastore.get_dataset(dataset_id)
        if dataset is None:
            raise Exception("No Such Dataset: {}".format(dataset_id))
        return VizualApiResult(dataset, {})


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

        ^--- Vistrails will automatically skip re-execution, so the only reason
        that we'd re-execute the cell is if the user manually asked us to.  If 
        that's the case, we should actually reload the file (e.g., because we
        may be reloading with different parameters).


        Parameters
        ----------
        datastore : Datastore to retireve and update datasets
        filestore: Filestore to retrieve uploaded datasets
        file_id: Identifier for a file in an associated filestore
        url: Identifier for a web resource
        detect_headers: Detect column names in loaded file if True
        infer_types: Infer column types for loaded dataset if True
        load_format: Format identifier
        options: Additional options for Mimirs load command
        username: User name for authentication when accessing restricted resources
        password: Password for authentication when accessing restricted resources
        resources: Dictionary of additional resources (i.e., key,value pairs) that were
            generated during a previous execution of the associated module
        reload: If set to false, avoid reloading the data if possible.
        human_readable_name: A user-facing name for this table
        proposed_schema: A list of name/type pairs that will override 
                         the inferred column names/types if present

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        dataset = None
        f_handle = None
        result_resources = dict()
        if url is not None:
            if(debug_is_on()):
                print("LOAD URL: {}".format(url))
            # If the same url has been previously used to generate a dataset
            # we do not need to download the file and re-create the dataset.
            if not reload and not resources is None and base.RESOURCE_URL in resources and base.RESOURCE_DATASET in resources:
                # Check if the previous download matches the given Uri
                if resources[base.RESOURCE_URL] == url:
                    ds_id = resources[base.RESOURCE_DATASET]
                    if(debug_is_on()):
                        print("   ... re-using existing dataset {}".format(ds_id))
                    dataset = datastore.get_dataset(ds_id)
            result_resources[base.RESOURCE_URL] = url
        elif file_id is not None:
            if debug_is_on():
                print("LOAD FILE: {}".format(file_id))
            # If the same file has been previously used to generate a dataset
            # we do not need to re-create it.
            if (not reload) and (resources is not None) and (base.RESOURCE_FILEID in resources) and (base.RESOURCE_DATASET in resources):
                if resources[base.RESOURCE_FILEID] == file_id:
                    ds_id = resources[base.RESOURCE_DATASET]
                    # if(debug_is_on()):
                    print("   ... re-using existing dataset {}".format(ds_id))
                    dataset = datastore.get_dataset(ds_id)
                    print("DATASET: {}".format(dataset))
            # If the dataset is None we will load the dataset from an uploaded
            # file. Need to get the file handle for the file here.
            if dataset is None:
                print("getting file")
                f_handle = filestore.get_file(file_id)
                if(f_handle is None): 
                    raise ValueError("The uploaded file got deleted, try re-uploading.")
            result_resources[base.RESOURCE_FILEID] = file_id
        else:
            raise ValueError('no source identifier given for load')
        
        # If the dataset is still None at this point we need to call the
        # load_dataset method of the datastore to load it.
        if dataset is None:
            if(url is None and f_handle is None):
                raise ValueError("Need an URL or an Uploaded File to load")
            assert(isinstance(datastore, MimirDatastore))
            if(debug_is_on()):
                print("   ... loading dataset {} / {}".format(url, f_handle))
            dataset = datastore.load_dataset(
                f_handle=f_handle,
                url=url,
                detect_headers=detect_headers,
                infer_types=infer_types,
                load_format=load_format,
                human_readable_name = human_readable_name,
                options=options,
                proposed_schema = proposed_schema
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

    def empty_dataset(self,
        datastore: Datastore, 
        filestore: Filestore, 
        initial_columns: List[Tuple[str,str]] = [ ("''", "unnamed_column") ]
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

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        assert(isinstance(datastore, MimirDatastore))
        ds = datastore.create_dataset(
            columns = [
                MimirDatasetColumn(
                    identifier = id,
                    name_in_dataset = col, 
                    data_type = "varchar"
                )
                for id, (default, col) in enumerate(initial_columns)
            ],
            rows = [
                DatasetRow(
                    identifier = str(id),
                    values = [ 
                        default
                        for default, col
                        in initial_columns
                    ]
                  )
                for id in range(1,2)
            ],
            human_readable_name = "Empty Table",
        )

        return VizualApiResult(
            dataset = ds
        )

    def unload_dataset(
        self, 
        dataset: DatasetDescriptor, 
        datastore: Datastore, 
        filestore: Filestore, 
        unload_format: str = 'csv', 
        options: List[Dict[str, Any]] = [], 
        resources: Dict[str, Any] = None
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

        assert(isinstance(datastore, MimirDatastore))
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
        source_idx = get_index_for_column(dataset, column_id)
        # No need to do anything if source position equals target position
        if source_idx != position:
            # Keep the mimir-side schema aligned with the vizier-side schema
            command = {
                "id" : "moveColumn",
                "column" : source_idx,
                "position" : position
            }
            response = mimir.vizualScript(dataset.identifier, command)
            return VizualApiResult.from_mimir(response)
        else:
            return VizualApiResult(dataset)

    def move_row(self, 
        identifier: str, 
        row_id: str, 
        position: int, 
        datastore: Datastore
    ) -> VizualApiResult:
        """Move a row within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row or position is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row_id: int
            Global row identifier for deleted row
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

        command = {
            "id" : "moveRow",
            "row" : row_id,
            "position" : position
        }
        response = mimir.vizualScript(dataset.identifier, command)
        return VizualApiResult.from_mimir(response)

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
            raise ValueError('invalid column name \'' + str(name) + '\'')
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Get the specified column that is to be renamed and set the column name
        # to the new name
        target_col_index = get_index_for_column(dataset, column_id)
        command = {
            "id" : "renameColumn",
            "column" : target_col_index,
            "name" : name
        }
        response = mimir.vizualScript(dataset.identifier, command)
        return VizualApiResult.from_mimir(response)

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
        assert(isinstance(dataset, MimirDatasetHandle))
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        # Create order by clause based on columns and reversed flags
        order_by_clause = list()
        for i in range(len(columns)):
            col_id = columns[i]
            stmt = cast(MimirDatasetColumn, dataset.column_by_id(col_id)).name_in_rdb
            if reversed[i]:
                stmt += ' DESC'
            order_by_clause.append(stmt)
        sql = 'SELECT * FROM '+dataset.identifier+' ORDER BY '
        sql += ','.join(order_by_clause) 
        view_name, dependencies, schema, properties, functionDeps = mimir.createView(
                datasets = { dataset.identifier : dataset.identifier }, 
                query = sql
            )
        ds = MimirDatasetHandle.from_mimir_result(view_name, schema, properties)
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
        # Get the index of the specified cell column
        col_index = get_index_for_column(dataset, column_id)
        # Raise exception if row id is not valid
        
        command = {
            "id" : "updateCell",
            "column" : col_index,
            "row" : row_id,
            "value" : str(value) if value is not None else None
        }
        response = mimir.vizualScript(dataset.identifier, command)
        return VizualApiResult.from_mimir(response)

    def materialize_dataset(self, 
            identifier: str, 
            datastore: Datastore
        ) -> VizualApiResult:
        """Create a materialized snapshot of the dataset for faster
        execution."""
        input_dataset = datastore.get_dataset(identifier)
        if input_dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        cast(MimirDatasetHandle, input_dataset)
        response = mimir.materialize(input_dataset.identifier)
        output_ds = MimirDatasetHandle(
                identifier = response["name"],
                columns = cast(List[MimirDatasetColumn], input_dataset.columns),
                properties = input_dataset.get_properties(),
                name = input_dataset.name if input_dataset.name is not None else "untitled dataset"
            )
        return VizualApiResult(output_ds)

