# Copyright (C) 2017-2020 New York University,
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

"""Implementation of the vizual API. This version uses the HISTORE data store
as the storage backend.
"""

from openclean import vizual

from vizier.core.util import is_valid_name, get_unique_identifier
from vizier.engine.packages.vizual.api.base import VizualApi, VizualApiResult

import vizier.engine.packages.vizual.api.base as base


class OpencleanVizualApi(VizualApi):
    """Implementation of the vizual API that uses the HISTORE data store
    as the storage backend. Manipulates pandas data frames in memory. Expects
    an instance of the vizier.datastore.histore.base.HistoreDatastore to
    persist datasets.
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
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown.
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Delete column from schema.
        df = vizual.delete_columns(dataset.to_dataframe(), colids=[column_id])
        # Store updated dataset to get new identifier.
        colfilter = [c.identifier for c in dataset.columns if c != column_id]
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
            annotations=dataset.annotations.filter(columns=colfilter)
        )
        return VizualApiResult(ds)

    def delete_row(self, identifier, rowid, datastore):
        """Delete a row in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.
        rowid: int
            Identifier for the deleted row.
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets.

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown.
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Delete the row at the given index position
        df = vizual.delete_rows(dataset.to_dataframe(), rowids=[rowid])
        # Store updated dataset to get new identifier.
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
            annotations=dataset.annotations.filter(rows=list(df.index))
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
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown.
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Filter columns by their identifier.
        df = dataset.to_dataframe()
        df = vizual.filter_columns(df=df, colids=columns, names=names)
        # Store updated dataset to get new identifier.
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
            annotations=dataset.annotations.filter(columns=columns)
        )
        return VizualApiResult(ds)

    def insert_column(self, identifier, position, name, datastore):
        """Insert column with given name at given position in dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column position is outside of the current schema bounds, or
        if the column name is invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        position: int
            Index position at which the column will be inserted
        name: string
            New column name
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Raise ValueError if given colum name is invalid.
        if name is not None and not is_valid_name(name):
            raise ValueError("invalid column name '{}'".format(name))
        # Get dataset. Raise exception if dataset is unknown.
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Insert new column into dataset.
        df = dataset.to_dataframe()
        df = vizual.insert_column(df=df, names=[name], pos=position)
        # Store updated dataset to get new identifier.
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
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
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown.
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Insert new row into dataset.
        df = vizual.insert_row(df=dataset.to_dataframe(), pos=position)
        # Store updated dataset to get new identifier.
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

    def load_dataset(
        self, datastore, filestore, file_id=None, url=None,
        detect_headers=True, infer_types=None, load_format='csv', options=[],
        username=None, password=None, resources=None, reload=False,
        human_readable_name=None
    ):
        """Create (or load) a new dataset from a given file or Uri. It is
        guaranteed that either the file identifier or the url are not None but
        one of them will be None. The user name and password may only be given
        if an url is given.

        The resources refer to any resoures (e.g., file identifier) that have
        been generated by a previous execution of the respective task. This
        allows to associate an identifier with a downloaded file to avoid
        future downloads (unless the reload flag is True).

        Parameters
        ----------
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets.
        filestore: vizier.filestore.Filestore
            Filestore to retrieve uploaded datasets.
        file_id: string, optional
            Identifier for a file in an associated filestore.
        url: string, optional
            Identifier for a web resource
        detect_headers: bool, optional
            Detect column names in loaded file if True.
        infer_types: string, optional
            Infer column types for loaded dataset if selected a profiler.
        load_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command.
        username: string, optional
            User name for authentication when accessing restricted resources.
        password: string, optional
            Password for authentication when accessing restricted resources.
        resources: dict, optional
            Dictionary of additional resources (i.e., key,value pairs) that
            were generated during a previous execution of the associated
            module.
        reload: bool, optional
            Flag to force download of a remote resource even if it was
            downloaded previously.

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        dataset = None
        file_handle = None
        result_resources = dict()
        if url is not None:
            # If the same url has been previously used to generate a dataset
            # we do not need to download the file and re-create the dataset.
            if not reload and resources is not None:
                if resources.get(base.RESOURCE_URL) == url:
                    ds_id = resources.get(base.RESOURCE_DATASET)
                    if ds_id:
                        dataset = datastore.get_dataset(ds_id)
            # If dataset is still None we need to create a new dataset by
            # downloading the given Uri
            if dataset is None:
                file_handle = filestore.download_file(
                    self,
                    uri=url,
                    username=username,
                    password=password
                )
            result_resources[base.RESOURCE_URL] = url
        else:
            # If the same file has been previously used to generate a dataset
            # we do not need to re-create it.
            if resources is not None:
                if resources.get(base.RESOURCE_FILEID) == file_id:
                    ds_id = resources.get(base.RESOURCE_DATASET)
                    if ds_id:
                        dataset = datastore.get_dataset(ds_id)
            # If dataset is still None we need to create a new dataset from the
            # specified file
            if dataset is None:
                file_handle = filestore.get_file(file_id)
            result_resources[base.RESOURCE_FILEID] = file_id
        # Ensure that the dataset is not None at this point
        if dataset is None:
            # Create dataset from the file that is represented by the file
            # handle.
            dataset = datastore.load_dataset(
                fh=file_handle,
                profiler=infer_types
            )
        result_resources[base.RESOURCE_DATASET] = dataset.identifier
        return VizualApiResult(
            dataset=dataset,
            resources=result_resources
        )

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
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Move the specified column.
        df = dataset.to_dataframe()
        df = vizual.move_columns(df=df, colids=column_id, pos=position)
        # Store updated dataset to get new identifier.
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

    def move_row(self, identifier, rowid, position, datastore):
        """Move a row within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row or position is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.
        rowid: int
            Identifier for row that is being moved.
        position: int
            Target position for the row.
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets.

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Move the specified row.
        df = dataset.to_dataframe()
        df = vizual.move_rows(df=df, rowids=rowid, pos=position)
        # Store updated dataset to get new identifier.
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

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
        datastore : vizier.datastore.histore.base.HistoreDatastore
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
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Rename the column.
        df = dataset.to_dataframe()
        df = vizual.rename_columns(df=df, colids=[column_id], names=[name])
        # Store updated dataset to get new identifier.
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

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
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Sort the data frame on the specified columns.
        df = dataset.to_dataframe()
        df = vizual.sort_dataset(df=df, colids=columns, reversed=reversed)
        # Store updated dataset to get new identifier.
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)

    def unload_dataset(
        self, dataset, datastore, filestore, unload_format='csv', options=[]
    ):
        """Export (or unload) a dataset to a given file format.

        The resources refer to any resoures (e.g., file identifier) that have
        been generated by a previous execution of the respective task. This
        allows to associate an identifier with a downloaded file to avoid
        future downloads (unless the reload flag is True).

        Parameters
        ----------
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets
        filestore: vizier.filestore.Filestore
            Filestore to retrieve uploaded datasets
        unload_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        f_handles = None
        result_resources = dict()

        if dataset is not None:
            f_handles = datastore.unload_dataset(
                filepath=filestore.get_file_dir(get_unique_identifier()),
                dataset_name=dataset.table_name,
                format=unload_format,
                options=options
            )
        result_resources[base.RESOURCE_FILEID] = f_handles
        return VizualApiResult(
            dataset=dataset,
            resources=result_resources
        )

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
        datastore : vizier.datastore.histore.base.HistoreDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        # Get dataset. Raise exception if dataset is unknown
        dataset = datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError("unknown dataset '{}'".format(identifier))
        # Update the specified cell in the data frame.
        df = vizual.update_cell(
            df=dataset.to_dataframe(),
            colid=column_id,
            rowid=row_id,
            value=value
        )
        # Store updated dataset to get new identifier.
        ds = datastore.update_dataset(
            origin=dataset,
            df=df,
            annotations=dataset.annotations
        )
        return VizualApiResult(ds)
