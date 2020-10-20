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

"""The vizual API defines an interface to execute command in the vizual package
against a (persistent) datastore. The engine allows manipulation of datasets
via VizUAL commands.

Each vizual command will create a new dataset instance on success.
"""

from abc import abstractmethod
from typing import Optional, List, Tuple, Dict, Any

from vizier.datastore.mimir.dataset import MimirDatasetHandle
from vizier.datastore.base import Datastore
from vizier.filestore.base import Filestore
from vizier.datastore.dataset import DatasetDescriptor

"""Labels for resources in a previous download state."""
RESOURCE_DATASET = 'dataset'
RESOURCE_FILEID = 'fileid'
RESOURCE_URL = 'url'


class VizualApiResult(object):
    """Each vizual API method returns a result that contains the descriptor for
    the newly created dataset. The result may also contain a dictionary that
    defines additional resources and other information generated during
    execution which will become part of the module provenance and will be made
    available when a task is re-executed. The resources dictionary should
    represent simple key,value pairs where values are scalars or lists or
    dictionaries.
    """
    def __init__(self, 
            dataset: DatasetDescriptor, 
            resources: Dict[str,Any]=dict()
        ):
        """Initialize the API result components.

        Parameters
        ----------
        dataset: vizier.datastore.dataset.DatasetDescriptor
            Descriptor for the generated dataset (on success)
        resources: dict, optional
            Resources generated during execution as part of the result
            provenance
        """
        self.dataset = dataset
        self.resources = resources

    @staticmethod
    def from_mimir(
            response: Dict[str, Any], 
            name: Optional[str] = None
        ) -> "VizualApiResult":
        ds = MimirDatasetHandle.from_mimir_result(
            table_name = response["name"], 
            schema = response["schema"], 
            properties = response["properties"],
            name = name
        )
        return VizualApiResult(ds)



class VizualApi(object):
    """Abstract interface to Vizual engine that allows manipulation of datasets
    via VizUAL commands. There may be various implementations of this interface
    for different storage backends.
    """
    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def insert_column(self, 
        identifier: str, 
        position: int, 
        name: str, 
        datastore: Datastore
    ) -> VizualApiResult:
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
        name: string, optional
            New column name
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def import_dataset(self, 
        datastore: Datastore, 
        project_id: str, 
        dataset_id: str
    ) -> VizualApiResult:
        """Import a published dataset from another project.

        The URL is expected to have the format
            vizier://ds/[package_id]/[dataset_id]
            or 
            vizier://view/[package_id]/[branch_id]/[workflow_id or 'head']/[dataset name]
        """
        raise NotImplementedError


    @abstractmethod
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
        allows to associate an identifier with a downloaded file to avoid
        future downloads (unless the reload flag is True).

        Parameters
        ----------
        datastore : vizier.datastore.fs.base.FileSystemDatastore
            Datastore to retireve and update datasets.
        filestore: vizier.filestore.Filestore
            Filestore to retrieve uploaded datasets.
        file_id: string, optional
            Identifier for a file in an associated filestore.
        url: string, optional
            Identifier for a web resource.
        detect_headers: bool, optional
            Detect column names in loaded file if True.
        infer_types: bool, optional
            vizier/engine/packages/vizual/api/base.py
        load_format: string, optional
            Format identifier.
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
        raise NotImplementedError()

    @abstractmethod
    def empty_dataset(self, 
        datastore: Datastore, 
        filestore: Filestore, 
        initial_columns: List[Tuple[str, str]] = [ ("''", "unnamed_column") ]
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
        initial_columns: array of (string, string)
            Default value / name pairs

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def rename_column(self, 
            identifier: str, 
            column_id: int, 
            name: str, 
            datastore: Datastore
        ):
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def update_cell(self, 
            identifier:str, 
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
        raise NotImplementedError

    @abstractmethod
    def materialize_dataset(self, 
            identifier: str, 
            datastore: Datastore
        ) -> VizualApiResult:
        """Create a materialized snapshot of the dataset for faster
        execution."""
        raise NotImplementedError
