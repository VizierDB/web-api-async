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

"""Implementation of the vizier datastore interface that uses Mimir as the
storage backend.
"""

import os
from typing import List, Dict, Any, Optional, Tuple

from vizier.core.util import get_unique_identifier
from vizier.filestore.base import FileHandle
from vizier.datastore.base import DefaultDatastore
from vizier.datastore.dataset import DatasetRow, DatasetColumn, DatasetDescriptor
from vizier.datastore.annotation.base import DatasetCaveat
from vizier.datastore.mimir.dataset import MimirDatasetColumn, MimirDatasetHandle

import vizier.mimir as mimir
import vizier.datastore.mimir.base as base
from vizier.filestore.fs.base import DATA_FILENAME, write_metadata_file
import shutil
from pandas import DataFrame
            
"""Name of file storing dataset (schema) information."""
DATASET_FILE = 'dataset.json'
DATA_OBJECT_FILE = 'dataobjects.json'

SAFE_FORMAT_IDENTIFIER_PREFIXES = set(["csv", "json", "text"])

class MimirDatastore(DefaultDatastore):
    """Vizier data store implementation using Mimir.

    Maintains information about the dataset schema separately. This is necessary
    because in a dataset column names are not necessarily unique. For each
    dataset a new subfolder is created in the store base directory. In that
    folder a dataset file and an annotation file are maintained. All files are
    in Yaml format.

    Note that every write_dataset call creates a new table in the underlying
    Mimir database. Other datasets are views on these tables.
    """
    def __init__(self, base_path):
        """Initialize the base directory that contains the dataset index and
        metadata files.

        Parameters
        ----------
        base_path: string
            Name of the directory where metadata is stored
        """
        super(MimirDatastore, self).__init__(base_path)

    def get_properties(self, identifier):
        schema, properties = mimir.getTableInfo(identifier)
        return properties

    def create_dataset(self, 
            columns: List[DatasetColumn], 
            rows: List[DatasetRow], 
            properties: Dict[str, Any] = None,
            human_readable_name: str = "Untitled Dataset",
            backend_options: Optional[List[Tuple[str, str]]] = None, 
            dependencies: Optional[List[str]] = None
        ) -> MimirDatasetHandle:
        """Create a new dataset in the datastore. Expects at least the list of
        columns and the rows for the dataset.

        Parameters
        ----------
        columns: list(vizier.datastore.dataset.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.dataset.DatasetRow)
            List of dataset rows.
        properties: dict(string, any), optional
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.dataset.DatasetDescriptor
        """
        # Get unique identifier for new dataset
        properties = {} if properties is None else properties
        backend_options = [] if backend_options is None else backend_options
        dependencies = [] if dependencies is None else dependencies
        identifier = 'DS_' + get_unique_identifier()
        columns = [
            col if isinstance(col, MimirDatasetColumn) else MimirDatasetColumn(
                    identifier = col.identifier,
                    name_in_dataset = col.name,
                    data_type = col.data_type
                )
            for col in columns
        ]

        table_name, schema = mimir.loadDataInline(
            schema = [
                { 
                    "name" : base.sanitize_column_name(col.name), 
                    "type" : col.data_type 
                }
                for col in columns
            ],
            rows = [
                row.values
                for row in rows
            ], 
            result_name = identifier,
            human_readable_name = human_readable_name,
            dependencies = dependencies,
            properties = properties
        )

        # Insert the new dataset metadata information into the datastore
        return MimirDatasetHandle.from_mimir_result(
            table_name = table_name, 
            schema = schema, 
            properties = properties, 
            name = human_readable_name
        )

    def get_dataset(self, 
            identifier: str, 
            force_profiler: Optional[bool] = None, 
            name: Optional[str] = None
        ) -> MimirDatasetHandle:
        """Read a full dataset from the data store. Returns None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.mimir.dataset.MimirDatasetHandle
        """
        # Return None if the dataset file does not exist
        schema, properties = mimir.getTableInfo(identifier, force_profiler = force_profiler)
        return MimirDatasetHandle.from_mimir_result(identifier, schema, properties, name)

    def get_dataset_frame(self, identifier: str, force_profiler: Optional[bool] = None) -> Optional[DataFrame]:
        import pyarrow as pa #type: ignore
        from pyspark.rdd import _load_from_socket #type: ignore
        from pyspark.sql.pandas.serializers import ArrowCollectSerializer #type: ignore
        
        portSecret = mimir.getDataframe(query = 'SELECT * FROM {}'.format(identifier))
        results = list(_load_from_socket((portSecret['port'], portSecret['secret']), ArrowCollectSerializer()))
        batches = results[:-1]
        batch_order = results[-1]
        ordered_batches = [batches[i] for i in batch_order]
        table = pa.Table.from_batches(ordered_batches)
        return table.to_pandas()
      
    def get_caveats(self, 
            identifier: str, 
            column_id: Optional[int] = None, 
            row_id: Optional[str] = None
        ) -> List[DatasetCaveat]:
        """Get list of annotations for a dataset component. Expects at least one
        of the given identifier to be a valid identifier (>= 0).

        Parameters
        ----------
        column_id: int, optional
            Unique column identifier
        row_id: int, optiona
            Unique row identifier

        Returns
        -------
        list(vizier.datastore.metadata.Annotation)
        """
        # Return immediately if request is for column or row annotations. At the
        # moment we only maintain uncertainty information for cells. If cell
        # annotations are requested we need to query the database to retrieve
        # any existing uncertainty annotations for the cell.
        return self.get_dataset(identifier).get_caveats(column_id,row_id)
    
    def get_object(self, identifier, expected_type=None):
        """Get list of data objects for a resources of a given dataset. 

        Parameters
        ----------
        identifier: string
            Unique object identifier
        expected_type: string, optional
            Will raise an error if the type of the object doesn't conform to the expected type.
            
        Returns
        -------
        bytes
        """
        return mimir.getBlob(identifier, expected_type = expected_type)#.encode
       
    def create_object(
        self, value: bytes, obj_type: str = "text/plain"
    ) -> str:
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the updated annotations or None if the dataset
        does not exist.

        The distinction between old value and new value is necessary since
        annotations have no unique identifier. We use the key,value pair to
        identify an existing annotation for update. When creating a new
        annotation th old value is None.

        Parameters
        ----------
        key: string, optional
            object key
        old_value: string, optional
            Previous value when updating an existing annotation.
        new_value: string, optional
            Updated value
        Returns
        -------
        identifier
        """
        return mimir.createBlob(
            identifier = "{}".format( get_unique_identifier()), 
            blob_type = obj_type, 
            data = value
        )

    def download_dataset(
        self, url, username=None, password=None, filestore=None, detect_headers=True, 
        infer_types='none', load_format='csv', options=[], human_readable_name=None
    ):
        """Create a new dataset from a given file. Returns the handle for the
        downloaded file only if the filestore has been provided as an argument
        in which case the file handle is a meaningful file handle.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        url : string
            Unique resource identifier for external resource that is accessed
        username: string, optional
            Optional user name for authentication
        password: string, optional
            Optional password for authentication
        detect_headers: bool, optional
            Detect column names in loaded file if True
        infer_types: string, optional
            Infer column types for loaded dataset if selected a profiler.
        load_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command
        filestore: vizier.filestore.base.Filestore, optional
            Optional filestore to save a local copy of the downloaded resource
        human_readable_name: string, optional
            Optional human readable name for the resulting table.
        Returns
        -------
        vizier.datastore.fs.dataset.FileSystemDatasetHandle,
        vizier.filestore.base.FileHandle
        """
        if username is not None:
            options += [("username", username)]
        if password is not None:
            options += [("password", password)]
        return self.load_dataset(
            url = url, 
            options = options,
            detect_headers = detect_headers,
            infer_types = infer_types,
            load_format = load_format,
            human_readable_name = human_readable_name
        )

    def load_dataset(self, 
            f_handle: Optional[FileHandle] = None, 
            proposed_schema: List[Tuple[str,str]] = [],
            url: Optional[str] = None, 
            detect_headers: bool = True, 
            infer_types: bool = True, 
            properties: Dict[str,Any] = {},
            load_format: str ='csv', 
            options: List[Dict[str,str]] = [], 
            human_readable_name: Optional[str] = None,
    ):
        """Create a new dataset from a given file or url. Expects that either
        the file handle or the url are not None. Raises ValueError if both are
        None or not None.


        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle, optional
            handle for an uploaded file on the associated file server.
        url: string, optional, optional
            Url for the file source
        detect_headers: bool, optional
            Detect column names in loaded file if True
        infer_types: bool, optional
            Infer column types for loaded dataset if True
        load_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command
        human_readable_name: string, optional
            Optional human readable name for the resulting table

        Returns
        -------
        vizier.datastore.mimir.dataset.MimirDatasetHandle
        """
        assert(url is not None or f_handle is not None)
        if f_handle is None and url is None:
            raise ValueError('no load source given')
        elif f_handle is not None and url is not None:
            raise ValueError('too many load sources given')
        elif url is None and f_handle is not None:
            # os.path.abspath((r'%s' % os.getcwd().replace('\\','/') ) + '/' + f_handle.filepath)
            abspath = f_handle.filepath
        elif url is not None:
            abspath = url


        # for ease of debugging, associate each table with a prefix identifying its nature
        prefix = load_format if load_format in SAFE_FORMAT_IDENTIFIER_PREFIXES else "LOADED_"

        # Load dataset into Mimir
        table_name, mimirSchema = mimir.loadDataSource(
            abspath,
            infer_types,
            detect_headers,
            load_format,
            human_readable_name,
            options, 
            properties = properties,
            result_name = prefix + get_unique_identifier(),
            proposed_schema = proposed_schema
        )
        return MimirDatasetHandle.from_mimir_result(table_name, mimirSchema, properties, human_readable_name)


    def unload_dataset(self, 
            filepath: str, 
            dataset_name: str, 
            format: str = 'csv', 
            options: List[Dict[str, Any]]=[], 
            filename=""
        ):
        """Export a dataset from a given name.
        Raises ValueError if the given dataset could not be exported.
        Parameters
        ----------
        dataset_name: string
            Name of the dataset to unload
            
        format: string
            Format for output (csv, json, ect.)
            
        options: dict
            Options for data unload
            
        filename: string
            The output filename - may be empty if outputting to a database
        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        name = os.path.basename(filepath).lower()
        basepath = filepath.replace(name,"")
        
        # Create a new unique identifier for the file.
        
        abspath = os.path.abspath((r'%s' % filepath ) )
        exported_files = mimir.unloadDataSource(dataset_name, abspath, format, options)
        file_handles = []
        for output_file in exported_files:
            name = os.path.basename(output_file).lower()
            identifier = get_unique_identifier()
            file_dir = os.path.join(basepath,identifier )
            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)
            fs_output_file = os.path.join(file_dir, DATA_FILENAME)
            shutil.move(os.path.join(filepath, output_file),fs_output_file)
            f_handle = FileHandle(
                    identifier,
                    output_file,
                    name
                )
            file_handles.append(f_handle )
            write_metadata_file(file_dir,f_handle)
        return file_handles
        
    def query(self, 
        query: str,
        datasets: Dict[str, DatasetDescriptor]
    ) -> Dict[str, Any]:
        """Pose a raw SQL query against the specified datasets.
        Doesn't actually change the data, just queries it.
        """
        views = dict(
            (view, datasets[view].identifier)
            for view in datasets
        )
        result = mimir.sqlQuery(
                        query = query, 
                        views = views
                )
        return result
