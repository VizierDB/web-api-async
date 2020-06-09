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

"""Default implementation for a vizier datastore. The file system datastore
maintains datasets information in separate file within a dataset-specific
subfolder of a given base directory.
"""

import csv
import json
import os
import shutil
import tempfile
import urllib.request, urllib.error, urllib.parse

from vizier.core.util import cast, get_unique_identifier
from vizier.datastore.base import DefaultDatastore
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor
from vizier.datastore.dataset import DatasetHandle, DatasetRow
from vizier.datastore.fs.dataset import FileSystemDatasetHandle
from vizier.datastore.object.dataobject import DataObjectMetadata
from vizier.datastore.reader import DefaultJsonDatasetReader
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.filestore.base import FileHandle
from vizier.filestore.base import get_download_filename

import datamart_profiler
import pandas as pd

import vizier.datastore.base as base


"""Constants for data file names."""
DATA_FILE = 'data.json'
DESCRIPTOR_FILE = 'descriptor.json'


class FileSystemDatastore(DefaultDatastore):
    """Implementation of Vizier data store. Uses the file system to maintain
    datasets. For each dataset a new subfolder is created. Within the folder the
    dataset information is split across three files containing the descriptor,
    annotation, and the dataset rows.
    """
    def __init__(self, base_path):
        """Initialize the base directory that contains datasets. Each dataset is
        maintained in a separate subfolder.

        Parameters
        ---------
        base_path : string
            Path to base directory for the datastore
        """
        super(FileSystemDatastore, self).__init__(base_path)

    def create_dataset(
        self, columns, rows, human_readable_name=None, annotations=None,
        backend_options=[], dependencies=[]
    ):
        """Create a new dataset in the datastore. Expects at least the list of
        columns and the rows for the dataset.

        Raises ValueError if (1) the column identifier are not unique, (2) the
        row identifier are not uniqe, (3) the number of columns and values in a
        row do not match, (4) any of the column or row identifier have a
        negative value, or (5) if the given column or row counter have value
        lower or equal to any of the column or row identifier.

        Parameters
        ----------
        columns: list(vizier.datastore.dataset.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.dataset.DatasetRow)
            List of dataset rows.
        human_readable_name: string, ignored
            TODO: Add description.
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata, optional
            Annotations for dataset components
        backend_options: list, ignored
            TODO: Add description.
        dependencies: string, ignored
            TODO: Add description.

        Returns
        -------
        vizier.datastore.dataset.DatasetDescriptor
        """
        # Validate (i) that each column has a unique identifier, (ii) each row
        # has a unique identifier, and (iii) that every row has exactly one
        # value per column.
        _, max_row_id = validate_dataset(columns=columns, rows=rows)
        # Get new identifier and create directory for new dataset
        identifier = get_unique_identifier()
        dataset_dir = self.get_dataset_dir(identifier)
        os.makedirs(dataset_dir)
        # Write rows to data file
        data_file = os.path.join(dataset_dir, DATA_FILE)
        DefaultJsonDatasetReader(data_file).write(rows)
        # Filter annotations for non-existing resources
        if not annotations is None:
            annotations = annotations.filter(
                columns=[c.identifier for c in columns],
                rows=[r.identifier for r in rows]
            )
        # Create dataset an write dataset file
        dataset = FileSystemDatasetHandle(
            identifier=identifier,
            columns=columns,
            data_file=data_file,
            row_count=len(rows),
            max_row_id=max_row_id,
            annotations=annotations
        )
        dataset.to_file(
            descriptor_file=os.path.join(dataset_dir, DESCRIPTOR_FILE)
        )
        # Write metadata file if annotations are given
        if not annotations is None:
            dataset.annotations.to_file(
                self.get_metadata_filename(identifier)
            )
        # Return handle for new dataset
        return DatasetDescriptor(
            identifier=dataset.identifier,
            columns=dataset.columns,
            row_count=dataset.row_count
        )

    def delete_dataset(self, identifier):
        """Delete dataset with given identifier. Returns True if dataset existed
        and False otherwise.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier.

        Returns
        -------
        bool
        """
        # Delete the dataset directory if it exists. Otherwise return False
        dataset_dir = self.get_dataset_dir(identifier)
        if not os.path.isdir(dataset_dir):
            return False
        shutil.rmtree(dataset_dir)
        return True

    def download_dataset(self, url, username=None, password=None, filestore=None):
        """Create a new dataset from a given file. Returns the handle for the
        downloaded file only if the filestore has been provided as an argument
        in which case the file handle is meaningful file handle.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        url : string
            Unique resource identifier for external resource that is accessed
        username: string, optional
            Optional user name for authentication
        password: string, optional
            Optional password for authentication
        filestore: vizier.filestore.base.Filestore, optional
            Optional filestore to save a local copy of the downloaded resource

        Returns
        -------
        vizier.datastore.fs.dataset.FileSystemDatasetHandle,
        vizier.filestore.base.FileHandle
        """
        if not filestore is None:
            # Upload the file to the filestore to get the file handle
            fh = filestore.download_file(
                url=url,
                username=username,
                password=password
            )
            # Since the filestore was given we return a tuple of dataset
            # descriptor and file handle
            return self.load_dataset(fh), fh
        else:
            # Manually download the file temporarily
            temp_dir = tempfile.mkdtemp()
            try:
                response = urllib.request.urlopen(url)
                filename = get_download_filename(url, response.info())
                download_file = os.path.join(temp_dir, filename)
                mode = 'w'
                if filename.endswith('.gz'):
                    mode += 'b'
                with open(download_file, mode) as f:
                    f.write(response.read())
                fh = FileHandle(
                    identifier=filename,
                    filepath=download_file,
                    file_name=filename
                )
                dataset = self.load_dataset(fh)
                shutil.rmtree(temp_dir)
                # Return only the dataset descriptor
                return dataset
            except Exception as ex:
                if os.path.isdir(temp_dir):
                    shutil.rmtree(temp_dir)
                raise ex

    def get_dataset(self, identifier):
        """Read a full dataset from the data store. Returns None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.fs.dataset.FileSystemDatasetHandle
        """
        # Test if a subfolder for the given dataset identifier exists. If not
        # return None.
        dataset_dir = self.get_dataset_dir(identifier)
        if not os.path.isdir(dataset_dir):
            return None
        # Load the dataset handle
        return FileSystemDatasetHandle.from_file(
            descriptor_file=os.path.join(dataset_dir, DESCRIPTOR_FILE),
            data_file=os.path.join(dataset_dir, DATA_FILE),
            annotations=DatasetMetadata.from_file(
                self.get_metadata_filename(identifier)
            )
        )

    def get_objects(self, identifier=None, obj_type=None, key=None):
        """Get list of data objects for a resources of a given dataset. If only
        the column id is provided annotations for the identifier column will be
        returned. If only the row identifier is given all annotations for the
        specified row are returned. Otherwise, all annotations for the specified
        cell are returned. If both identifier are None all annotations for the
        dataset are returned.

        Parameters
        ----------
        identifier: string, optional
            Unique object identifier
        obj_type: string, optional
            object type
        key: string, optional
            object key

        Returns
        -------
        vizier.datastore.object.dataobject.DataObjectMetadata
        """
        return DataObjectMetadata()

    def format_type(self, structural_type, semantic_types=None ):
        # typeData = ['String', 'Integer', 'Float', 'Categorical', 'DateTime', 'Text', 'Boolean', 'Real'];
        switcher = {
            'Enumeration' :  'categorical',
            'DateTime' :  'datetime',
            'Text' :  'varchar',
            'Real' :  'real',
            'Float' :  'real',
            'Integer' :  'int',
            'Boolean' :  'boolean'
        }
        if len(semantic_types) > 0:
            column_type = semantic_types[0][semantic_types[0].rindex('/') + 1:]
        else:
            column_type = structural_type[structural_type.rindex('/') + 1:]
        # Get the function from switcher dictionary
        vizier_column_type = switcher.get(column_type, 'varchar')
        return vizier_column_type

    def get_types(self, f_handle):
        print("Computing column types ...")
        columns_name = []
        data_rows = []
        with f_handle.open() as csvfile:
            reader = csv.reader(csvfile, delimiter=f_handle.delimiter)
            for col_name in next(reader):
                columns_name.append(col_name.strip())
            for row in reader:
                values = [cast(v.strip()) for v in row]
                data_rows.append(values)
        df = pd.DataFrame(data=data_rows, columns=columns_name)
        metadata = datamart_profiler.process_dataset(df, include_sample=True)

        # get types
        columns_info = metadata['columns']
        column_types = {}
        for column in columns_info:
            vizier_column_type = self.format_type(column['structural_type'], column['semantic_types'])
            column_types[column['name']] = vizier_column_type
        return column_types

    def load_dataset(
        self, f_handle=None, url=None, detect_headers=True, infer_types='none',
        load_format='csv', options=[], human_readable_name=None
    ):
        """Create a new dataset from a given file or Url.

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle, optional
            handle for an uploaded file on the associated file server.
        url: string, optional, optional
            Url for the file source
        detect_headers: bool, optional
            Detect column names in loaded file if True
        infer_types: string, optional
            Infer column types for loaded dataset if selected a profiler.
        load_format: string, optional
            Format identifier
        options: list, optional
            Additional options for Mimirs load command
        human_readable_name: string, optional
            Optional human readable name for the resulting table

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        # The file handle might be None in which case an exception is raised
        if f_handle is None:
            raise ValueError('unknown file')
        # Expects a file in a supported tabular data format.
        if not f_handle.is_tabular:
            raise ValueError('cannot create dataset from file \'' + f_handle.name + '\'')
        # Open the file as a csv file. Expects that the first row contains the
        # column names. Read dataset schema and dataset rows into two separate
        # lists.
        column_types = self.get_types(f_handle)
        columns = []
        rows = []
        with f_handle.open() as csvfile:
            reader = csv.reader(csvfile, delimiter=f_handle.delimiter)
            for col_name in next(reader):
                column_type = column_types[col_name.strip()] if infer_types == 'datamartprofiler' else None
                columns.append(
                    DatasetColumn(
                        identifier=len(columns),
                        name=col_name.strip(),
                        data_type=column_type
                    )
                )
            for row in reader:
                values = [cast(v.strip()) for v in row]
                rows.append(DatasetRow(identifier=len(rows), values=values))
        # Get unique identifier and create subfolder for the new dataset
        identifier = get_unique_identifier()
        dataset_dir = self.get_dataset_dir(identifier)
        os.makedirs(dataset_dir)
        # Write rows to data file
        data_file = os.path.join(dataset_dir, DATA_FILE)
        DefaultJsonDatasetReader(data_file).write(rows)
        # Create dataset an write descriptor to file
        dataset = FileSystemDatasetHandle(
            identifier=identifier,
            columns=columns,
            data_file=data_file,
            row_count=len(rows),
            max_row_id=len(rows) - 1
        )
        dataset.to_file(
            descriptor_file=os.path.join(dataset_dir, DESCRIPTOR_FILE)
        )
        return dataset

    def update_object(
        self, identifier, key, old_value=None, new_value=None, obj_type=None
    ):
        """Update a data object.

        Parameters
        ----------
        identifier : string
            Unique object identifier
        key: string, optional
            object key
        old_value: string, optional
            Previous value when updating an existing annotation.
        new_value: string, optional
            Updated value
        Returns
        -------
        bool
        """
        # TODO: Implementation needed
        raise NotImplementedError()

    def unload_dataset(
        self, filepath, dataset_name, format='csv', options=[], filename=''
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
        # TODO: Implementation needed
        raise NotImplementedError()


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def validate_dataset(columns, rows):
    """Validate that (i) each column has a unique identifier, (ii) each row has
    a unique identifier, and (iii) each row has exactly one value per column.

    Returns the maximum column and row identifiers. Raises ValueError in case
    of a schema violation.

    Parameters
    ----------
    columns: list(vizier.datastore.dataset.DatasetColumn)
        List of columns. It is expected that each column has a unique
        identifier.
    rows: list(vizier.datastore.dataset.DatasetRow)
        List of dataset rows.

    Returns
    -------
    int, int
    """
    # Ensure that all column identifier are zero or greater, unique, and smaller
    # than the column counter (if given)
    col_ids = set()
    for col in columns:
        if col.identifier < 0:
            raise ValueError('negative column identifier \'' + str(col.identifier) + '\'')
        elif col.identifier in col_ids:
            raise ValueError('duplicate column identifier \'' + str(col.identifier) + '\'')
        col_ids.add(col.identifier)
    # Ensure that all row identifier are zero or greater, unique, smaller than
    # the row counter (if given), and contain exactly one value for each column
    row_ids = set()
    for row in rows:
        if len(row.values) != len(columns):
            raise ValueError('schema violation for row \'' + str(row.identifier) + '\'')
        elif row.identifier < 0:
            raise ValueError('negative row identifier \'' + str(row.identifier) + '\'')
        elif row.identifier in row_ids:
            raise ValueError('duplicate row identifier \'' + str(row.identifier) + '\'')
        row_ids.add(row.identifier)
    return max(col_ids) if len(col_ids) > 0 else -1, max(row_ids) if len(row_ids) > 0 else -1
