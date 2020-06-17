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
import os
import pandas as pd
import shutil
import tempfile
import urllib.request
import urllib.error
import urllib.parse

from vizier.core.util import cast, get_unique_identifier
from vizier.datastore.base import DefaultDatastore
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor
from vizier.datastore.fs.dataset import FileSystemDatasetHandle
from vizier.datastore.object.dataobject import DataObjectMetadata
from vizier.datastore.reader import DefaultJsonDatasetReader
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.filestore.base import FileHandle
from vizier.filestore.base import get_download_filename

import vizier.datastore.profiling.datamart as datamart


"""Constants for data file names."""
DATA_FILE = 'data.json'
DESCRIPTOR_FILE = 'descriptor.json'


class FileSystemDatastore(DefaultDatastore):
    """Implementation of Vizier data store. Uses the file system to maintain
    datasets. For each dataset a new subfolder is created. Within the folder
    the dataset information is split across three files containing the
    descriptor, annotation, and the dataset rows.
    """
    def __init__(self, base_path):
        """Initialize the base directory that contains datasets. Each dataset
        is maintained in a separate subfolder.

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
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata,
        default=None
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
        if annotations is not None:
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
        if annotations is not None:
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

    def download_dataset(
        self, url, username=None, password=None, filestore=None
    ):
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
        if filestore is not None:
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
        specified row are returned. Otherwise, all annotations for the
        specified cell are returned. If both identifier are None all
        annotations for the dataset are returned.

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

    def load_dataset(
        self, f_handle=None, url=None, detect_headers=True, infer_types='none',
        load_format='csv', options=[], human_readable_name=None
    ):
        """Create a new dataset from a given file or Url.

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle, optional
            handle for an uploaded file on the associated file server.
        url: string, default=None
            Url for the file source. Included for API completeness.
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
            raise ValueError('cannot load dataset from %s ' % (f_handle.name))
        # Open the file as a csv file. Expects that the first row contains the
        # column names. Read dataset schema and dataset rows into two separate
        # lists.
        column_names = []
        rows = []
        with f_handle.open() as csvfile:
            reader = csv.reader(csvfile, delimiter=f_handle.delimiter)
            for col_name in next(reader):
                column_names.append(col_name.strip())
            for row in reader:
                values = [cast(v.strip()) for v in row]
                rows.append(values)
        if infer_types == 'datamartprofiler':
            # Run the Datamart profiler if requested by the user.
            df = pd.DataFrame(data=rows, columns=column_names)
            metadata = datamart.profile(df, include_sample=True)
            column_types = datamart.get_types(metadata)
        else:
            metadata = None
            column_types = dict()
        # Create column objects.
        columns = []
        for col_name in column_names:
            column_type = column_types.get(col_name)
            columns.append(
                DatasetColumn(
                    identifier=len(columns),
                    name=col_name.strip(),
                    data_type=column_type
                )
            )
        # Get unique identifier and create subfolder for the new dataset
        identifier = get_unique_identifier()
        dataset_dir = self.get_dataset_dir(identifier)
        os.makedirs(dataset_dir)
        # Write rows to data file
        data_file = os.path.join(dataset_dir, DATA_FILE)
        DefaultJsonDatasetReader(data_file).write(rows)
        # Create dataset and write descriptor to file
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
    # Ensure that all column identifier are zero or greater, unique, and
    # smaller than the column counter (if given)
    col_ids = set()
    for col in columns:
        col_id = col.identifier
        if col.identifier < 0:
            raise ValueError('negative identifier %d' % (col_id))
        elif col.identifier in col_ids:
            raise ValueError('duplicate identifier %d' % (col_id))
        col_ids.add(col_id)
    # Ensure that all row identifier are zero or greater, unique, smaller than
    # the row counter (if given), and contain exactly one value for each column
    row_ids = set()
    for row in rows:
        row_id = row.identifier
        if len(row.values) != len(columns):
            raise ValueError('schema violation for row %d' % str(row_id))
        elif row.identifier < 0:
            raise ValueError('negative row identifier %d' % str(row_id))
        elif row.identifier in row_ids:
            raise ValueError('duplicate row identifier %d' % str(row_id))
        row_ids.add(row_id)
    max_colid = max(col_ids) if len(col_ids) > 0 else -1
    max_rowid = max(row_ids) if len(row_ids) > 0 else -1
    return max_colid, max_rowid
