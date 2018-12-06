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

"""Default implementation for a vizier datastore. The file system datastore
maintains datasets information in separate file within a dataset-specific
subfolder of a given base directory.
"""

import csv
import json
import os
import shutil
import tempfile
import urllib2

from vizier.core.system import build_info
from vizier.core.util import cast, get_unique_identifier
from vizier.datastore.base import Datastore
from vizier.datastore.dataset import DatasetHandle, DatasetColumn, DatasetRow
from vizier.datastore.fs import VERSION_INFO
from vizier.datastore.fs.dataset import FileSystemDatasetHandle
from vizier.datastore.reader import DefaultJsonDatasetReader
from vizier.datastore.metadata import DatasetMetadata
from vizier.filestore.base import FileHandle
from vizier.filestore.base import get_download_filename, get_fileformat


"""Constants for data file names."""
DATA_FILE = 'data.json'
DESCRIPTOR_FILE = 'descriptor.json'
METADATA_FILE = 'annotations.json'

"""Configuration parameter."""
PARA_DIRECTORY = 'directory'


class FileSystemDatastore(Datastore):
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
        super(FileSystemDatastore, self).__init__(
            build_info(
                name='vizier.datastore.fs.base.FileSystemDatastore',
                version_info=VERSION_INFO
            )
        )
        # Create the base directory if it does not exist
        self.base_path = os.path.abspath(base_path)
        if not os.path.isdir(self.base_path):
            os.makedirs(self.base_path)

    def create_dataset(
        self, columns, rows, column_counter=None, row_counter=None,
        annotations=None
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
        column_counter: int, optional
            Counter to generate unique column identifier
        row_counter: int, optional
            Counter to generate unique row identifier
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.fs.dataset.FileSystemDatasetHandle
        """
        # Validate (i) that each column has a unique identifier, and (ii) that
        # every row has exactly one value per column. If the column counter is
        # not None it is also verified that the column id's are smaller than the
        # counter value. If the row_counter is given it is verified that all
        # row id's are smaller than the counter value. Returns the maximum
        # column and row identifier
        max_col_id, max_row_id = validate_dataset(
            columns,
            rows,
            column_counter=column_counter,
            row_counter=row_counter
        )
        # Get new identifier and create directory for new dataset
        identifier = get_unique_identifier()
        dataset_dir = os.path.join(self.base_path, identifier)
        os.makedirs(dataset_dir)
        # Write rows to data file
        data_file = os.path.join(dataset_dir, DATA_FILE)
        DefaultJsonDatasetReader(data_file).write(rows)
        # Create dataset an write dataset file
        dataset = FileSystemDatasetHandle(
            identifier=identifier,
            columns=columns,
            data_file=data_file,
            row_count=len(rows),
            column_counter=column_counter if not column_counter is None else max_col_id + 1,
            row_counter=row_counter if not row_counter is None else max_row_id + 1,
            annotations=annotations
        )
        dataset.to_file(
            descriptor_file=os.path.join(dataset_dir, DESCRIPTOR_FILE)
        )
        # Write metadata file if annotations are given
        if not annotations is None:
            dataset.annotations.to_file(
                os.path.join(dataset_dir, METADATA_FILE)
            )
        # Return handle for new dataset
        return dataset

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
        dataset_dir = os.path.join(self.base_path, identifier)
        if not os.path.isdir(dataset_dir):
            return False
        shutil.rmtree(dataset_dir)
        return True

    def download_dataset(self, uri, username=None, password=None, filestore=None):
        """Create a new dataset from a given file. Returns the handle for the
        downloaded file only if the filestore has been provided as an argument
        in which case the file handle is meaningful file handle.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        uri : string
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
                uri=uri,
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
                response = urllib2.urlopen(uri)
                filename = get_download_filename(uri, response.info())
                download_file = os.path.join(temp_dir, filename)
                mode = 'w'
                if filename.endswith('.gz'):
                    mode += 'b'
                with open(download_file, mode) as f:
                    f.write(response.read())
                fh = FileHandle(
                    identifier=filename,
                    filepath=download_file,
                    file_name=filename,
                    file_format=get_fileformat(filename)
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
        dataset_dir = os.path.join(self.base_path, identifier)
        if not os.path.isdir(dataset_dir):
            return None
        # Load the dataset handle
        return FileSystemDatasetHandle.from_file(
            descriptor_file=os.path.join(dataset_dir, DESCRIPTOR_FILE),
            data_file=os.path.join(dataset_dir, DATA_FILE),
            annotations=DatasetMetadata.from_file(
                os.path.join(dataset_dir, METADATA_FILE)
            )
        )

    def get_descriptor(self, identifier):
        """Get the descriptor for the dataset with given identifier from the
        data store. Returns None if no dataset with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.DatasetDescriptor
        """
        return self.get_dataset(identifier)

    def load_dataset(self, f_handle):
        """Create a new dataset from a given file.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle
            Handle for an uploaded file

        Returns
        -------
        vizier.datastore.fs.dataset.FileSystemDatasetHandle
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
        columns = []
        rows = []
        with f_handle.open() as csvfile:
            reader = csv.reader(csvfile, delimiter=f_handle.delimiter)
            for col_name in reader.next():
                columns.append(
                    DatasetColumn(
                        identifier=len(columns),
                        name=col_name.strip()
                    )
                )
            for row in reader:
                values = [cast(v.strip()) for v in row]
                rows.append(DatasetRow(identifier=len(rows), values=values))
        # Get unique identifier and create subfolder for the new dataset
        identifier = get_unique_identifier()
        dataset_dir = os.path.join(self.base_path, identifier)
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
            column_counter=len(columns),
            row_counter=len(rows)
        )
        dataset.to_file(
            descriptor_file=os.path.join(dataset_dir, DESCRIPTOR_FILE)
        )
        return dataset

    def update_annotation(
        self, identifier, column_id=-1, row_id=-1, anno_id=-1, key=None,
        value=None
    ):
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the updated annotations or None if the dataset
        does not exist.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column_id: int, optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier
        anno_id: int
            Unique annotation identifier
        key: string, optional
            Annotation key
        value: string, optional
            Annotation value

        Returns
        -------
        vizier.datastore.metadata.Annotation
        """
        # Return None if the dataset is unknown
        dataset_dir = os.path.join(self.base_path, identifier)
        if not os.path.isdir(dataset_dir):
            return None
        # Read annotations from file, Evaluate update statement and write result
        # back to file.
        annotations = DatasetMetadata.from_file(
            os.path.join(dataset_dir, METADATA_FILE)
        )
        # Get object annotations and update
        obj_annos = annotations.for_object(column_id=column_id, row_id=row_id)
        result = obj_annos.update(identifier=anno_id, key=key, value=value)
        # Write modified annotations to file
        annotations.to_file(os.path.join(dataset_dir, METADATA_FILE))
        return result


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def validate_dataset(columns, rows, column_counter=None, row_counter=None):
    """Validate (i) that each column has a unique identifier, and (ii) that
    every row has exactly one value per column. If the column counter is not
    None it is also verified that the column id's are smaller than the counter
    value. If the row_counter is given it is verified that all row id's are
    smaller than the counter value. Returns the maximum column and row
    identifier.

    Returns the maximum column and row identifier.

    Raises ValueError in case of a schema violation.

    Parameters
    ----------
    columns: list(vizier.datastore.dataset.DatasetColumn)
        List of columns. It is expected that each column has a unique
        identifier.
    rows: list(vizier.datastore.dataset.DatasetRow)
        List of dataset rows.
    column_counter: int, optional
        Counter to generate unique column identifier
    row_counter: int, optional
        Counter to generate unique row identifier

    Returns
    -------
    int, int
    """
    # Keep track of column and row ids
    max_col_id = -1
    max_row_id = -1
    # Ensure that all column identifier are zero or greater, unique, and smaller
    # than the column counter (if given)
    col_ids = set()
    for col in columns:
        if col.identifier < 0:
            raise ValueError('negative column identifier \'' + str(col.identifier) + '\'')
        elif col.identifier in col_ids:
            raise ValueError('duplicate column identifier \'' + str(col.identifier) + '\'')
        elif not column_counter is None and col.identifier >= column_counter:
            raise ValueError('invalid column identifier \'' + str(col.identifier) + '\'')
        elif col.identifier > max_col_id:
            max_col_id = col.identifier
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
        elif not row_counter is None and row.identifier >= row_counter:
            raise ValueError('invalid row identifier \'' + str(row.identifier) + '\'')
        elif row.identifier > max_row_id:
            max_row_id = row.identifier
        row_ids.add(row.identifier)
    # Return maximum column and row identifier
    return max_col_id, max_row_id
