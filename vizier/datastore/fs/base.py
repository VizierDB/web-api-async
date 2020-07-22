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
from vizier.datastore.reader import DefaultJsonDatasetReader
from vizier.filestore.base import FileHandle
from vizier.filestore.base import get_download_filename

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

    def create_dataset(self, columns, rows, properties=None, human_readable_name=None, backend_options=None, dependencies=None):
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
        properties: dict(string, ANY), optional
            Properties for dataset components

        Returns
        -------
        vizier.datastore.dataset.DatasetDescriptor
        """
        # Validate (i) that each column has a unique identifier, (ii) each row
        # has a unique identifier, and (iii) that every row has exactly one
        # value per column.
        identifiers = set(row.identifier for row in rows if row.identifier >= 0)
        identifiers.add(0)
        max_row_id = max(identifiers)
        rows = [
            DatasetRow(
                identifier = row.identifier if row.identifier >= 0 else idx + max_row_id,
                values = row.values,
                caveats = row.caveats
            )
            for idx, row in enumerate(rows)
        ]
        _, max_row_id = validate_dataset(columns=columns, rows=rows)
        # Get new identifier and create directory for new dataset
        identifier = get_unique_identifier()
        dataset_dir = self.get_dataset_dir(identifier)
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
            max_row_id=max_row_id,
            properties=properties
        )
        dataset.to_file(
            descriptor_file=os.path.join(dataset_dir, DESCRIPTOR_FILE)
        )
        # Write metadata file if annotations are given
        if properties is not None:
            dataset.write_properties_to_file(self.get_properties_filename(identifier))
        # Return handle for new dataset
        return DatasetDescriptor(
            identifier=dataset.identifier,
            columns=dataset.columns
        )

    def get_properties(self, identifier):
        properties_filename = self.get_properties_filename(identifier)
        if os.path.isfile(properties_filename):
            with open(properties_filename, 'r') as f:
                return json.loads(f.read())
        else:
            return {}

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
                with open(download_file, 'wb') as f:
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
            properties_filename=self.get_properties_filename(identifier)
        )

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
            for col_name in next(reader):
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

    def create_object(
        self, value, obj_type="text/plain"
    ):
        """Update the annotations for a component of the datasets with the given
        identifier. Returns the updated annotations or None if the dataset
        does not exist.

        The distinction between old value and new value is necessary since
        annotations have no unique identifier. We use the key,value pair to
        identify an existing annotation for update. When creating a new
        annotation th old value is None.

        Parameters
        ----------
        value: bytes
            The value of the object
        obj_type: string, optional
            The type of the object
        Returns
        -------
        identifier
        """
        data_object_filename = None
        identifier = None
        while data_object_filename is None:
            identifier = "OBJ_"+get_unique_identifier()
            data_object_filename = self.get_data_object_file(identifier)
            if os.path.exists(data_object_filename):
                data_object_filename = None

        with open(data_object_filename, "wb") as f:
            f.write(value)
        with open(data_object_filename+".mime", "w") as f:
            f.write(obj_type)

        return identifier

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
        data_object_filename = self.get_data_object_file(identifier)
        if expected_type is not None:
            with open(data_object_filename+".mime") as f:
                actual_type = f.read()
                if actual_type != expected_type:
                    raise Exception("Object {} is of type {}, but of type {}".format(identifier, actual_type, expected_type))
        with open(data_object_filename, 'rb') as f:
            f.read()

    def get_data_object_file(self, identifier):
        """Get the absolute path of the file that maintains the dataset metadata
        such as the order of row id's and column information.
        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        Returns
        -------
        string
        """
        return os.path.join(self.get_dataobject_dir(identifier), DATA_OBJECT_FILE)

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
