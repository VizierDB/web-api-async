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

"""File System Data Store - File System-based implementation of the Vizier DB
dataset store.

Maintains individual datasets in individual sub-folders of a base directory.
For each dataset two files are maintained: info.yaml contains the dataset
handle and data.tsv.gz contains the tab-delimited data file.
"""

import json
import os
import shutil

from vizier.core.system import build_info
from vizier.core.util import get_unique_identifier
from vizier.datastore.base import DatasetHandle, DatasetColumn, Datastore
from vizier.datastore.base import validate_schema
from vizier.datastore.mem import InMemDatasetHandle
from vizier.datastore.reader import DefaultJsonDatasetReader
from vizier.datastore.metadata import DatasetMetadata


"""Constants for data file names."""
DATA_FILE = 'data.json'
HANDLE_FILE = 'handle.json'
METADATA_FILE = 'annotation.json'

"""Configuration parameter."""
PARA_DIRECTORY = 'directory'


class FileSystemDatasetHandle(DatasetHandle):
    """Handle for a dataset that is stored on the file system.

    The dataset handle keeps counters for columns and rows id's to generate
    unique unique identifier.

    The dataset rows are stored in a separate file. The file format is JSON with
    the following structure:
        {
            'rows': [
                {'id': int, 'values': [...]}
            ]
        }
    """
    def __init__(
        self, identifier, columns, datafile, row_count=0, column_counter=0,
        row_counter=0, annotations=None
    ):
        """Initialize the dataset handle.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.
        columns: list(vizier.datastore.base.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        datafile: string
            Path to the file that contains the dataset rows. The data is stored
            in Json format.
        column_counter: int, optional
            Counter to generate unique column identifier
        rows: int, optional
            Number of rows in the dataset
        row_counter: int, optional
            Counter to generate unique row identifier
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components
        """
        super(FileSystemDatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=row_count,
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations
        )
        self.datafile = datafile

    @staticmethod
    def from_file(filename, datafile, annotations=None):
        """Read dataset from file. Expects the file to be in Json format which
        is the default serialization format used by to_file().

        Parameters
        ----------
        filename: string
            Name of the file to read.
        datafile: string
            Path to the file that contains the dataset rows. The data is stored
            in Json format.
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components
        Returns
        -------
        vizier.datastore.base.Dataset
        """
        with open(filename, 'r') as f:
            doc = json.loads(f.read())
        return FileSystemDatasetHandle(
            identifier=doc['id'],
            columns=[
                DatasetColumn.from_dict(col) for col in doc['columns']
            ],
            datafile=datafile,
            row_count=doc['rows'],
            column_counter=doc['columnCounter'],
            row_counter=doc['rowCounter'],
            annotations=annotations
        )

    def get_annotations(self, column_id=-1, row_id=-1):
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
        annos = self.annotations.for_object(column_id=column_id, row_id=row_id)
        return annos.values()

    def reader(self, offset=0, limit=-1):
        """Get reader for the dataset to access the dataset rows. The optional
        offset amd limit parameters are used to retrieve only a subset of
        rows.

        Parameters
        ----------
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.

        Returns
        -------
        vizier.datastore.reader.DefaultJsonDatasetReader
        """
        return DefaultJsonDatasetReader(
            self.datafile,
            columns=self.columns,
            offset=offset,
            limit=limit,
            annotations=self.annotations
        )

    def to_file(self, filename):
        """Write dataset handle to file. The default serialization format is
        Json. The datafile information is not written to disk. The intention is
        to make it easier for Vizier data folders to be moved around.

        Parameters
        ----------
        filename: string
            Name of the file to write
        """
        doc = {
            'id': self.identifier,
            'columns': [col.to_dict() for col in self.columns],
            'rows': self.row_count,
            'columnCounter': self.column_counter,
            'rowCounter': self.row_counter
        }
        with open(filename, 'w') as f:
            json.dump(doc, f)


class FileSystemDatastore(Datastore):
    """Implementation of Vizier data store. Uses the file system to maintain
    datasets.
    """
    def __init__(self, properties):
        """Initialize the base directory that contains datasets. Each dataset is
        maintained in a separate sub-folder.

        Parameters
        ---------
        properties : dict()
            Dictionary with configuration parameters. The only parameter that
            currently is expected is 'directory'
        """
        super(FileSystemDatastore, self).__init__(
            build_info('FileSystemDatastore')
        )
        if not PARA_DIRECTORY in properties:
            raise ValueError('missing parameter \'' + PARA_DIRECTORY + '\'')
        self.base_dir = os.path.abspath(properties[PARA_DIRECTORY])
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)

    def create_dataset(
        self, identifier=None, columns=None, rows=None, column_counter=None,
        row_counter=None, annotations=None
    ):
        """Create a new dataset in the data store for the given data.

        Raises ValueError if (1) any of the column or row identifier have a
        negative value, or (2) if the given column or row counter have value
        lower or equal to any of the column or row identifier.

        Parameters
        ----------
        identifier: string, optional
            Unique dataset identifier
        columns: list(vizier.datastore.base.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.base.DatasetRow)
            List of dataset rows.
        column_counter: int, optional
            Counter to generate unique column identifier
        row_counter: int, optional
            Counter to generate unique row identifier
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.fs.FileSystemDatasetHandle
        """
        # Set columns and rows if not given
        if columns is None:
            columns = list()
        if rows is None:
            rows = list()
        else:
            # Validate the number of values in the given rows
            validate_schema(columns, rows)
        # Validate that all column identifier are smaller that the given
        # column counter
        if not column_counter is None:
            for col in columns:
                if col.identifier >= column_counter:
                    raise ValueError('invalid column counter')
        else:
            # Set column counter to max. column identifier + 1
            column_counter = -1
            for col in columns:
                if col.identifier > column_counter:
                    column_counter = col.identifier
            column_counter += 1
        # Validate that all row ids are non-negative, unique, lower that the
        # given row_counter
        max_rowid = -1
        row_ids = set()
        for row in rows:
            if row.identifier < 0:
                raise ValueError('invalid row identifier \'' + str(row.identifier) + '\'')
            elif not row_counter is None and row.identifier >= row_counter:
                raise ValueError('invalid row counter')
            elif row.identifier in row_ids:
                raise ValueError('duplicate row identifier \'' + str(row.identifier) + '\'')
            row_ids.add(row.identifier)
            if row_counter is None and row.identifier > max_rowid:
                max_rowid = row.identifier
        if row_counter is None:
            row_counter = max_rowid + 1
        # Get new identifier and create directory for new dataset
        identifier = get_unique_identifier()
        dataset_dir = self.get_dataset_dir(identifier)
        os.makedirs(dataset_dir)
        # Write rows to data file
        datafile = os.path.join(dataset_dir, DATA_FILE)
        DefaultJsonDatasetReader(datafile).write(rows)
        # Create dataset an write dataset file
        dataset = FileSystemDatasetHandle(
            identifier=identifier,
            columns=columns,
            row_count=len(rows),
            datafile=datafile,
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations
        )
        dataset.to_file(os.path.join(dataset_dir, HANDLE_FILE))
        # Write metadata file
        dataset.annotations.to_file(os.path.join(dataset_dir, METADATA_FILE))
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
        dataset_dir = self.get_dataset_dir(identifier)
        if os.path.isdir(dataset_dir):
            shutil.rmtree(dataset_dir)
            return True
        return False

    def get_dataset_dir(self, identifier):
        """Get the base directory for a dataset with given identifier. Having a
        separate method will make future changes to the folder structure that is
        used to store datasets easire.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.base_dir, identifier)

    def get_dataset(self, identifier):
        """Read a full dataset from the data store. Returns None if no dataset
        with the given identifier exists.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        dataset_dir = self.get_dataset_dir(identifier)
        datafile = os.path.join(dataset_dir, DATA_FILE)
        if os.path.isdir(dataset_dir):
            return FileSystemDatasetHandle.from_file(
                filename=os.path.join(dataset_dir, HANDLE_FILE),
                datafile=os.path.join(dataset_dir, DATA_FILE),
                annotations=DatasetMetadata.from_file(
                    os.path.join(dataset_dir, METADATA_FILE)
                )
            )
        return None

    def load_dataset(self, f_handle):
        """Create a new dataset from a given file.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle
            handle for an uploaded file on the associated file server.

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        dataset = InMemDatasetHandle.from_file(f_handle)
        return self.create_dataset(
            columns=dataset.columns,
            rows=dataset.fetch_rows(),
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter
        )

    def update_annotation(self, identifier, column_id=-1, row_id=-1, anno_id=-1, key=None, value=None):
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
        dataset_dir = self.get_dataset_dir(identifier)
        if not os.path.isdir(dataset_dir):
            return None
        # Read annotations from file, evaluate update statement and write result
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
