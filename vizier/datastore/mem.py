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

"""Main memory based data store. Datasets are not stored permanently. This
implementation is primarily for test purposes.
"""

import csv

from vizier.core.system import build_info
from vizier.core.util import cast, get_unique_identifier
from vizier.datastore.base import DatasetHandle, DatasetColumn, DatasetRow
from vizier.datastore.base import DataStore, max_column_id, max_row_id
from vizier.datastore.base import validate_schema
from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.reader import InMemDatasetReader


class InMemDatasetHandle(DatasetHandle):
    """Handle for a dataset that are kept in memory."""
    def __init__(
        self, identifier, columns, rows, column_counter=0, row_counter=0,
        annotations=None
    ):
        """Initialize the dataset handle.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.
        columns: list(vizier.datastore.base.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        rows: list(vizier.datastore.base.DatasetRow)
            List of rows in the dataset
        column_counter: int, optional
            Counter to generate unique column identifier
        row_counter: int, optional
            Counter to generate unique row identifier
        annotations: vizier.datastore.metadata.DatasetMetadata, optional
            Annotations for dataset components
        """
        super(InMemDatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=len(rows),
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations
        )
        self.datarows = rows

    @staticmethod
    def from_file(f_handle):
        """Read dataset from file. Expects the file to be in Json format which
        is the default serialization format used by to_file().

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle
            Handle for an uploaded file on a file server

        Returns
        -------
        vizier.datastore.base.Dataset
        """
        # Expects a CSV/TSV file. The first row contains the column names.
        # Read all information and return a InMemDatasetHandle
        if not f_handle.is_verified_csv:
            raise ValueError('failed to create dataset from file \'' + f_handle.name + '\'')
        # Read all information and return a InMemDatasetHandle
        columns = []
        rows = []
        with f_handle.open() as csvfile:
            reader = csv.reader(csvfile, delimiter=f_handle.delimiter)
            for col_name in reader.next():
                columns.append(DatasetColumn(len(columns), col_name.strip()))
            for row in reader:
                values = [cast(v.strip()) for v in row]
                rows.append(DatasetRow(len(rows), values))
        # Return InMemDatasetHandle
        return InMemDatasetHandle(
            identifier=get_unique_identifier(),
            columns=columns,
            rows=rows,
            column_counter=len(columns),
            row_counter=len(rows)
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
        vizier.datastore.reader.InMemDatasetReader
        """
        # Select the set of dataset rows for the reader depending on whether
        # offset or limit arguments are given.
        if offset > 0 or limit > 0:
            rows = list()
            skip = offset
            for row in self.datarows:
                if skip > 0:
                    skip -= 1
                else:
                    rows.append(row)
                    # Update cell annotation flags
                    for i in range(len(self.columns)):
                        col = self.columns[i]
                        has_anno = self.annotations.has_cell_annotation(
                            col.identifier,
                            row.identifier
                        )
                        row.cell_annotations[i] = has_anno
                    if limit > 0 and len(rows) >= limit:
                        break
        else:
            rows = self.datarows
        return InMemDatasetReader(rows)


class InMemDataStore(DataStore):
    """Non-persistent implementation of data store. Maintains a dictionary of
    datasets in main memory. Data is not stored persistently.
    """
    def __init__(self):
        """Initialize the build information and data store dictionary."""
        super(InMemDataStore, self).__init__(build_info('InMemDataStore'))
        self.datasets = dict()

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
        vizier.datastore.mem.InMemDatasetHandle
        """
        # Set columns and rows if not given
        if columns is None:
            columns = list()
        if rows is None:
            rows = list()
        else:
            # Validate the number of values in the given rows
            validate_schema(columns, rows)
        # Validate the given dataset schema. Will raise ValueError in case of
        # schema violations
        if identifier is None:
            identifier = get_unique_identifier()
        if column_counter is None:
            column_counter = max_column_id(columns) + 1
        if row_counter is None:
            row_counter = max_row_id(rows)
        # Make sure annotation sis not None
        if annotations is None:
            annotations = DatasetMetadata()
        self.datasets[identifier] = InMemDatasetHandle(
            identifier=identifier,
            columns=list(columns),
            rows=list(rows),
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations.copy_metadata()
        )
        return self.datasets[identifier]

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
        if identifier in self.datasets:
            del self.datasets[identifier]
            return True
        else:
            return False

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
        if identifier in self.datasets:
            dataset = self.datasets[identifier]
            return InMemDatasetHandle(
                identifier=identifier,
                columns=[
                    DatasetColumn(col.identifier, col.name)
                        for col in dataset.columns
                ],
                rows=[
                    DatasetRow(row.identifier, list(row.values))
                        for row in dataset.fetch_rows()
                ],
                column_counter=dataset.column_counter,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations.copy_metadata()
            )

    def load_dataset(self, f_handle):
        """Create a new dataset from a given file.

        Raises ValueError if the given file could not be loaded as a dataset.

        Parameters
        ----------
        f_handle : vizier.filestore.base.FileHandle
            Handle for an uploaded file on a file server.

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        dataset = InMemDatasetHandle.from_file(f_handle)
        return self.create_dataset(
            identifier=dataset.identifier,
            columns=dataset.columns,
            rows=dataset.fetch_rows(),
            column_counter=dataset.column_counter,
            row_counter=dataset.row_counter,
            annotations=dataset.annotations
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
        if not identifier in self.datasets:
            return None
        # Get object annotations
        annotations = self.datasets[identifier].annotations
        obj_annos = annotations.for_object(column_id=column_id, row_id=row_id)
        # Update annotation and return result
        return obj_annos.update(identifier=anno_id, key=key, value=value)


class VolatileDataStore(DataStore):
    """Non-persistent implementation of data store that reads datasets from
    an existing data store.

    This data store is primarily used to re-execute non-cached Python modules
    without manipulating the underlying persistent data store. Updates to
    datasets are kept in main-memory and discarded when the object is destroyed.
    """
    def __init__(self, datastore):
        """Initialize the build information and data store dictionary.

        Parameters
        ----------
        datastore: vizier.datastore.base.DataStore
            Existing data store containing the database state.
        """
        super(VolatileDataStore, self).__init__(build_info('VolatileDataStore'))
        self.datastore = datastore
        self.mem_store = InMemDataStore()
        self.deleted_datasets = set()

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
        vizier.datastore.mem.InMemDatasetHandle
        """
        return self.mem_store.create_dataset(
            identifier=identifier,
            columns=columns,
            rows=rows,
            column_counter=column_counter,
            row_counter=row_counter,
            annotations=annotations
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
        if identifier in self.mem_store.datasets:
            return self.mem_store.delete_dataset(identifier)
        elif not identifier in self.deleted_datasets:
            if not self.datastore.get_dataset(identifier) is None:
                self.deleted_datasets.add(identifier)
                return True
        return False

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
        if identifier in self.deleted_datasets:
            return None
        else:
            ds = self.mem_store.get_dataset(identifier)
            if not ds is None:
                return ds
            else:
                return self.datastore.get_dataset(identifier)
