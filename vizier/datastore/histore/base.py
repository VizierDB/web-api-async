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

"""Data store that maintains the different versions of a dataset in a HISTORE
archive on the file system. When a dataset is first created, a new archive for
that dataset is initialized. Each archive is maintaind in a separate folder.
With each archive metadata is maintained in a separate folder.
"""

import csv
import os
import pandas as pd

from histore.archive.base import PersistentArchive
from histore.document.schema import Column

from vizier.core.util import cast, get_unique_identifier
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.datastore.base import DefaultDatastore
from vizier.datastore.dataset import DatasetColumn
from vizier.datastore.histore.dataset import HistoreSnapshotHandle
from vizier.datastore.histore.metadata import SnapshotMetadata
from vizier.datastore.histore.reader import OnDemandReader
from vizier.datastore.object.dataobject import DataObjectMetadata

import vizier.datastore.histore.identifier as ID
import vizier.datastore.profiling.base as profiling
import vizier.datastore.util as util


class HistoreDatastore(DefaultDatastore):
    """Implementation of Vizier data store. Uses the file system to maintain
    datasets. For each dataset a new subfolder is created. Within the folder
    the dataset information is split across three files containing the
    descriptor, annotation, and the dataset rows.
    """
    def __init__(self, basedir):
        """Initialize the base directory that contains datasets. Each dataset
        is maintained in a separate subfolder.

        Parameters
        ---------
        basedir : string
            Path to base directory for the datastore
        """
        super(HistoreDatastore, self).__init__(basedir)

    def create_archive(
        self, df, profiler, column_types=dict(), annotations=None
    ):
        """Create a new persistent archive that contains the given data frame
        as the first snapshot. Returns a handle for the dataset snapshot.

        Parameters
        ----------
        df: pandas.DataFrame
            Data frame containing the dataset snapshot.
        profiler: string, default=None
            Identifier for data profiler that is used to infer column types.
            If None, all column types will be 'varchar'.
        column_types: dict
            Optional mapping of column names to column types.
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata,
                default=None
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.histore.dataset.HistoreSnapshotHandle
        """
        # Generate data profiling results and column types for the data frame.
        metadata, column_types = profiling.run(
            df=df,
            profiler=profiler,
            column_types=column_types
        )
        # Create a new folder for the archive that maintains the dataset
        archive_id = get_unique_identifier()
        dataset_dir = self.get_dataset_dir(archive_id)
        os.makedirs(dataset_dir)
        # Persist the data frame as the first archive snapshot.
        archive = PersistentArchive(basedir=dataset_dir)
        snapshot = archive.commit(df)
        return self.commit_dataset(
            archive=archive,
            archive_id=archive_id,
            snapshot_id=snapshot.version,
            dataset_dir=dataset_dir,
            df=df,
            schema=list(df.columns),
            metadata=metadata,
            column_types=column_types,
            annotations=annotations
        )

    def commit_dataset(
        self, archive, archive_id, snapshot_id, dataset_dir, df, schema,
        metadata, column_types, annotations
    ):
        """Commit changes to a given archive after merging a dataset snapshot.
        Snapshot metadata and annotations are written to file.

        Parameters
        ----------
        archive: histore.archive.base.Archive
            Archive for dataset snapshots.
        archive_id: string
            Unique dataset archive identifier
        snapshot_id: int
            Dataset snapshot version number.
        dataset_dir: string
            Base directory for all files that are associated with the dataset
            archive.
        df: pandas.DataFrame
            Data frame containing the dataset snapshot.
        schema: list(histore.archive.schema.Column)
            Columns in the commited data frame schema.
        metadata: dict
            Dictionary containing data profiling results.
        column_types: dict
            Mapping of column names to column types.
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata,
                default=None
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.histore.dataset.HistoreSnapshotHandle
        """
        # Create column objects.
        columns = []
        for column in schema:
            columns.append(
                DatasetColumn(
                    identifier=column.colid,
                    name=column,
                    data_type=column_types.get(column)
                )
            )
        # Write annotations if given.
        if annotations is not None:
            annotations.to_file(
                self.get_metadata_filename(archive_id)
            )
        # Write snapshot metadata.
        SnapshotMetadata(dataset_dir).write(snapshot_id, columns, metadata)
        # Create handle for the new dataset snapshot.
        return HistoreSnapshotHandle(
            identifier=ID.create(archive_id, snapshot_id),
            columns=columns,
            row_count=len(df.index),
            reader=OnDemandReader(archive=archive, snapshot_id=snapshot_id),
            profiling=metadata.get(profiling.PROFILING_RESULTS, dict()),
            annotations=annotations
        )

    def create_dataset(
        self, columns, rows, human_readable_name=None, annotations=None,
        backend_options=[], dependencies=[], profiler=None
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
        profiler: string, default=None
            Identifier for data profiler that is used to infer column types.

        Returns
        -------
        vizier.datastore.dataset.DatasetDescriptor
        """
        # Validate (i) that each column has a unique identifier, (ii) each row
        # has a unique identifier, and (iii) that every row has exactly one
        # value per column.
        util.validate_dataset(columns=columns, rows=rows)
        # Convert given columns into type that is expected by HISTORE. Generate
        # mapping of column names to the defined column type.
        df_columns = list()
        column_types = dict()
        for col in columns:
            df_columns.append(Column(colid=col.identifier, name=col.name))
            column_types[col.name] = col.data_type
        # Extract values and identifier from data rows to create the pandas
        # data frame.
        index = list()
        data = list()
        for row in rows:
            index.append(row.identifier)
            data.append(row.values)
        # Create a data frame for the data that was read from the file.
        df = pd.DataFrame(data=data, columns=df_columns)
        # Create archive for dataset snapshot and return the dataset handle.
        return self.create_archive(
            df=df,
            profiler=profiler,
            column_types=column_types,
            annotation=annotations
        )

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
        # Split dataset archive and snapshot identifier. This will raise a
        # ValueError if the identifier is invalid.
        archive_id, snapshot_id = ID.parse(identifier)
        # Test if a folder for the given dataset archive exists. If not
        # return None.
        dataset_dir = self.get_dataset_dir(archive_id)
        if not os.path.isdir(dataset_dir):
            return None
        # Create instance of the archive that maintains dataset snapshots.
        # Returns None if the archive does not have a snapshot with the given
        # identifier.
        archive = PersistentArchive(basedir=dataset_dir)
        if not archive.snapshots().has_version(snapshot_id):
            return None
        # Read the snapshot meatdata.
        columns, metadata = SnapshotMetadata(dataset_dir).read(snapshot_id)
        # Load the dataset handle
        return HistoreSnapshotHandle(
            identifier=identifier,
            columns=columns,
            row_count=metadata[profiling.ROWCOUNT],
            reader=OnDemandReader(archive=archive, snapshot_id=snapshot_id),
            profiling=metadata.get(profiling.PROFILING_RESULTS, dict()),
            annotations=DatasetMetadata.from_file(
                self.get_metadata_filename(archive_id)
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

    def load_dataset(self, fh, profiler=None):
        """Create a new dataset from a given CSV file.

        Parameters
        ----------
        fh : vizier.filestore.base.FileHandle
            Handle for CSV file on the file system.
        profiler: string, default=None
            Identifier for data profiler that is used to infer column types.
            If None, all column types will be 'varchar'.

        Returns
        -------
        vizier.datastore.base.DatasetHandle
        """
        # The file handle might be None in which case an exception is raised
        if fh is None:
            raise ValueError('unknown file')
        # Expects a file in a supported tabular data format.
        if not fh.is_tabular:
            raise ValueError('cannot load dataset from %s ' % (fh.name))
        # Open the file as a csv file. Expects that the first row contains the
        # column names. Read dataset schema and dataset rows into two separate
        # lists.
        df_columns = []
        rows = []
        with fh.open() as csvfile:
            reader = csv.reader(
                csvfile,
                delimiter=fh.delimiter,
                quotechar=fh.quotechar,
                quoting=fh.quoting
            )
            for col_name in next(reader):
                column = Column(colid=len(df_columns), name=col_name.strip())
                df_columns.append(column)
            for row in reader:
                values = [cast(v.strip()) for v in row]
                rows.append(values)
        # Create a data frame for the data that was read from the file.
        df = pd.DataFrame(data=rows, columns=df_columns)
        # Create archive for dataset snapshot and return the dataset handle.
        return self.create_archive(df=df, profiler=profiler)

    def update_dataset(self, origin, df, profiler=None, annotations=None):
        """Create a new persistent archive that contains the given data frame
        as the first snapshot. Returns a handle for the dataset snapshot.

        Parameters
        ----------
        origin : vizier.datastore.histore.dataset.HistoreSnapshotHandle
            Handle for the dataset snapshot from which the given data frame was
            generated.
        df: pandas.DataFrame
            Data frame containing the dataset snapshot.
        profiler: string, default=None
            Identifier for data profiler that is used to infer column types.
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata,
                default=None
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.histore.dataset.HistoreSnapshotHandle
        """
        # Split dataset archive and snapshot identifier. This will raise a
        # ValueError if the identifier is invalid.
        archive_id, snapshot_id = ID.parse(origin.identifier)
        # Test if a folder for the given dataset archive exists. If not
        # raise an error.
        dataset_dir = self.get_dataset_dir(archive_id)
        if not os.path.isdir(dataset_dir):
            raise ValueError("unknown dataset '{}'".format(origin.identifier))
        # Create instance of the archive that maintains dataset snapshots.
        # Raises an error if the archive does not have a snapshot with the
        # given identifier.
        archive = PersistentArchive(basedir=dataset_dir)
        if not archive.snapshots().has_version(snapshot_id):
            raise ValueError("unknown dataset '{}'".format(origin.identifier))
        # Generate data profiling results and column types for the data frame.
        metadata, column_types = profiling.run(df=df, profiler=profiler)
        # Commit the given data frame to the archive.
        snapshot = archive.commit(df, origin=snapshot_id)
        # Make sure to update the data fra,e columns.
        schema = archive.schema().at_version(snapshot.version)
        df.columns = schema
        return self.commit_dataset(
            archive=archive,
            archive_id=archive_id,
            snapshot_id=snapshot.version,
            dataset_dir=dataset_dir,
            df=df,
            schema=schema,
            metadata=metadata,
            column_types=column_types,
            annotations=annotations
        )

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
