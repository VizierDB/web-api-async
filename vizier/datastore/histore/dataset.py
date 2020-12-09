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

"""Implementation of the dataset handle for dataset snapshots that are
maintained in HISTORE archive.
"""

from vizier.datastore.dataset import DatasetDescriptor, DatasetHandle
from typing import Any

import vizier.datastore.profiling.base as profiling


class HistoreSnapshotHandle(DatasetHandle):
    """Handle for a dataset snapshot that is maintained in a HISTORE archive.
    """
    def __init__(
        self, identifier, columns, metadata, annotations, archive, reader
    ):
        """Initialize the dataset handle.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.
        columns: list(vizier.datastore.base.DatasetColumn)
            List of columns. It is expected that each column has a unique
            identifier.
        profiling: dict
            Dictionary mapping profiler names to profiling results.
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata,
                default=None
            Annotations for dataset components
        archive: histore.archive.base.Archive
            Archive that maintains the dataset snapshots.
        reader: vizier.datastore.histore.reader.ReaderFactory
            Factory for readers that provide access to the dataset rows.
        """
        super(HistoreSnapshotHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=metadata.get(profiling.ROWCOUNT),
            annotations=annotations
        )
        self.archive = archive
        self._reader = reader
        self._profiling = metadata.get(profiling.PROFILING_RESULTS, dict())

    def descriptor(self):
        """Get the descriptor for this dataset.

        Returns
        -------
        vizier.datastore.dataset.DatasetDescriptor
        """
        return DatasetDescriptor(
            identifier=self.identifier,
            columns=self.columns,
            row_count=self.row_count
        )

    def get_annotations(self, column_id=None, row_id=None):
        """Get all annotations for a given dataset resource. If both identifier
        are None all dataset annotations are returned.

        Parameters
        ----------
        column_id: int, optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier

        Returns
        -------
        list(vizier.datastpre.annotation.base.DatasetAnnotation)
        """
        if column_id is None and row_id is None:
            return self.annotations.values
        elif row_id is None:
            return self.annotations.for_column(row_id)
        elif column_id is None:
            return self.annotations.for_row(row_id)
        else:
            return self.annotations.for_cell(
                column_id=column_id,
                row_id=row_id
            )

    def get_profiling(self) -> Any:
        """Get profiling results for the dataset.

        Returns
        -------
        dict
        """
        return self._profiling.get(profiling.PROFILER_DATA, dict())

    # Keep this for compatibility reasons.
    profile = get_profiling

    def profiler(self):
        """Get name of data profiler that was executed on the dataset.

        Returns
        -------
        string
        """
        return self._profiling.get(profiling.PROFILER_NAME)

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
        return self._reader.get_reader(offset=offset, limit=limit)

    def to_dataframe(self):
        """Get pandas data frame containing the full dataset.

        Returns
        -------
        pandas.DataFrame
        """
        return self._reader.get_dataframe()
