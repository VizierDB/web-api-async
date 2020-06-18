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

"""Dataset reader for pandas data frames."""

from abc import ABCMeta, abstractmethod

from vizier.datastore.dataset import DatasetRow
from vizier.datastore.reader import DatasetReader


# -- Data frame reader factory ------------------------------------------------

class ReaderFactory(metaclass=ABCMeta):
    """Factory pattern for data frame readers."""
    @abstractmethod
    def get_dataframe(self):
        """Get pandas data frame containing the full dataset.

        Returns
        -------
        pandas.DataFrame
        """
        raise NotImplementedError()

    @abstractmethod
    def get_reader(self, offset=0, limit=-1):
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
        vizier.datastore.histore.reader.DataFrameReader
        """
        raise NotImplementedError()


class OnDemandReader(ReaderFactory):
    """Factory for reading archive snapshot datasets on demand. Reads the data
    frame from an associated archive when the get_reader method is first
    called.
    """
    def __init__(self, archive, snapshot_id):
        """Initialize the archive and snapshot identifier.

        Parameters
        ----------
        archive: histore.archive.Archive
            HISTORE archive containing dataset snapshots.
        snapshot_id: int
            Unique snapshot version identifier.
        """
        self.archive = archive
        self.snapshot_id = snapshot_id
        # Cache the data frame once it has been read.
        self._df = None

    def get_dataframe(self):
        """Get pandas data frame containing the full dataset.

        Returns
        -------
        pandas.DataFrame
        """
        # Read the data frame for the snapshot if it is not cached.
        if not self._df:
            self._df = self.archive.checkout(version=self.snapshot_id)
        return self._df

    def get_reader(self, offset=0, limit=-1):
        """Get reader for the dataset to access the dataset rows. The optional
        offset amd limit parameters are used to retrieve only a subset of
        rows.

        Reads the data frame for the associated snapshot once when the method
        is called for the first time.

        Parameters
        ----------
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.

        Returns
        -------
        vizier.datastore.histore.reader.DataFrameReader
        """
        df = self.get_dataframe()
        return DataFrameReader(df=df, offset=offset, limit=limit)


# -- Reader for HISTORE datasets ----------------------------------------------

class DataFrameReader(DatasetReader, ReaderFactory):
    """Dataset reader for rows in a pandas DataFrame."""
    def __init__(self, df, offset=0, limit=-1):
        """Initialize the data frame and read offsets.

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas data frame.
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.
        """
        self.df = df
        self.is_open = False
        self.read_index = offset
        if limit > 0:
            self.size = min(offset + limit, len(df.index))
        else:
            self.size = len(df.index)

    def __next__(self):
        """Return the next row in the dataset iterator. Raises StopIteration if
        end of rows are reached or if the reader has been closed.

        Returns
        -------
        vizier.datastore.base.DatasetRow

        Raises
        ------
        StopIteration
        """
        if self.is_open:
            if self.read_index < self.size:
                row = DatasetRow(
                    identifier=self.df.index[self.read_index],
                    values=list(self.df.iloc[self.read_index])
                )
                self.read_index += 1
                return row
        raise StopIteration

    def close(self):
        """Set the is_open flag to False."""
        self.is_open = False

    def get_dataframe(self):
        """Get pandas data frame containing the full dataset.

        Returns
        -------
        pandas.DataFrame
        """
        return self.df

    def get_reader(self, offset=0, limit=-1):
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
        vizier.datastore.histore.reader.DataFrameReader
        """
        return DataFrameReader(df=self.df, offset=offset, limit=limit)

    def open(self):
        """Setup the is_open flag to True.

        Returns
        -------
        vizier.datastore.base.DelimitedFileReader
        """
        # Only open if flag is false. Otherwise, return immediately
        if not self.is_open:
            self.is_open = True
        return self
