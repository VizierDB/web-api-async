# Copyright (C) 2018 New York University,
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

"""The vizual API defines an interface to execute command in the vizual package
against a (persistent) datastore. The engine allows manipulation of datasets
via VizUAL commands.

Each vizual command will create a new dataset instance on success.
"""

from abc import abstractmethod

from vizier.viztrail.module import ModuleOutputs, TextOutput


class VizualApiResult(object):
    """Each vizual API method returns a result that contains the descriptor for
    the newly created dataset and the standard error and standard output
    streams. The result may also contain a dictionary that defines additional
    resources and other information generated during execution which will become
    part of the module provenance and available when a task is re-executed. The
    resources dictionary should represent simple key,value pairs where values
    are scalars or lists or dictionaries.

    If the dataset descriptor is None it is assumed that the respective command
    failed and returned an error.
    """
    def __init__(self, dataset=None, stdout=None, stderr=None, resources=None):
        """Initialize the API result components.

        Parameters
        ----------
        dataset: vizier.datastore.dataset.DatasetDescriptor
            Descriptor for the generated dataset (on success)
        stdout: list(string), optional
            Lines in the standard output stream
        stderr: list(string), optional
            Lines in the standard error stream
        resources: dict, optional
            Resources generated during execution as part of the result
            provenance
        """
        self.dataset = dataset
        self.outputs = ModuleOutputs(
            stdout=[TextOutput(line) for line in stdout] if not stdout is None else None,
            stderr=[TextOutput(line) for line in stderr] if not stderr is None else None
        )
        self.resources = resources

    @property
    def _is_error(self):
        """True if the result was an error (indicated by a missing descriptor
        for the resulting dataset).

        Returns
        -------
        bool
        """
        return self.dataset is None

    @property
    def is_success(self):
        """True if the result was a success (indicated by a given descriptor
        for the resulting dataset).

        Returns
        -------
        bool
        """
        return not self.dataset is None


class VizualApi(object):
    """Abstract interface to Vizual engine that allows manipulation of datasets
    via VizUAL commands. There may be various implementations of this interface
    for different storage backends.
    """
    @abstractmethod
    def delete_column(self, identifier, column_id):
        """Delete a column in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column_id: int
            Unique column identifier

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def delete_row(self, identifier, row):
        """Delete a row in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row: int
            Row index for deleted row

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def filter_columns(self, identifier, columns, names):
        """Dataset projection operator. Returns a copy of the dataset with the
        given identifier that contains only those columns listed in columns.
        The list of names contains optional new names for the filtered columns.
        A value of None in names indicates that the name of the corresponding
        column is not changed.

        Raises ValueError if no dataset with given identifier exists or if any
        of the filter columns are unknown.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(int)
            List of column identifier for columns in the result.
        names: list(string)
            Optional new names for filtered columns.

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def insert_column(self, identifier, position, name=None):
        """Insert column with given name at given position in dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column position is outside of the current schema bounds, or if
        the column name is invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        position: int
            Index position at which the column will be inserted
        name: string, optional
            New column name

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def insert_row(self, identifier, position):
        """Insert row at given position in a dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row psotion isoutside the dataset bounds.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        position: int
            Index position at which the row will be inserted

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def load_dataset(
        self, file_id=None, uri=None, username=None, password=None,
        resources=None, reload=False
    ):
        """Create (or load) a new dataset from a given file or Uri. It is
        guaranteed that either the file identifier or the uri are not None but
        one of them will be None. The user name and password may only be given
        if an uri is given.

        The resources refer to any resoures (e.g., file identifier) that have
        been generated by a previous execution of the respective task. This
        allows to associate an identifier with a downloaded file to avoid future
        downloads (unless the reload flag is True).

        Parameters
        ----------
        file_id: string, optional
            Identifier for a file in an associated filestore
        uri: string, optional
            Identifier for a web resource
        username: string, optional
            User name for authentication when accessing restricted resources
        password: string, optional
            Password for authentication when accessing restricted resources
        resources: dict, optional
            Dictionary of additional resources (i.e., key,value pairs) that were
            generated during a previous execution of the associated module
        reload: bool, optional
            Flag to force download of a remote resource even if it was
            downloaded previously
            
        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def move_column(self, identifier, column, position):
        """Move a column within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown or the target position invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column: int
            Unique column identifier
        position: int
            Target position for the column

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def move_row(self, identifier, row, position):
        """Move a row within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified row or position is not within the range of the dataset.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        row: int
            Row index for deleted row
        position: int
            Target position for the row

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def rename_column(self, identifier, column, name):
        """Rename column in a given dataset.

        Raises ValueError if no dataset with given identifier exists, if the
        specified column is unknown, or if the given column name is invalid.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        column: int
            Unique column identifier
        name: string
            New column name

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def sort_dataset(self, identifier, columns, reversed):
        """Sort the dataset with the given identifier according to the order by
        statement. The order by statement is a pair of lists. The first list
        contains the identifier of columns to sort on. The second list contains
        boolean flags, one for each entry in columns, indicating whether sort
        order is revered for the corresponding column or not.

        Returns the number of rows in the dataset and the identifier of the
        sorted dataset.

        Raises ValueError if no dataset with given identifier exists or if any
        of the columns in the order by clause are unknown.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(int)
            List of column identifier for sort columns.
        reversed: list(bool)
            Flags indicating whether the sort order of the corresponding column
            is reveresed.

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError

    @abstractmethod
    def update_cell(self, identifier, column, row, value):
        """Update a cell in a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified cell is outside of the current dataset ranges.

        Parameters
        ----------
        identifier : string
            Unique dataset identifier
        column: int
            Unique column identifier for updated cell
        row: int
            Row index for updated cell (starting at 0)
        value: string
            New cell value

        Returns
        -------
        vizier.engine.packages.vizual.api.VizualApiResult
        """
        raise NotImplementedError