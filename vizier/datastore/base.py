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

"""Vizier DB - Database - Collection of objects and methods to maintain and
manipulate different versions of datasets that are manipulated by data curation
workflows.
"""

from abc import abstractmethod
import csv
import yaml

from vizier.core.system import component_descriptor, VizierSystemComponent
from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.query import DataStreamConsumer


"""Unique identifier for datastore types."""
DATASTORE_DEFAULT = 'DEFAULT'
DATASTORE_MIMIR = 'MIMIR'


# ------------------------------------------------------------------------------
# Datsets
# ------------------------------------------------------------------------------

class DatasetHandle(object):
    """Abstract class to maintain information about a dataset in a Vizier
    datastore. Contains the unique dataset identifier, the lists of
    columns in the dataset schema, and a reference to the dataset
    annotations.

    The dataset reader is dependent on the different datastore
    implementations.

    Attributes
    ----------
    annotations: vizier.datastore.metadata.DatasetMetadata
        Annotations for dataset components
    columns: list(DatasetColumns)
        List of dataset columns
    column_counter: int
        Counter to generate unique column identifier
    row_count: int
        Number of rows in the dataset
    row_counter: int
        Counter to generate unique row identifier
    identifier: string
        Unique dataset identifier
    """
    def __init__(self, identifier, columns, row_count=0, column_counter=0, row_counter=0, annotations=None):
        """Initialize the dataset.

        Raises ValueError if dataset columns or rows do not have unique
        identifiers.

        Parameters
        ----------
        identifier: string, optional
            Unique dataset identifier.
        columns: list(DatasetColumn), optional
            List of columns. It is expected that each column has a unique
            identifier.
        rows: int, optional
            Number of rows in the dataset
        column_counter: int, optional
            Counter to generate unique column identifier
        row_counter: int, optional
            Counter to generate unique row identifier
        annotations: vizier.datastore.metadata.DatasetMetadata
            Annotations for dataset components
        """
        self.identifier = identifier
        # Ensure that all columns have a unique identifier
        ids = set()
        for col in columns:
            if col.identifier in ids:
                raise ValueError('duplicate column identifier \'' + str(col.identifier) + '\'')
            elif col.identifier < 0:
                raise ValueError('invalid column identifier \'' + str(col.identifier) + '\'')
            ids.add(col.identifier)
        self.columns = columns
        # Row count
        self.row_count = row_count
        # Column and row counter
        self.column_counter = column_counter
        self.row_counter = row_counter
        # Set the dataset annotations. If no annotations were given
        # create an empty annotation set
        self.annotations = annotations if not annotations is None else DatasetMetadata()
        # Create an internal index of columns by identifier
        self.column_id_index = dict()
        for col in self.columns:
            self.column_id_index[col.identifier] = col

    def column_by_id(self, identifier):
        """Get the column with the given identifier.

        Raises ValueError if no column with the given indentifier exists.

        Parameters
        ----------
        identifier: int
            Unique column identifier

        Returns
        -------
        vizier.datastore.base.DatasetColumn
        """
        if identifier in self.column_id_index:
            return self.column_id_index[identifier]
        else:
            # The column id index might be out of data. Search for column in
            # column list
            for col in self.columns:
                if col.identifier == identifier:
                    return col
        raise ValueError('unknown column \'' + str(identifier) + '\'')

    def column_by_name(self, name, ignore_case=True):
        """Returns the first column with a matching name. The result is None if
        no matching column is found.

        Parameters
        ----------
        name: string
            Column name
        ignore_case: bool, optional
            Ignore case in name comparison if True

        Returns
        -------
        vizier.datastore.base.DatasetColumn
        """
        for col in self.columns:
            if ignore_case:
                if name.upper() == col.name.upper():
                    return col
            elif name == col.name:
                return col

    def column_index(self, column_id):
        """Get position of a given column in the dataset schema. The given
        column identifier could either be of type int (i.e., the index position
        of the column), or a string (either the column name or column label). If
        column_id is of type string it is first assumed to be a column name.
        Only if no column matches the column name or if multiple columns with
        the given name exist will the value of column_id be interpreted as a
        label.

        Raises ValueError if column_id does not reference an existing column in
        the dataset schema.

        Parameters
        ----------
        column_id : int or string
            Column index, name, or label

        Returns
        -------
        int
        """
        return get_column_index(self.columns, column_id)

    def fetch_rows(self, offset=0, limit=-1):
        """Get list of dataset rows. The offset and limit parameters are
        intended for pagination.

        Parameters
        ----------
        offset: int, optional
            Number of rows at the beginning of the list that are skipped.
        limit: int, optional
            Limits the number of rows that are returned.

        Result
        ------
        list(vizier.dataset.base.DatasetRow)
        """
        # Return empty list for special case that limit is 0
        if limit == 0:
            return list()
        # Collect rows in result list. Skip first rows if offset is greater than
        # zero
        rows = list()
        with self.reader(offset=offset, limit=limit) as reader:
            for row in reader:
                rows.append(row)
        return rows

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        vizier.datastore.reader.DatasetReader
        """
        raise NotImplementedError


class DatasetColumn(object):
    """Column in a dataset. Each column has a unique identifier and a
    column name. Column names are not necessarily unique within a
    dataset.

    Attributes
    ----------
    identifier: int
        Unique column identifier
    name: string
        Column name
    """
    def __init__(self, identifier=None, name=None):
        """Initialize the column object.

        Parameters
        ----------
        identifier: int, optional
            Unique column identifier
        name: string, optional
            Column name
        """
        self.identifier = identifier if not identifier is None else -1
        self.name = name

    def __str__(self):
        """Human-readable string representation for the column.

        Returns
        -------
        string
        """
        return self.name

    @staticmethod
    def from_dict(obj):
        """Create dataset column instance from a given dictionary serialization.

        Returns
        -------
        vizier.datastore.base.DatasetColumn
        """
        return DatasetColumn(int(obj['id']), obj['name'])

    def to_dict(self):
        """Dictionary serialization of the dataset column object.

        Returns
        -------
        dict
        """
        return {'id': self.identifier, 'name': self.name}


class DatasetRow(object):
    """Row in a Vizier DB dataset.

    Attributes
    ----------
    identifier: int
        Unique row identifier
    values : list(string)
        List of column values in the row
    """
    def __init__(self, identifier=None, values=None, annotations=None):
        """Initialize the row object.

        Parameters
        ----------
        identifier: int, optional
            Unique row identifier
        values : list(string), optional
            List of column values in the row
        """
        self.identifier = identifier if not identifier is None else -1
        self.values = values
        self.cell_annotations = annotations if not annotations is None else [False] * len(values)

    @staticmethod
    def from_dict(obj):
        """Create dataset row instance from a given dictionary serialization.

        Returns
        -------
        vizier.datastore.base.DatasetRow
        """
        return DatasetRow(
            int(obj['id']),
            obj['values']
        )

    def to_dict(self):
        """Dictionary serialization of the dataset row object.

        Returns
        -------
        dict
        """
        return {
            'id': self.identifier,
            'values': encode_values(self.values)
        }


# ------------------------------------------------------------------------------
# Datastore
# ------------------------------------------------------------------------------

class Datastore(VizierSystemComponent):
    """Abstract API to store and retireve Vizier datasets."""
    def __init__(self, build):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        build : dict()
            Build information
        """
        super(Datastore, self).__init__(build)

    def components(self):
        """List containing component descriptor.

        Returns
        -------
        list
        """
        return [component_descriptor('datastore', self.system_build())]

    @abstractmethod
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
        vizier.datastore.base.DatasetHandle
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    def get_dataset_chart(self, identifier, view):
        """Query a given dataset by selecting the columns in the given list.
        Each row in the result is the result of projecting a tuple in the
        dataset on the given columns.

        Raises ValueError if any of the specified columns do not exist.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        view: vizier.plot.view.ChartViewHandle
            Chart view definition handle

        Returns
        -------
        list()
        """
        dataset = self.get_dataset(identifier)
        # Get index position for x-axis. Set to negative value if none is given.
        # the value is used to determine which data series are converted to
        # numeric values and which are not.
        x_axis = -1
        if not view.x_axis is None:
            x_axis = view.x_axis
        # Create a list of data consumers, one for each data series
        consumers = list()
        for s_idx in range(len(view.data)):
            s = view.data[s_idx]
            c_idx = get_index_for_column(dataset, s.column)
            consumers.append(
                DataStreamConsumer(
                    column_index=c_idx,
                    range_start=s.range_start,
                    range_end=s.range_end,
                    cast_to_number=(s_idx != x_axis)
                )
            )
        # Consume all dataset rows
        rows = dataset.fetch_rows()
        for row_index in range(len(rows)):
            row = rows[row_index]
            for c in consumers:
                c.consume(row=row, row_index=row_index)
        # the size of the result set is determined by the longest data series
        max_values = -1
        for c in consumers:
            if len(c.values) > max_values:
                max_values = len(c.values)
        # Create result array
        data = []
        for idx_row in range(max_values):
            row = list()
            for idx_series in range(len(consumers)):
                consumer = consumers[idx_series]
                if idx_row < len(consumer.values):
                    row.append(consumer.values[idx_row])
                else:
                    row.append(None)
            data.append(row)
        return data

    @abstractmethod
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
        raise NotImplementedError


# ------------------------------------------------------------------------------
#
# Helper Methods
#
# ------------------------------------------------------------------------------

def encode_values(values):
    """Encode a given list of cell values into utf-8 format.

    Parameters
    ----------
    values: list(string)

    Returns
    -------
    list(string)
    """
    result = list()
    for val in values:
        if isinstance(val, basestring):
            try:
                result.append(val.encode('utf-8'))
            except UnicodeDecodeError as ex:
                try:
                    result.append(val.decode('cp1252').encode('utf-8'))
                except UnicodeDecodeError as ex:
                    result.append(val.decode('latin1').encode('utf-8'))
        else:
            result.append(val)
    return result


def collabel_2_index(label):
    """Convert a column label into a column index (based at 0), e.g., 'A'-> 1,
    'B' -> 2, ..., 'AA' -> 27, etc.

    Returns -1 if the given labe is not composed only of upper case letters A-Z.
    Parameters
    ----------
    label : string
        Column label (expected to be composed of upper case letters A-Z)

    Returns
    -------
    int
    """
    # The following code is adopted from
    # https://stackoverflow.com/questions/7261936/convert-an-excel-or-spreadsheet-column-letter-to-its-number-in-pythonic-fashion
    num = 0
    for c in label:
        if ord('A') <= ord(c) <= ord('Z'):
            num = num * 26 + (ord(c) - ord('A')) + 1
        else:
            return -1
    return num


def get_column_index(columns, column_id):
    """Get position of a column in a given column list. The column identifier
    can either be of type int (i.e., the index position of the column in the
    given list), or a string (either the column name or column label). If
    column_id is of type string it is first assumed to be a column name.
    Only if no column matches the column name or if multiple columns with
    the given name exist will the value of column_id be interpreted as a
    label.

    Raises ValueError if column_id does not reference an existing column in
    the dataset schema.

    Parameters
    ----------
    columns: list(vizier.datastore.base.DatasetColumn)
        List of columns in a dataset schema
    column_id : int or string
        Column index, name, or label

    Returns
    -------
    int
    """
    if isinstance(column_id, int):
        # Return column if it is a column index and withing the range of
        # dataset columns
        if column_id >= 0 and column_id < len(columns):
            return column_id
        raise ValueError('invalid column index \'' + str(column_id) + '\'')
    elif isinstance(column_id, basestring):
        # Get index for column that has a name that matches column_id. If
        # multiple matches are detected column_id will be interpreted as a
        # column label
        name_index = -1
        for i in range(len(columns)):
            col_name = columns[i].name
            if col_name.lower() == column_id.lower():
                if name_index == -1:
                    name_index = i
                else:
                    # Multiple columns with the same name exist. Signal that
                    # no unique column was found by setting name_index to -1.
                    name_index = -2
                    break
        if name_index < 0:
            # Check whether column_id is a column label that is within the
            # range of the dataset schema
            label_index = collabel_2_index(column_id)
            if label_index > 0:
                if label_index <= len(columns):
                    name_index = label_index - 1
        # Return index of column with matching name or label if there exists
        # a unique solution. Otherwise raise exception.
        if name_index >= 0:
            return name_index
        elif name_index == -1:
            raise ValueError('unknown column \'' + str(column_id) + '\'')
        else:
            raise ValueError('not a unique column name \'' + str(column_id) + '\'')


def get_index_for_column(dataset, col_id):
    """Get index position for column with given id in dataset schema.

    Raises ValueError if no column with col_id exists.

    Parameters
    ----------
    dataset: vizier.datastore.base.DatasetHandle
        Handle for dataset
    col_id: int
        Unique column identifier

    Returns
    -------
    int
    """
    for i in range(len(dataset.columns)):
        if dataset.columns[i].identifier == col_id:
            return i
    # Raise ValueError if no column was found
    raise ValueError('unknown column identifier \'' + str(col_id) + '\'')


def max_column_id(columns):
    """Return maximum identifier for a list of columns.

    Parameters
    ----------
    columns: list(vizier.datastore.base.DatasetColumn)
        List of dataset columns

    Returns
    -------
    int
    """
    return max_object_id(columns)


def max_object_id(objects):
    """Return maximum identifier for a list of identifiable objects.

    Parameters
    ----------
    object: list()
        List of dataset columns or rows

    Returns
    -------
    int
    """
    max_id = -1
    for obj in objects:
        if obj.identifier > max_id:
            max_id = obj.identifier
    return max_id


def max_row_id(rows):
    """Return maximum identifier for a list of rows.

    Parameters
    ----------
    rows: list(vizier.datastore.base.DataseRow)
        List of dataset rows

    Returns
    -------
    int
    """
    return max_object_id(rows)


def validate_schema(columns, rows):
    """Validate that the given set of rows contains exactly one value for each
    column in a dataset schema.

    Raises ValueError in case of a schema violation.
    """
    for i in range(len(rows)):
        row = rows[i]
        if len(row.values) != len(columns):
            raise ValueError('schema violation for row \'' + str(i) + '\'')
