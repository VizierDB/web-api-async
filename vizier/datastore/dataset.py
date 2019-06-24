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

"""Vizier DB - Database - Collection of objects and methods to maintain and
manipulate different versions of datasets that are manipulated by data curation
workflows.
"""

from abc import abstractmethod

from vizier.datastore.annotation.dataset import DatasetMetadata


"""Identifier for column data types. By now the following data types are
distinguished: date (format yyyy-MM-dd), int, varchar, real, and datetime
(format yyyy-MM-dd hh:mm:ss:zzzz).
"""
DATATYPE_DATE = 'date'
DATATYPE_DATETIME = 'datetime'
DATATYPE_INT = 'int'
DATATYPE_REAL = 'real'
DATATYPE_VARCHAR = 'varchar'

COLUMN_DATATYPES = [
    DATATYPE_DATE,
    DATATYPE_DATETIME,
    DATATYPE_INT,
    DATATYPE_REAL,
    DATATYPE_VARCHAR
]


class DatasetColumn(object):
    """Column in a dataset. Each column has a unique identifier and a
    column name. Column names are not necessarily unique within a dataset.

    Attributes
    ----------
    identifier: int
        Unique column identifier
    name: string
        Column name
    data_type: string, optional
        String representation of the column type in the database. By now the
        following data_type values are expected: date (format yyyy-MM-dd), int,
        varchar, real, and datetime (format yyyy-MM-dd hh:mm:ss:zzzz).
    """
    def __init__(self, identifier=None, name=None, data_type=None):
        """Initialize the column object.

        Parameters
        ----------
        identifier: int, optional
            Unique column identifier
        name: string, optional
            Column name
        data_type: string, optional
            String representation of the column data type.
        """
        self.identifier = identifier if not identifier is None else -1
        self.name = name
        self.data_type = data_type if not data_type is None else DATATYPE_VARCHAR

    def __str__(self):
        """Human-readable string representation for the column.

        Returns
        -------
        string
        """
        name = self.name
        if not self.data_type is None:
             name += '(' + str(self.data_type) + ')'
        return name


class DatasetDescriptor(object):
    """The descriptor maintains the dataset schema and basic information
    including the dataset identifier and row count.

    Attributes
    ----------
    columns: list(DatasetColumns)
        List of dataset columns
    identifier: string
        Unique dataset identifier
    row_count: int
        Number of rows in the dataset
    """
    def __init__(self, identifier, columns=None, row_count=None):
        """Initialize the dataset descriptor.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.
        columns: list(DatasetColumn), optional
            List of columns.
        row_count: int, optional
            Number of rows in the dataset
        """
        self.identifier = identifier
        self.columns = columns if not columns is None else list()
        self.row_count = row_count if not row_count is None else 0

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

    def get_index(self, column_id):
        """Get index position for column with given id. Returns None if no
        column with column_id exists.

        Parameters
        ----------
        column_id: int
            Unique column identifier

        Returns
        -------
        int
        """
        for i in range(len(self.columns)):
            if self.columns[i].identifier == column_id:
                return i
        return None

    def get_unique_name(self, name):
        """Get a unique version of the given column name. If no column with the
        given name exists the name itself is returned. Otherwise, we append
        indices (starting at 1) to the name until a non-exisitng name is found.

        Parameters
        ----------
        name: string
             Column name

        Returns
        -------
        string
        """
        names = [c.name.upper() for c in self.columns]
        if not name.upper() in names:
            return name
        index = 1
        while True:
            col_name = name + '_' + str(index)
            if not col_name.upper() in names:
                return col_name
            index += 1

    def max_column_id(self):
        """Get maximum identifier for columns in the dataset schema. If the
        schema is empty the result is -1.

        Returns
        -------
        int
        """
        if len(self.columns) == 0:
            return -1
        else:
            return max([col.identifier for col in self.columns])

    @abstractmethod
    def max_row_id(self):
        """Get maximum identifier for all rows in the dataset. If the dataset
        is empty the result is -1.

        Returns
        -------
        int
        """
        raise NotImplementedError

    def print_schema(self, name):
        """Print dataset schema as a list of lines.

        Parameters
        ----------
        name: string
            Dataset name

        Returns
        -------
        list(string)
        """
        output = [name + ' (']
        for i in range(len(self.columns)):
            col = self.columns[i]
            text = '  ' + col.name + ' ' + col.data_type
            if i != len(self.columns) - 1:
                text += ','
            output.append(text)
        output.append(')')
        return output


class DatasetHandle(DatasetDescriptor):
    """Abstract class to maintain information about a dataset in a Vizier
    datastore. Contains the unique dataset identifier, the lists of
    columns in the dataset schema, and a reference to the dataset
    annotations.

    The dataset reader is dependent on the different datastore
    implementations.

    Attributes
    ----------
    columns: list(DatasetColumns)
        List of dataset columns
    identifier: string
        Unique dataset identifier
    row_count: int
        Number of rows in the dataset
    """
    def __init__(self, identifier, columns=None, row_count=None, annotations=None):
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
        row_count: int, optional
            Number of rows in the dataset
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata, optional
            Annotations for dataset components
        """
        super(DatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=row_count
        )
        self.annotations = annotations if not annotations is None else DatasetMetadata()

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


class DatasetRow(object):
    """Row in a Vizier DB dataset.

    Attributes
    ----------
    identifier: int
        Unique row identifier
    values : list(string)
        List of column values in the row
    annotations: list(bool), optional
        Optional flags indicating whether row cells are annotated
    """
    def __init__(self, identifier=None, values=None, annotations=None):
        """Initialize the row object.

        Parameters
        ----------
        identifier: int, optional
            Unique row identifier
        values : list(string)
            List of column values in the row
        annotations: list(bool), optional
            Optional flags indicating whether row cells are annotated
        """
        self.identifier = identifier if not identifier is None else -1
        self.values = values if not values is None else list()
        self.annotations = annotations if not values is None else list()


# ------------------------------------------------------------------------------
#
# Helper Methods
#
# ------------------------------------------------------------------------------

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
