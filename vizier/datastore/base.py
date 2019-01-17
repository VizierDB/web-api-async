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

import os

from vizier.datastore.annotation.base import DatasetAnnotation
from vizier.datastore.annotation.dataset import DatasetMetadata


"""Metadata file name for datasets in the the default datastore."""
METADATA_FILE = 'annotations.json'


class Datastore(object):
    """Abstract API to store and retireve datasets."""
    @abstractmethod
    def create_dataset(self, columns, rows, annotations=None):
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
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata, optional
            Annotations for dataset components

        Returns
        -------
        vizier.datastore.dataset.DatasetDescriptor
        """
        raise NotImplementedError

    @abstractmethod
    def get_annotations(self, identifier, column_id=None, row_id=None):
        """Get list of annotations for a resources of a given dataset. If only
        the column id is provided annotations for the identifier column will be
        returned. If only the row identifier is given all annotations for the
        specified row are returned. Otherwise, all annotations for the specified
        cell are returned. If both identifier are None all annotations for the
        dataset are returned.

        Parameters
        ----------
        column_id: int, optional
            Unique column identifier
        row_id: int, optiona
            Unique row identifier

        Returns
        -------
        vizier.datastore.annotation.dataset.DatasetMetadata
        """
        raise NotImplementedError

    @abstractmethod
    def get_dataset(self, identifier):
        """Get the handle for the dataset with given identifier from the data
        store. Returns None if no dataset with the given identifier exists.

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

    @abstractmethod
    def update_annotation(
        self, identifier, key, old_value=None, new_value=None, column_id=None,
        row_id=None
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
        identifier : string
            Unique dataset identifier
        column_id: int, optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier
        key: string, optional
            Annotation key
        old_value: string, optional
            Previous annotation value whan updating an existing annotation.
        new_value: string, optional
            Updated annotation value
        Returns
        -------
        bool
        """
        raise NotImplementedError


class DefaultDatastore(Datastore):
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
        # Create the base directory if it does not exist
        self.base_path = os.path.abspath(base_path)
        if not os.path.isdir(self.base_path):
            os.makedirs(self.base_path)

    def get_annotations(self, identifier, column_id=None, row_id=None):
        """Get list of annotations for a resources of a given dataset. If only
        the column id is provided annotations for the identifier column will be
        returned. If only the row identifier is given all annotations for the
        specified row are returned. Otherwise, all annotations for the specified
        cell are returned. If both identifier are None all annotations for the
        dataset are returned.

        Parameters
        ----------
        column_id: int, optional
            Unique column identifier
        row_id: int, optiona
            Unique row identifier

        Returns
        -------
        vizier.datastore.annotation.dataset.DatasetMetadata
        """
        # Test if a subfolder for the given dataset identifier exists. If not
        # return None.
        dataset_dir = self.get_dataset_dir(identifier)
        if not os.path.isdir(dataset_dir):
            return None
        annotations=DatasetMetadata.from_file(
            self.get_metadata_filename(identifier)
        )
        if column_id is None and row_id is None:
            return annotations
        elif column_id is None:
            return DatasetMetadata(rows=annotations.rows).filter(rows=[row_id])
        elif row_id is None:
            return DatasetMetadata(columns=annotations.columns).filter(
                columns=[column_id]
            )
        else:
            return DatasetMetadata(cells=annotations.cells).filter(
                columns=[column_id],
                rows=[row_id]
            )

    def get_dataset_dir(self, identifier):
        """Get the base directory for a dataset with given identifier. Having a
        separate method makes it easier to change the folder structure used to
        store datasets.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.base_path, identifier)

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

    def get_metadata_filename(self, identifier):
        """Get filename of meatdata file for the dataset with the given
        identifier.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier

        Returns
        -------
        string
        """
        return os.path.join(self.get_dataset_dir(identifier), METADATA_FILE)

    def update_annotation(
        self, identifier, key, old_value=None, new_value=None, column_id=None,
        row_id=None
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
        key: string, optional
            Annotation key
        old_value: string, optional
            Previous annotation value whan updating an existing annotation.
        new_value: string, optional
            Updated annotation value

        Returns
        -------
        bool
        """
        # Raise ValueError if column id and row id are both None
        if column_id is None and row_id is None:
            raise ValueError('invalid dataset resource identifier')
        # Return None if the dataset is unknown
        dataset_dir = self.get_dataset_dir(identifier)
        if not os.path.isdir(dataset_dir):
            return None
        # Read annotations from file, Evaluate update statement and write result
        # back to file.
        metadata_filename = self.get_metadata_filename(identifier)
        annotations = DatasetMetadata.from_file(metadata_filename)
        # Get object annotations
        if column_id is None:
            elements = annotations.rows
        elif row_id is None:
            elements = annotations.columns
        else:
            elements = annotations.cells
        # Identify the type of operation: INSERT, DELETE or UPDATE
        if old_value is None and not new_value is None:
            elements.append(
                DatasetAnnotation(
                    key=key,
                    value=new_value,
                    column_id=column_id,
                    row_id=row_id
                )
            )
        elif not old_value is None and new_value is None:
            del_index = None
            for i in range(len(elements)):
                a = elements[i]
                if a.column_id == column_id and a.row_id == row_id:
                    if a.key == key and a.value == old_value:
                        del_index = i
                        break
            if del_index is None:
                return False
            del elements[del_index]
        elif not old_value is None and not new_value is None:
            anno = None
            for a in elements:
                if a.column_id == column_id and a.row_id == row_id:
                    if a.key == key and a.value == old_value:
                        anno = a
                        break
            if anno is None:
                return False
            anno.value = new_value
        else:
            raise ValueError('invalid modification operation')
        # Write modified annotations to file
        annotations.to_file(metadata_filename)
        return True


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
    object: list
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
