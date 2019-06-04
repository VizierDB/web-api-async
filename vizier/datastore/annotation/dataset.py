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

"""Dataset maintain annotations for three type of resources: columns, rows, and
cells. Vizier does not reason about annotations at this point and therefore
there is only limited functionality provided to query annotations. The dataset
metadata object is barely a wrapper around three lists of resource annotations.
"""

import json
import os

from vizier.datastore.annotation.base import DatasetAnnotation


class DatasetMetadata(object):
    """Collection of annotations for a dataset object. For each of the three
    resource types a list of annotations is maintained.
    """
    def __init__(self, columns=None, rows=None, cells=None):
        """Initialize the metadata lists for the three different types of
        dataset resources that can be annotated.

        Parameters
        ----------
        columns: list(vizier.datastpre.annotation.base.ColumnAnnotation), optional
            Annotations for dataset columns
        rows: list(vizier.datastpre.annotation.base.RowAnnotation), optional
            Annotations for dataset rows
        cells: list(vizier.datastpre.annotation.base.CellAnnotation), optional
            Annotations for dataset cells
        """
        self.annotations = list()
        self.columns = columns if not columns is None else list()
        self.rows = rows if not rows is None else list()
        self.cells = cells if not cells is None else list()

    def add(self, key, value, column_id=None, row_id=None):
        """Add a new annotation for a dataset resource. The resource type is
        determined based on the column and row identifier values. At least one
        of them has to be not None. Otherwise, a ValueError is raised.

        Parameters
        ----------
        key: string
            Annotation key
        value: scalar
            Annotation value
        column_id: int, 'rows'optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier
        """
        # Create the annotation object. This will raise an exception if the
        # resource identifier is invalid.
        annotation = DatasetAnnotation(
            key=key,
            value=value,
            column_id=column_id,
            row_id=row_id
        )
        if row_id is None and column_id is None:
            self.annotations.append(annotation) 
        if row_id is None:
            self.columns.append(annotation)
        elif column_id is None:
            self.rows.append(annotation)
        else:
            self.cells.append(annotation)

    def clear_cell(self, column_id, row_id):
        """Remove all annotations for a given cell.

        Parameters
        ----------
        column_id: int
            Unique column identifier
        row_id: int
            Unique row identifier
        """
        # Delete by making a copy of the cells list containing the remaining
        # annotations
        annotations = list()
        for anno in self.cells:
            if anno.column_id != column_id or anno.row_id != row_id:
                annotations.append(anno)
        self.cells = annotations

    def find_all(self, values, key):
        """Get the list of annotations that are associated with the given key.
        If no annotation is associated with the key an empty list is returned.

        Parameters
        ----------
        values: list(vizier.datastore.annotation.base.DatasetAnnotation)
            List of annotations
        key: string
            Unique property key

        Returns
        -------
        list(vizier.datastore.annotation.base.DatasetAnnotation)
        """
        result = list()
        for anno in values:
            if anno.key == key:
                result.append(anno)
        return result

    def filter(self, columns=None, rows=None):
        """Filter annotations to keep only those that reference existing
        resources. Returns a new dataset metadata object.

        Parameters
        ----------
        columns: list(int), optional
            List of dataset column identifier
        rows: list(int), optional
            List of dataset row identifier, optional

        Returns
        -------
        vizier.datastore.annotation.dataset.DatasetMetadata
        """
        result = DatasetMetadata(
            columns=self.columns if columns is None else list(),
            rows=self.rows if rows is None else list(),
            cells=self.cells if columns is None and rows is None else list()
        )
        if not columns is None:
            for anno in self.columns:
                if anno.column_id in columns:
                    result.columns.append(anno)
        if not rows is None:
            for anno in self.rows:
                if anno.row_id in rows:
                    result.rows.append(anno)
        if not columns is None or not rows is None:
            for anno in self.cells:
                if not columns is None and not anno.column_id in columns:
                    continue
                elif not rows is None and not anno.row_id in rows:
                    continue
                result.cells.append(anno)
        return result

    def find_one(self, values, key, raise_error_on_multi_value=True):
        """Get a single annotation that is associated with the given key. If no
        annotation is associated with the keyNone is returned. If multiple
        annotations are associated with the given key a ValueError is raised
        unless the raise_error_on_multi_value flag is False. In the latter case
        one of the found annotations is returned.

        Parameters
        ----------
        values: list(vizier.datastore.annotation.base.DatasetAnnotation)
            List of annotations
        key: string
            Unique property key
        raise_error_on_multi_value: bool, optional
            Raises a ValueError if True and the given key is associated with
            multiple values.

        Returns
        -------
        vizier.datastore.annotation.base.DatasetAnnotation
        """
        result = self.find_all(values=values, key=key)
        if len(result) == 0:
            return None
        elif len(result) == 1 or not raise_error_on_multi_value:
            return result[0]
        else:
            raise ValueError('multiple annotation values for \'' + str(key) + '\'')

    def for_cell(self, column_id, row_id):
        """Get list of annotations for the specified cell

        Parameters
        ----------
        column_id: int
            Unique column identifier
        row_id: int
            Unique row identifier

        Returns
        -------
        list(vizier.datastpre.annotation.base.DatasetAnnotation)
        """
        result = list()
        for anno in self.cells:
            if anno.column_id == column_id and anno.row_id == row_id:
                result.append(anno)
        return result

    def for_column(self, column_id):
        """Get object metadata set for a dataset column.

        Parameters
        ----------
        column_id: int
            Unique column identifier

        Returns
        -------
        list(vizier.datastpre.annotation.base.DatasetAnnotation)
        """
        result = list()
        for anno in self.columns:
            if anno.column_id == column_id:
                result.append(anno)
        return result

    def for_row(self, row_id):
        """Get object metadata set for a dataset row.

        Parameters
        ----------
        row_id: int
            Unique row identifier

        Returns
        -------
        list(vizier.datastpre.annotation.base.DatasetAnnotation)
        """
        result = list()
        for anno in self.rows:
            if anno.row_id == row_id:
                result.append(anno)
        return result

    @staticmethod
    def from_file(filename):
        """Read dataset annotations from file. Assumes that the file has been
        created using the default serialization (to_file), i.e., is in Json
        format.

        Parameters
        ----------
        filename: string
            Name of the file to read from

        Returns
        -------
        vizier.database.annotation.dataset.DatsetMetadata
        """
        # Return an empty annotation set if the file does not exist
        if not os.path.isfile(filename):
            return DatasetMetadata()
        with open(filename, 'r') as f:
            doc = json.loads(f.read())
        cells = None
        columns = None
        rows = None
        if 'cells' in doc:
            cells = [DatasetAnnotation.from_dict(a) for a in doc['cells']]
        if 'columns' in doc:
            columns = [DatasetAnnotation.from_dict(a) for a in doc['columns']]
        if 'rows' in doc:
            rows = [DatasetAnnotation.from_dict(a) for a in doc['rows']]
        return DatasetMetadata(
            cells=cells,
            columns=columns,
            rows=rows
        )

    @staticmethod
    def from_list(values):
        """Create dataset metadata instance from list of dataset annotations

        Parameters
        ----------
        values: list(vizier.datastore.annotation.base.DatasetAnnotation)
            List containing all annotations for a dataset

        Returns
        -------
        vizier.database.annotation.dataset.DatsetMetadata
        """
        cells = list()
        columns = list()
        rows = list()
        for anno in values:
            if anno.column_id is None and anno.row_id is None:
                raise ValueError('invalid dataset annotaiton')
            elif anno.row_id is None:
                columns.append(anno)
            elif anno.column_id is None:
                rows.append(anno)
            else:
                cells.append(anno)
        return DatasetMetadata(
            cells=cells,
            columns=columns,
            rows=rows
        )

    def remove(self, key=None, value=None, column_id=None, row_id=None):
        """Remove annotations for a dataset resource. The resource type is
        determined based on the column and row identifier values. At least one
        of them has to be not None. Otherwise, a ValueError is raised.

        If the key and/or value are given they are used as additional filters.
        Otherwise, all annotations for the resource are removed.

        Parameters
        ----------
        key: string, optional
            Annotation key
        value: scalar, optional
            Annotation value
        column_id: int, optional
            Unique column identifier
        row_id: int, optional
            Unique row identifier
        """
        # Get the resource annotations list and the indices of the candidates
        # that match the resource identifier.
        if column_id is None and row_id is None:
            raise ValueError('must specify at least one dataset resource identifier')
        elif column_id is None:
            elements = self.rows
        elif row_id is None:
            elements = self.columns
        else:
            elements = self.cells
        candidates = list()
        for i in range(len(elements)):
            anno = elements[i]
            if anno.column_id == column_id and anno.row_id == row_id:
                candidates.append(i)
        # Get indices of all annotations that are being deleted. Use key and
        # value as filter if given. Otherwise, remove all elements in the list
        del_idx_list = list()
        if key is None and value is None:
            del_idx_list = candidates
        elif key is None:
            for i in candidates:
                if elements[i].value == value:
                    del_idx_list.append(i)
        elif value is None:
            for i in candidates:
                if elements[i].key == key:
                    del_idx_list.append(i)
        else:
            for i in candidates:
                el = elements[i]
                if el.key == key and el.value == value:
                    del_idx_list.append(i)
        # Remove all elements at the given index positions. Make sure to adjust
        # indices as elements are deleted.
        for j in range(len(del_idx_list)):
            i = del_idx_list[j] - j
            del elements[i]

    def to_file(self, filename):
        """Write current annotations to file in default file format. The default
        serializartion format is Json.

        Parameters
        ----------
        filename: string
            Name of the file to write
        """
        doc = dict()
        if len(self.cells) > 0:
            doc['cells'] = [a.to_dict() for a in deduplicate(self.cells)]
        if len(self.columns) > 0:
            doc['columns'] = [a.to_dict() for a in deduplicate(self.columns)]
        if len(self.rows) > 0:
            doc['rows'] = [a.to_dict() for a in deduplicate(self.rows)]
        with open(filename, 'w') as f:
            json.dump(doc, f)

    @property
    def values(self):
        """List all annotations in the metadata set.

        Returns
        -------
        list(vizier.datastore.annotation.base.DatasetAnnotation)
        """
        return self.columns + self.rows + self.cells


#  -----------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def deduplicate(elements):
    """Remove duplicate entries in a list of dataset annotations.

    Parameters
    ----------
    elements: list(vizier.datastore.annotation.base.DatasetAnnotation)
        List of dataset annotations

    Returns
    -------
    list(vizier.datastore.annotation.base.DatasetAnnotation)
    """
    if len(elements) < 2:
        return elements
    s = sorted(elements, key=lambda a: (a.column_id, a.row_id, a.key, a.value))
    result = s[:1]
    for a in s[1:]:
        l = result[-1]
        if a.column_id != l.column_id or a.row_id != l.row_id or a.key != l.key or a.value != l.value:
            result.append(a)
    return result
