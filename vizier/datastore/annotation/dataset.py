# Copyright (C) 2019 New York University,
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

"""Dataset maintain annotations for three type of resources: columns, rows, and
cells. Vizier does not reason about annotations at this point and therefore
there is only limited functionality provided to query annotations. The dataset
metadata object is barely a wrapper around three lists of resource annotations.
"""


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
        self.column = columns if not columns is None else list()
        self.rows = rows if not rows is None else list()
        self.cels = cels if not cels is None else list()

    def find_all(self, values, key):
        """Get the list of annotations that are associated with the given key.
        If no annotation is associated with the key an empty list is returned.

        Parameters
        ----------
        values: list(vizier.datastore.annotation.base.Annotation)
            List of annotations
        key: string
            Unique property key

        Returns
        -------
        list(vizier.datastore.annotation.base.Annotation)
        """
        result = list()
        for anno in values:
            if anno.key == key:
                result.append(anno)
        return result

    def find_one(self, values, key, raise_error_on_multi_value=True):
        """Get a single annotation that is associated with the given key. If no
        annotation is associated with the keyNone is returned. If multiple
        annotations are associated with the given key a ValueError is raised
        unless the raise_error_on_multi_value flag is False. In the latter case
        one of the found annotations is returned.

        Parameters
        ----------
        values: list(vizier.datastore.annotation.base.Annotation)
            List of annotations
        key: string
            Unique property key
        raise_error_on_multi_value: bool, optional
            Raises a ValueError if True and the given key is associated with
            multiple values.

        Returns
        -------
        vizier.datastore.annotation.base.Annotation
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
        list(vizier.datastpre.annotation.base.CellAnnotation)
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
        list(vizier.datastpre.annotation.base.ColumnAnnotation)
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
        list(vizier.datastpre.annotation.base.RowAnnotation)
        """
        result = list()
        for anno in self.rows:
            if anno.row_id == row_id:
                result.append(anno)
        return result
