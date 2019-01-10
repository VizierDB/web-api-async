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

"""Vizier supports annotations for dataset columns, rows, and cells. Each
annotation is a key,value pair and has a unique identifier. This module contains
annotation objects for the three types of resources listed above.
"""


class Annotation(object):
    """Annotations are identifiable key value pairs. The object identifier are
    unique only among annotations for the same dataset component. They are not
    global unique identifier.

    Attributes
    ----------
    identifier: int, optional
        Unique annotation identifier
    key: string
        Annotation key
    value: string
        Annotation value
    """
    def __init__(self, key, value, identifier=None):
        """Initialize the annotation components. When creating a new annotation
        object the identifier can be None.

        Parameters
        ----------
        key: string
            Annotation key
        value: string
            Annotation value
        identifier: int, optional
            Unique annotation identifier
        """
        self.identifier = identifier
        self.key = key
        self.value = value


class CellAnnotation(Annotation):
    """A cell annotation extends the annotation object with the unique column
    identifier and row identifier for the cell.
    """
    def __init__(self, column_id, row_id, key, value, identifier=None):
        """Initialize the annotation components.

        Parameters
        ----------
        column_id: int
            Unique column identifier
        row_id: int
            Unique row identifier
        key: string
            Annotation key
        value: string
            Annotation value
        identifier: int, optional
            Unique annotation identifier
        """
        super(ColumnAnnotation, self).__init__(
            key=key,
            value=value,
            identifier=identifier
        )
        self.column_id = column_id
        self.row_id = row_id


class ColumnAnnotation(Annotation):
    """A column annotation extends the annotation object with the unique column
    identifier.
    """
    def __init__(self, column_id, key, value, identifier=None):
        """Initialize the annotation components.

        Parameters
        ----------
        column_id: int
            Unique column identifier
        key: string
            Annotation key
        value: string
            Annotation value
        identifier: int, optional
            Unique annotation identifier
        """
        super(ColumnAnnotation, self).__init__(
            key=key,
            value=value,
            identifier=identifier
        )
        self.column_id = column_id


class RowAnnotation(Annotation):
    """A row annotation extends the annotation object with the unique row
    identifier.
    """
    def __init__(self, row_id, key, value, identifier=None):
        """Initialize the annotation components.

        Parameters
        ----------
        row_id: int
            Unique row identifier
        key: string
            Annotation key
        value: string
            Annotation value
        identifier: int, optional
            Unique annotation identifier
        """
        super(RowAnnotation, self).__init__(
            key=key,
            value=value,
            identifier=identifier
        )
        self.row_id = row_id
