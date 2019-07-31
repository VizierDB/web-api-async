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

import json
import re

from vizier.core.util import dump_json, load_json
from vizier.datastore.annotation.base import DatasetAnnotation
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.datastore.dataset import DatasetHandle, DatasetColumn, DatasetRow
from vizier.datastore.mimir.reader import MimirDatasetReader

import vizier.mimir as mimir


"""Mimir annotation keys."""
ANNO_UNCERTAIN = 'mimir:uncertain'

"""Value casts for SQL update statements."""
CAST_TRUE = 'CAST(1 AS BOOL)'
CAST_FALSE = 'CAST(0 AS BOOL)'

"""Compiled regular expressions to identify valid date and datetime values.
Note that this does not check if a date string actually specifies a valid
calendar date. But it appears that Mimir accepts any sting that follows this
format."""
DATE_FORMAT = re.compile('^\d{4}-\d\d?-\d\d?$')
DATETIME_FORMAT = re.compile('^\d{4}-\d\d?-\d\d? \d\d?:\d\d?:\d\d?(\.\d+)?$')


class MimirDatasetColumn(DatasetColumn):
    """Column in a dataset that is stored as a Mimir table or view. Given that
    column names are not necessarily unique in a dataset, there is a need to
    maintain a mapping of dataset names to attribute names for tables/views in
    the relational database.

    Attributes
    ----------
    identifier: int
        Unique column identifier
    name: string
        Name of column in the dataset
    name_in_rdb: string
        Name of the corresponding attribute in a relational table or views
    data_type: string, optional
        String representation of the column type in the database. By now the
        following data_type values are expected: date (format yyyy-MM-dd), int,
        varchar, real, and datetime (format yyyy-MM-dd hh:mm:ss:zzzz).
    """
    def __init__(self, identifier=None, name_in_dataset=None, name_in_rdb=None, data_type=None):
        """Initialize the dataset column.

        Parameters
        ----------
        identifier: int
            Unique column identifier
        name_in_dataset: string
            Name of column in the dataset
        name_in_rdb: string, optional
            Name of the corresponding attribute in a relational table or views
        data_type: string, optional
            Identifier for data type of column values. Default is String
        """
        # Ensure that a valid data type is given
        super(MimirDatasetColumn, self).__init__(
            identifier=identifier,
            name=name_in_dataset,
            data_type=data_type
        )
        if not name_in_rdb is None:
            self.name_in_rdb = name_in_rdb.upper()
        else:
            self.name_in_rdb = name_in_dataset.upper()

    @staticmethod
    def from_dict(doc):
        """Create dataset column object from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for dataset column object

        Returns
        -------
        vizier.datastore.mimir.DatasetColumn
        """
        return MimirDatasetColumn(
            identifier=doc['id'],
            name_in_dataset=doc['name'],
            name_in_rdb=doc['rdbName'],
            data_type=doc['dataType']
        )

    def is_numeric(self):
        """Flag indicating if the data type of this column is numeric, i.e.,
        integer or real.

        Returns
        -------
        bool
        """
        return self.data_type.lower() in ['int', 'real']

    def to_dict(self):
        """Get dictionary serialization for dataset column object.

        Returns
        -------
        dict
        """
        return {
            'id': self.identifier,
            'name': self.name,
            'rdbName': self.name_in_rdb,
            'dataType': self.data_type
        }

    def to_sql_value(self, value):
        """Return an SQL conform representation of the given value based on the
        column's data type.

        Raises ValueError if the column type is numeric but the given value
        cannot be converted to a numeric value.

        Parameters
        ----------
        value: string
            Dataset cell value

        Returns
        -------
        string
        """
        # If the given value is None simply return the keyword NULL
        if value is None:
            return 'NULL'
        # If the data type of the columns is numeric (int or real) try to
        # convert the given argument to check whether it actually is a numeric
        # value. Note that we always return a string beacuse the result is
        # intended to be concatenated as part of a SQL query string.
        if self.is_numeric():
            try:
                int(value)
                return str(value)
            except ValueError:
                return str(float(value))
        elif self.data_type.lower() == 'date':
            if DATE_FORMAT.match(value):
                return 'CAST(\'' + str(value) + '\' AS DATE)'
            raise ValueError('not a date \'' + str(value) + '\'')
        elif self.data_type.lower() == 'datetime':
            if DATETIME_FORMAT.match(value):
                return 'CAST(\'' + str(value) + '\' AS DATETIME)'
            raise ValueError('not a datetime \'' + str(value) + '\'')
        elif self.data_type.lower() == 'bool':
            if isinstance(value, bool):
                if value:
                    return CAST_TRUE
                else:
                    return CAST_FALSE
            elif isinstance(value, int):
                if value == 1:
                    return CAST_TRUE
                elif value == 0:
                    return CAST_FALSE
            else:
                str_val = str(value).upper()
                if str_val in ['TRUE', '1']:
                    return CAST_TRUE
                elif str_val in ['FALSE', '0']:
                    return CAST_FALSE
            # If none of the previous tests returned a bool representation we
            # raise an exception to trigger value casting.
            raise ValueError('not a boolean value \'' + str(value) + '\'')
        #elif self.data_type.lower() in ['date', 'datetime']:
            #return self.data_type.upper() + '(\'' + str(value) + '\')'
        #    return 'DATE(\'' + str(value) + '\')'
        # By default and in case the given value could not be transformed into
        # the target format return a representation for a string value
        return '\'' + str(value) + '\''


class MimirDatasetHandle(DatasetHandle):
    """Internal descriptor for datasets managed by the Mimir data store.
    Contains mapping for column names from a dataset to the corresponding object
    in a relational and a reference to the table or view that contains the
    dataset.
    """
    def __init__(
        self, identifier, columns, rowid_column, table_name, row_idxs, row_ids,
        row_counter, annotations=None
    ):
        """Initialize the descriptor.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(vizier.datastore.mimir.MimirDatasetColumn)
            List of column names in the dataset schema and their corresponding
            names in the relational database table or view.
        rowid_column: vizier.datastore.mimir.MimirDatasetColumn
            Descriptor for unique row id column
        table_name: string
            Reference to relational database table containing the dataset.
        row_idxs: list(int)
            List of row indexes. Determines the order of rows in the dataset
        row_ids: list(string)
            List of row ids. for provenance of dataset
        row_counter: int
            Counter for unique row ids
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata
            Annotations for dataset components
        """
        super(MimirDatasetHandle, self).__init__(
            identifier=identifier,
            columns=columns,
            row_count=len(row_ids),
            annotations=annotations
        )
        self.rowid_column = rowid_column
        self.table_name = table_name
        self.row_idxs = row_idxs
        self.row_ids = row_ids
        self.row_counter = row_counter

    @staticmethod
    def from_file(filename, annotations=None):
        """Read dataset from file. Expects the file to be in Json format which
        is the default serialization format used by to_file().

        Parameters
        ----------
        filename: string
            Name of the file to read.
        annotations: vizier.datastore.annotation.dataset.DatasetMetadata, optional
            Annotations for dataset components
        Returns
        -------
        vizier.datastore.mimir.dataset.MimirDatasetHandle
        """
        with open(filename, 'r') as f:
            doc = load_json(f.read())
        return MimirDatasetHandle(
            identifier=doc['id'],
            columns=[MimirDatasetColumn.from_dict(obj) for obj in doc['columns']],
            rowid_column=MimirDatasetColumn.from_dict(doc['rowIdColumn']),
            table_name=doc['tableName'],
            row_idxs=doc['idxs'],
            row_ids=doc['rows'],
            row_counter=doc['rowCounter']
        )

    def get_annotations(self, column_id=None, row_id=None):
        """Get list of annotations for a dataset component. If both identifier
        equal -1 all annotations for a dataset are returned.

        Parameters
        ----------
        column_id: int, optional
            Unique column identifier
        row_id: string, optional
            Unique row identifier

        Returns
        -------
        list(vizier.datastpre.annotation.base.DatasetAnnotation)
        """
        if column_id is None and row_id is None:
            # TODO: If there is an option to get all annotations from Mimir for
            # all dataset cells we should add those annotations here. By now
            # this command will only return user-defined annotations for the
            # dataset.
            annotations = DatasetMetadata()
            sql = 'SELECT * '
            sql += 'FROM ' + self.table_name + ' '
            annoList = mimir.explainEverythingJson(sql)
            for anno in annoList:
                annotations.add(ANNO_UNCERTAIN, anno)
            #return [item for sublist in map(lambda (i,x): self.annotations.for_column(i).values(), enumerate(self.columns)) for item in sublist]
            #return self.annotations.values
            return annotations
        elif row_id is None:
            return self.annotations.for_column(column_id)
        elif column_id is None:
            return self.annotations.for_row(row_id)
        else:
            annotations = self.annotations.for_cell(
                column_id=column_id,
                row_id=int(row_id)
            )
            column = self.column_by_id(column_id)
            sql = 'SELECT * '
            sql += 'FROM ' + self.table_name + ' '
            buffer = mimir.explainCell(sql, column.name_in_rdb, str(row_id))
            has_reasons = buffer.size() > 0
            if has_reasons:
                for value in buffer.mkString("-*-*-").split("-*-*-"):
                    # Remove references to lenses
                    while 'LENS_' in value:
                        start_pos = value.find('LENS_')
                        end_pos = value.find('.', start_pos)
                        if end_pos > start_pos:
                            value = value[:start_pos] + value[end_pos + 1:]
                        else:
                            value = value[:start_pos]
                    # Replace references to column name
                    value = value.replace(column.name_in_rdb, column.name)
                    # Remove content in double square brackets
                    if '{{' in value:
                        value = value[:value.find('{{')].strip()
                    if value != '':
                        annotations.append(
                            DatasetAnnotation(
                                key=ANNO_UNCERTAIN,
                                value=value,
                                column_id=column_id,
                                row_id=int(row_id)
                            )
                        )
            return annotations

    def max_row_id(self):
        """Get maximum identifier for all rows in the dataset. If the dataset
        is empty the result is -1.

        Returns
        -------
        int
        """
        return max(self.row_idxs) if len(self.row_idxs) > 0 else -1

    def reader(self, offset=0, limit=-1, rowid=None):
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
        vizier.datastore.mimir.MimirDatasetReader
        """
        return MimirDatasetReader(
            table_name=self.table_name,
            columns=self.columns,
            row_idxs=self.row_idxs,
            row_ids=self.row_ids,
            rowid_column_numeric=self.rowid_column.is_numeric(),
            offset=offset,
            limit=limit,
            rowid=rowid
        )

    def to_file(self, filename):
        """Write dataset to file. The default serialization format is Json.

        Parameters
        ----------
        filename: string
            Name of the file to write
        """
        doc = {
            'id': self.identifier,
            'columns': [col.to_dict() for col in self.columns],
            'rowIdColumn': self.rowid_column.to_dict(),
            'idxs': self.row_idxs,
            'rows': self.row_ids,
            'tableName': str(self.table_name),
            'rowCounter': self.row_counter
        }
        with open(filename, 'w') as f:
            dump_json(doc, f)
