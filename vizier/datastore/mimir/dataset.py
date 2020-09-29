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

import re
from typing import cast, Optional, List, Dict, Any

from vizier.core.util import dump_json
from vizier.datastore.dataset import DatasetHandle, DatasetColumn, DATATYPE_VARCHAR
from vizier.datastore.mimir.reader import MimirDatasetReader
from vizier.datastore.annotation.base import DatasetCaveat

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
DATE_FORMAT = re.compile('^\d{4}-\d\d?-\d\d?$') # noqa: W605
DATETIME_FORMAT = re.compile('^\d{4}-\d\d?-\d\d? \d\d?:\d\d?:\d\d?(\.\d+)?$') # noqa: W605


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
    def __init__(self, 
            identifier: int, 
            name_in_dataset: str, 
            name_in_rdb: Optional[str] = None, 
            data_type: str = DATATYPE_VARCHAR
        ):
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

    def __repr__(self):
        ret = self.name
        if self.name != self.name_in_rdb:
            ret += "[{}]".format(self.name_in_rdb)
        ret += "({}):{}".format(self.identifier, self.data_type)
        return ret

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

    def to_dict(self) -> Dict[str, Any]:
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
        if self.data_type.lower() in ['int', 'real']:
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


MIMIR_ROWID_COL= MimirDatasetColumn( identifier = -1, name_in_dataset='', data_type='rowid') #type: ignore[no-untyped-call]


class MimirDatasetHandle(DatasetHandle):
    """Internal descriptor for datasets managed by the Mimir data store.
    Contains mapping for column names from a dataset to the corresponding object
    in a relational and a reference to the table or view that contains the
    dataset.
    """
    def __init__(self, 
            identifier: str, 
            columns: List[MimirDatasetColumn], 
            properties: Dict[str, Any], 
            name: str = "Untitled Dataset"
        ):
        """Initialize the descriptor.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier
        columns: list(vizier.datastore.mimir.MimirDatasetColumn)
            List of column names in the dataset schema and their corresponding
            names in the relational database table or view.
        row_counter: int
            Counter for unique row ids
        properties: dict(string, any)
            Properties for the newly created dataset for dataset components
        """
        super(MimirDatasetHandle, self).__init__(
            identifier=identifier,
            columns=list(columns), # list(...) to make the typechecker happy; see http://mypy.readthedocs.io/en/latest/common_issues.html#variance
            name=name
        )
        self._row_count: Optional[int] = None
        self._properties = properties

    def descriptor(self):
        """Get the descriptor for this dataset.

        Returns
        -------
        vizier.datastore.base.DatasetDescriptor
        """
        return self

    @staticmethod
    def from_mimir_result(
            table_name: str, 
            schema: List[Dict[str,str]], 
            properties: Dict[str, Any],
            name: Optional[str] = None
        ):
        columns = [
            MimirDatasetColumn(
                identifier=identifier,
                name_in_dataset=col["name"],
                name_in_rdb=col["name"],
                data_type=col["type"]
            )
            for identifier, col in enumerate(schema)
        ]

        return MimirDatasetHandle(
            identifier = table_name,
            columns = columns,
            properties = properties,
            name = table_name if name is None else name
        )

    def get_row_count(self) -> int:
        if self._row_count is None:
            # try to use the count property if present
            self._row_count = self.get_properties().get("count", None)
            if self._row_count is None:
                self._row_count = mimir.countRows(self.identifier)
        return self._row_count

    def get_properties(self) -> Dict[str, Any]:
        return self._properties

    def write_properties_to_file(self, file):
        if self.properties != {}:
            raise Exception("MIMIR SHOULD NOT BE SAVING PROPERTIES LOCALLY")

    def get_caveats(self, 
            column_id: Optional[int]=None, 
            row_id: Optional[str]=None
        ) -> List[DatasetCaveat]:
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
            sql = 'SELECT * '
            sql += 'FROM ' + self.identifier + ' '
            return mimir.explainEverythingJson(sql)
        elif row_id is None and column_id is not None:
            raise ValueError("Can't get caveats for just one column")
        elif column_id is None and row_id is not None:
            raise ValueError("Can't get caveats for just one row")
        else:
            assert column_id is not None
            column = self.column_by_id(column_id)
            assert isinstance(column, MimirDatasetColumn)
            sql = 'SELECT * '
            sql += 'FROM ' + self.identifier + ' '
            return mimir.explainCell(sql, column.name_in_rdb, str(row_id))

    def get_profiling(self):
        """Get profiling results for the dataset.

        Returns
        -------
        dict
        """
        return dict()

    def reader(self, offset=0, limit=None, rowid=None):
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
            table_name=self.identifier,
            columns=self.columns,
            offset=offset,
            limit=limit,
            rowid=rowid
        )

    def to_file(self, filename: str) -> None:
        """Write dataset to file. The default serialization format is Json.

        Parameters
        ----------
        filename: string
            Name of the file to write
        """
        doc = {
            'id': self.identifier,
            'columns': [cast(MimirDatasetColumn, col).to_dict() for col in self.columns],
            'tableName': str(self.identifier),
            'rowCounter': self._row_count
        }
        with open(filename, 'w') as f:
            dump_json(doc, f)

    def confirm_sync_with_mimir(self):
        try:
            mimir.getSchema("SELECT * FROM `{}`".format(self.identifier))
            return True
        except mimir.MimirError:
            return False

