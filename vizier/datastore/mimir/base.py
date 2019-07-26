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

"""Declaration of constants and helper methods for the Mimir datastore."""


"""Name of the database column that contains the row id for tuples
(Important: Use upper case!).
"""
from vizier.datastore.dataset import DATATYPE_DATE, DATATYPE_DATETIME, DATATYPE_INT, DATATYPE_REAL, DATATYPE_VARCHAR
from datetime import date, datetime

ROW_ID = 'RID'
BAD_COL_NAMES = {
    "ABORT":"`ABORT`", 
    "ACTION":"`ACTION`", 
    "ADD":"`ADD`", 
    "AFTER":"`AFTER`", 
    "ALL":"`ALL`", 
    "ALTER":"`ALTER`", 
    "ANALYZE":"`ANALYZE`", 
    "AND":"`AND`", 
    "AS":"`AS`", 
    "ASC":"`ASC`", 
    "ATTACH":"`ATTACH`", 
    "AUTOINCREMENT":"`AUTOINCREMENT`", 
    "BEFORE":"`BEFORE`", 
    "BEGIN":"`BEGIN`", 
    "BETWEEN":"`BETWEEN`", 
    "BY":"`BY`", 
    "CASCADE":"`CASCADE`", 
    "CASE":"`CASE`", 
    "CAST":"`CAST`", 
    "CHECK":"`CHECK`", 
    "COLLATE":"`COLLATE`", 
    "COLUMN":"`COLUMN`", 
    "COMMIT":"`COMMIT`", 
    "CONFLICT":"`CONFLICT`", 
    "CONSTRAINT":"`CONSTRAINT`", 
    "CREATE":"`CREATE`", 
    "CROSS":"`CROSS`", 
    "CURRENT":"`CURRENT`", 
    "CURRENT_DATE":"`CURRENT_DATE`", 
    "CURRENT_TIME":"`CURRENT_TIME`", 
    "CURRENT_TIMESTAMP":"`CURRENT_TIMESTAMP`", 
    "DATABASE":"`DATABASE`", 
    "DEFAULT":"`DEFAULT`", 
    "DEFERRABLE":"`DEFERRABLE`", 
    "DEFERRED":"`DEFERRED`", 
    "DELETE":"`DELETE`", 
    "DESC":"`DESC`", 
    "DETACH":"`DETACH`", 
    "DISTINCT":"`DISTINCT`", 
    "DO":"`DO`", 
    "DROP":"`DROP`", 
    "EACH":"`EACH`", 
    "ELSE":"`ELSE`", 
    "END":"`END`", 
    "ESCAPE":"`ESCAPE`", 
    "EXCEPT":"`EXCEPT`", 
    "EXCLUSIVE":"`EXCLUSIVE`", 
    "EXISTS":"`EXISTS`", 
    "EXPLAIN":"`EXPLAIN`", 
    "FAIL":"`FAIL`", 
    "FILTER":"`FILTER`", 
    "FOLLOWING":"`FOLLOWING`", 
    "FOR":"`FOR`", 
    "FOREIGN":"`FOREIGN`", 
    "FROM":"`FROM`", 
    "FULL":"`FULL`", 
    "GLOB":"`GLOB`", 
    "GROUP":"`GROUP`", 
    "HAVING":"`HAVING`", 
    "IF":"`IF`", 
    "IGNORE":"`IGNORE`", 
    "IMMEDIATE":"`IMMEDIATE`", 
    "IN":"`IN`", 
    "INDEX":"`INDEX`", 
    "INDEXED":"`INDEXED`", 
    "INITIALLY":"`INITIALLY`", 
    "INNER":"`INNER`", 
    "INSERT":"`INSERT`", 
    "INSTEAD":"`INSTEAD`", 
    "INTERSECT":"`INTERSECT`", 
    "INTO":"`INTO`", 
    "IS":"`IS`", 
    "ISNULL":"`ISNULL`", 
    "JOIN":"`JOIN`", 
    "KEY":"`KEY`", 
    "LEFT":"`LEFT`", 
    "LIKE":"`LIKE`", 
    "LIMIT":"`LIMIT`", 
    "MATCH":"`MATCH`", 
    "NATURAL":"`NATURAL`", 
    "NO":"`NO`", 
    "NOT":"`NOT`", 
    "NOTHING":"`NOTHING`", 
    "NOTNULL":"`NOTNULL`", 
    "NULL":"`NULL`", 
    "OF":"`OF`", 
    "OFFSET":"`OFFSET`", 
    "ON":"`ON`", 
    "OR":"`OR`", 
    "ORDER":"`ORDER`", 
    "OUTER":"`OUTER`", 
    "OVER":"`OVER`", 
    "PARTITION":"`PARTITION`", 
    "PLAN":"`PLAN`", 
    "PRAGMA":"`PRAGMA`", 
    "PRECEDING":"`PRECEDING`", 
    "PRIMARY":"`PRIMARY`", 
    "QUERY":"`QUERY`", 
    "RAISE":"`RAISE`", 
    "RANGE":"`RANGE`", 
    "RECURSIVE":"`RECURSIVE`", 
    "REFERENCES":"`REFERENCES`", 
    "REGEXP":"`REGEXP`", 
    "REINDEX":"`REINDEX`", 
    "RELEASE":"`RELEASE`", 
    "RENAME":"`RENAME`", 
    "REPLACE":"`REPLACE`", 
    "RESTRICT":"`RESTRICT`", 
    "RIGHT":"`RIGHT`", 
    "ROLLBACK":"`ROLLBACK`", 
    "ROW":"`ROW`", 
    "ROWS":"`ROWS`", 
    "SAVEPOINT":"`SAVEPOINT`", 
    "SELECT":"`SELECT`", 
    "SET":"`SET`", 
    "TABLE":"`TABLE`", 
    "TEMP":"`TEMP`", 
    "TEMPORARY":"`TEMPORARY`", 
    "THEN":"`THEN`", 
    "TO":"`TO`", 
    "TRANSACTION":"`TRANSACTION`", 
    "TRIGGER":"`TRIGGER`", 
    "UNBOUNDED":"`UNBOUNDED`", 
    "UNION":"`UNION`", 
    "UNIQUE":"`UNIQUE`", 
    "UPDATE":"`UPDATE`", 
    "USING":"`USING`", 
    "VACUUM":"`VACUUM`", 
    "VALUES":"`VALUES`", 
    "VIEW":"`VIEW`", 
    "VIRTUAL":"`VIRTUAL`", 
    "WHEN":"`WHEN`", 
    "WHERE":"`WHERE`", 
    "WINDOW":"`WINDOW`", 
    "WITH":"`WITH`", 
    "WITHOUT":"`WITHOUT`"
}

# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_select_query(table_name, columns=None):
    """Get SQL query to select a full dataset with columns in order of their
    appearance as defined in the given column list. The first column will be
    the ROW ID.

    Parameters
    ----------
    table_name: string
        Name of the database table or view
    columns: list(vizier.datastore.mimir.MimirDatasetColumn), optional
        List of columns in the dataset

    Returns
    -------
    str
    """
    if not columns is None:
        col_list = ','.join([col.name_in_rdb for col in columns])
        return 'SELECT ' + ROW_ID + ',' + col_list + ' FROM ' + table_name
    else:
        return 'SELECT ' + ROW_ID + ' FROM ' + table_name

def convertrowid(s, idx):
        try:
            return int(s)
        except ValueError:
            pass
        try:
            return int(s.replace("'", ""))
        except ValueError:
            pass
        try:
            return int(s.split('|')[0])
        except:
            return idx

def mimir_value_to_python(encoded, column):
    if column.data_type == DATATYPE_DATE and type(encoded) is dict:
        return date(encoded["year"], encoded["month"], encoded["date"])
    elif column.data_type == DATATYPE_DATETIME and type(encoded) is dict:
        return datetime(
                encoded["year"], encoded["month"], encoded["date"],
                encoded.get("hour", 0), 
                encoded.get("min", 0), 
                encoded.get("sec", 0), 
                encoded.get("msec", 0)
            )
    else:
        return encoded


def sanitize_column_name(name):
    return BAD_COL_NAMES.get(name, name)

