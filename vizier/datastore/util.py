# Copyright (C) 2017-2020 New York University,
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

"""Helper methods for datasets and data stores."""


def validate_dataset(columns, rows):
    """Validate that (i) each column has a unique identifier, (ii) each row has
    a unique identifier, and (iii) each row has exactly one value per column.

    Returns the maximum column and row identifiers. Raises ValueError in case
    of a schema violation.

    Parameters
    ----------
    columns: list(vizier.datastore.dataset.DatasetColumn)
        List of columns. It is expected that each column has a unique
        identifier.
    rows: list(vizier.datastore.dataset.DatasetRow)
        List of dataset rows.

    Returns
    -------
    int, int
    """
    # Ensure that all column identifier are zero or greater, unique, and
    # smaller than the column counter (if given)
    col_ids = set()
    for col in columns:
        col_id = col.identifier
        if col.identifier < 0:
            raise ValueError('negative identifier %d' % (col_id))
        elif col.identifier in col_ids:
            raise ValueError('duplicate identifier %d' % (col_id))
        col_ids.add(col_id)
    # Ensure that all row identifier are zero or greater, unique, smaller than
    # the row counter (if given), and contain exactly one value for each column
    row_ids = set()
    for row in rows:
        row_id = row.identifier
        if len(row.values) != len(columns):
            raise ValueError('schema violation for row %d' % str(row_id))
        elif row.identifier < 0:
            raise ValueError('negative row identifier %d' % str(row_id))
        elif row.identifier in row_ids:
            raise ValueError('duplicate row identifier %d' % str(row_id))
        row_ids.add(row_id)
    max_colid = max(col_ids) if len(col_ids) > 0 else -1
    max_rowid = max(row_ids) if len(row_ids) > 0 else -1
    return max_colid, max_rowid
