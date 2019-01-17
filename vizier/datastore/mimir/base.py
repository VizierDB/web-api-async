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
ROW_ID = 'RID'


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
