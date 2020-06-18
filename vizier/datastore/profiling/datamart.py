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

import datamart_profiler as dmp


"""Mapping from Datamart data type names to Vizier data type names."""
SWITCHER = {
    'Enumeration':  'categorical',
    'DateTime':  'datetime',
    'Text':  'varchar',
    'Real':  'real',
    'Float':  'real',
    'Integer':  'int',
    'Boolean':  'boolean'
}


def get_types(metadata, column_types=dict()):
    """Parse Datamart profiler result to return a dictionary that maps column
    names to data types.

    Parameters
    ----------
    metadata: dict
        Metadata dictionary returned bt the Datamart profiler.
    column_types: dict
        Optional dictionary of user provided default column types.

    Returns
    -------
    dict
    """
    column_types = {}
    for column in metadata['columns']:
        semantic_types = column['semantic_types']
        if semantic_types:
            column_type = semantic_types[0][semantic_types[0].rindex('/') + 1:]
        else:
            structural_type = column['structural_type']
            column_type = structural_type[structural_type.rindex('/') + 1:]
        col_name = column['name']
        default_type = column_types.get(col_name, 'varchar')
        column_types[col_name] = SWITCHER.get(column_type, default_type)
    return column_types


def run(df):
    """Execute the Datamart profiler on a given data frame.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame that is being profiled.

    Returns
    -------
    dict
    """
    return dmp.process_dataset(df, include_sample=False)
