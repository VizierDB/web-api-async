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


def get_types(df, metadata):
    """Parse Datamart profiler result to return a list with data types. The
    returned list contains exactly one data type for each column in the data
    frame. The position in the list corresponds to the column at the respective
    position in the data frame schema.

    Parameters
    ----------
    metadata: dict
        Metadata dictionary returned bt the Datamart profiler.

    Returns
    -------
    list
    """
    # This code assumes that the datamart profiler returns a list of profiling
    # results for each column with the order in the list corresponding the the
    # order of columns in the input data frame.
    column_types = list()
    for colidx, colname in enumerate(list(df.columns)):
        column = metadata['columns'][colidx]
        if column['name'] == colname:
            semantic = column['semantic_types']
            if semantic:
                column_type = semantic[0][semantic[0].rindex('/') + 1:]
            else:
                structural = column['structural_type']
                column_type = structural[structural.rindex('/') + 1:]
            column_type = SWITCHER.get(column_type, 'varchar')
        else:
            # If the assumption about the order of results does not hold
            # set the type for this column to a default value.
            column_type = 'varchar'
        column_types.append(column_type)
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
