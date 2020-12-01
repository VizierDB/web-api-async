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

"""Methods for running data profiling algorithms on a data frame."""

import vizier.datastore.profiling.datamart as datamart


"""Default elements in data profiling results."""
COLUMN_TYPES = 'columnTypes'
PROFILING_RESULTS = 'profiling'
PROFILER_NAME = 'name'
PROFILER_DATA = 'data'
ROWCOUNT = 'rowCount'


def run(df, profiler=None, default_types=None):
    """Create profiling results for a given data frame. Will at least assign
    a data type to each column in the data frame.

    Returns a dictionary containing at least two elements (at most three):

    - ROWCOUNT ('rowCount'): Number of rows in the data frame.
    - COLUMN_TYPES ('columnTypes'): List if raw data types for columns in the
      data frame. The position in the returned list corresponds to the position
      of columns in the data frame.
    - PROFILING_RESULTS ('profiling'): Results from running the optional data
      profiler on the data frame. This element is optional. If present, it
      contains two elements:
      - PROFILER_NAME ('name'): Unique profiler identifier
      - PROFILER_DATA ('data'): Profiler results.

    Parameters
    ----------
    df: pandas.DataFrame
        Data frame containing the dataset snapshot.
    profilers: string, default=None
        Identifier for data profiler that is used to infer column types.
        If None, all column types will be 'varchar' by default.
    default_types: list, default=None
        Optional list of default column types.

    Returns
    -------
    dict
    """
    # If the list of default column types is given, the number of elements in
    # the list have to match the number of columns in the data frame.
    if default_types is not None:
        if len(df.columns) != len(default_types):
            msg = 'incompatible data type list {}'
            raise ValueError(msg.format(default_types))
    else:
        # By default, the data type for each column is 'varchar'
        default_types = ['varchar'] * len(df.columns)
    # The number of rows is always part of the returned meta data dictionary.
    metadata = dict({ROWCOUNT: len(df.index)})
    if profiler == 'datamartprofiler':
        data = datamart.run(df)
        metadata[PROFILING_RESULTS] = {
            PROFILER_NAME: profiler,
            PROFILER_DATA: data
        }
        metadata[COLUMN_TYPES] = datamart.get_types(df, data)
    else:
        metadata[COLUMN_TYPES] = default_types
    return metadata
