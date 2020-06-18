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
ROWCOUNT = 'rowCount'
PROFILING_RESULTS = 'profiling'


def run(df, profiler=None, column_types=dict()):
    """Create profiling results for a given data frame. Will at least assign
    a data type to each column in the data frame.

    Returns a tuple containing the profiling result dictionary and the mapping
    of column names to data types.

    The profiling result will contain the row count for the given data frame.
    If a profiler is specified the results of running that profiler on the data
    frame will be included in the returned metadata dictionary.

    NOTE: The profiling will currently not work properly for data frames with
    duplicate column names.

    Parameters
    ----------
    df: pandas.DataFrame
        Data frame containing the dataset snapshot.
    profilers: string, default=None
        Identifier for data profiler that is used to infer column types.
        If None, all column types will be 'varchar' by default.
    column_types: dict, default=dict
        Optional mapping of column names to default column types.

    Returns
    -------
    dict, dict
    """
    metadata = dict({ROWCOUNT: len(df.index)})
    if profiler == 'datamartprofiler':
        # Run the Datamart profiler if requested by the user.
        results = datamart.run(df)
        column_types = datamart.get_types(results, column_types=column_types)
        metadata[PROFILING_RESULTS] = {'datamartprofiler': results}
    else:
        # By default, the data type for each column is 'varchar'
        for col in df.columns:
            column_types = column_types.get(col, 'varchar')
    return metadata, column_types
