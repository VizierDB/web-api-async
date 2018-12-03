# Copyright (C) 2018 New York University
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

"""Classes and methods to support simple queries over datasets."""

class DataStreamConsumer(object):
    """Row consumers are used to filter values for a data series."""
    def __init__(self, column_index, range_start=None, range_end=None, cast_to_number=True):
        """
        """
        self.column_index = column_index
        self.range_start = range_start if not range_start is None else 0
        self.range_end = range_end
        self.cast_to_number = cast_to_number
        self.values = list()

    def consume(self, row, row_index, cast_to_numeric=True):
        """
        """
        if row_index >= self.range_start:
            if self.range_end is None or row_index <= self.range_end:
                val = row.values[self.column_index]
                if self.cast_to_number:
                    # Only convert if not already a numeric value. Assumes a
                    # string if not numeric
                    if not isinstance(val, int) and not isinstance(val, float):
                        # Try to cast to integer first. Remove commas.
                        try:
                            val = int(val.replace(',', ''))
                        except ValueError:
                            # Try to convert to float if int failed
                            try:
                                val = float(val)
                            except ValueError:
                                pass
                self.values.append(val)


def get_dataset_chart(self, identifier, view):
    """Query a given dataset by selecting the columns in the given list.
    Each row in the result is the result of projecting a tuple in the
    dataset on the given columns.

    Raises ValueError if any of the specified columns do not exist.

    Parameters
    ----------
    identifier: string
        Unique dataset identifier
    view: vizier.plot.view.ChartViewHandle
        Chart view definition handle

    Returns
    -------
    list()
    """
    dataset = self.get_dataset(identifier)
    # Get index position for x-axis. Set to negative value if none is given.
    # the value is used to determine which data series are converted to
    # numeric values and which are not.
    x_axis = -1
    if not view.x_axis is None:
        x_axis = view.x_axis
    # Create a list of data consumers, one for each data series
    consumers = list()
    for s_idx in range(len(view.data)):
        s = view.data[s_idx]
        c_idx = get_index_for_column(dataset, s.column)
        consumers.append(
            DataStreamConsumer(
                column_index=c_idx,
                range_start=s.range_start,
                range_end=s.range_end,
                cast_to_number=(s_idx != x_axis)
            )
        )
    # Consume all dataset rows
    rows = dataset.fetch_rows()
    for row_index in range(len(rows)):
        row = rows[row_index]
        for c in consumers:
            c.consume(row=row, row_index=row_index)
    # the size of the result set is determined by the longest data series
    max_values = -1
    for c in consumers:
        if len(c.values) > max_values:
            max_values = len(c.values)
    # Create result array
    data = []
    for idx_row in range(max_values):
        row = list()
        for idx_series in range(len(consumers)):
            consumer = consumers[idx_series]
            if idx_row < len(consumer.values):
                row.append(consumer.values[idx_row])
            else:
                row.append(None)
        data.append(row)
    return data
