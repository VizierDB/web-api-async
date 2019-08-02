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

"""Classes to support queries over datasets to generate simple plot charts."""

from datetime import date, datetime
import time

class DataStreamConsumer(object):
    """Consumer for data rows. The row consumers are used to filter cell values
    for a given column and a range interval of rows. The result is a list of
    values that represent a data series in a chart plot.

    Attributes
    ----------
    values: list
        List of values in the resulting data series
    """
    def __init__(self, column_index, range_start=None, range_end=None, cast_to_number=True):
        """Initialize the index position for the column in the dataset schema
        that is being consumed and the range interval of row indexes.

        If range_start is None the considered interval of rows starts at 0. If
        range_end is None all rows until the end of the dataset are being
        consumed.

        Parameters
        ----------
        column_index: int
            Position of consumed column in dataset schema
        range_start: int, optional
            Interval start (inclusive) for range of rows that are consumed
        range_end: int, optional
            Interval end (inclusive) for range of rows that are consumed
        cast_to_number: bool, optional
            Attempt to cast cell values to integers if True
        """
        self.column_index = column_index
        self.range_start = range_start if not range_start is None else 0
        self.range_end = range_end
        self.cast_to_number = cast_to_number
        # Initialize the result set
        self.values = list()

    def consume(self, row, row_index):
        """Consume a dataset row. The position of the row in the ordered list of
        dataset rows is given by the row_index.

        Parameters
        ----------
        row: vizier.datastore.dataset.DatasetRow
            Row in a dataset
        row_index: int
            Position of row in datasets
        """
        # Check if the row index falls inside the consumed interval
        if row_index >= self.range_start and (self.range_end is None or row_index <= self.range_end):
            val = row.values[self.column_index]
            if isinstance(val, date) or isinstance(val, datetime):
                val = time.mktime(val.timetuple())
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


class ChartQuery(object):
    """Query processor for simple chart queries."""
    @staticmethod
    def exec_query(dataset, view):
        """Query a given dataset by selecting the columns in the given list.
        Each row in the result is the result of projecting a tuple in the
        dataset on the given columns.

        Raises ValueError if any of the specified columns do not exist.

        Parameters
        ----------
        dataset: vizier.datastore.dataset.DatasetHandle
            Handle for dataset that is being queried
        view: vizier.view.chart.ChartViewHandle
            Chart view definition handle

        Returns
        -------
        list()
        """
        # Get index position for x-axis. Set to negative value if none is given.
        # the value is used to determine which data series are converted to
        # numeric values and which are not.
        x_axis = -1
        if not view.x_axis is None:
            x_axis = view.x_axis
        # Create a list of data consumers, one for each data series. Keep track
        # of the maximum range interval
        consumers = list()
        max_interval = (dataset.row_count + 1, 0)
        for s_idx in range(len(view.data)):
            series = view.data[s_idx]
            column_index = dataset.get_index(series.column)
            if column_index is None:
                raise ValueError('unknown column identifier \'' + str(series.column) + '\'')
            range_start = series.range_start if not series.range_start is None else 0
            range_end = series.range_end if not series.range_end is None else dataset.row_count
            consumers.append(
                DataStreamConsumer(
                    column_index=column_index,
                    range_start=range_start,
                    range_end=range_end,
                    cast_to_number=(s_idx != x_axis)
                )
            )
            if range_start < max_interval[0]:
                max_interval = (range_start, max_interval[1])
            if range_end > max_interval[1]:
                max_interval = (max_interval[0], range_end)
        # Consume all dataset rows in the maximum interval
        rows = dataset.fetch_rows(
            offset=max_interval[0],
            limit=(max_interval[1]-max_interval[0])+1
        )
        row_index = max_interval[0]
        for row in rows:
            for c in consumers:
                c.consume(row=row, row_index=row_index)
            row_index += 1
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
