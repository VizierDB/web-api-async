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

"""Manage dataset views for plotting purposes

Chart views are definitions of views on datasets that are indented for plotting
as charts in the Web UI.
"""

from vizier.core.util import get_unique_identifier


class DataSeriesHandle(object):
    """A data series is a chart is a pair of column identifier and series
    label. In the future we may add additional information, e.g., line color
    etc. for each chart.

    Attributes
    ----------
    column: string
        Column containing the series data
    label: string
        Data series label
    """
    def __init__(self, column, label=None, range_start=None, range_end=None):
        """Initialize the data series handle.

        Parameters
        ----------
        column: int
            Unique column identifier
        label: string, optional
            Data series label
        range_start: int, optional
            Series range start index. Default is 0.
        range_end: int, optional
            Series range end index. Defautl is number of rows. Note that the
            end index is inclusive
        """
        self.column = column
        self.label = label
        self.range_start = range_start
        self.range_end = range_end

    @staticmethod
    def from_dict(obj):
        """Create data series handle from dictionary serialization.

        Parameters
        ----------
        obj: dict
            Dictionary serialization of a data series handle

        Returns
        -------
        vizier.plot.view.DataSeriesHandle
        """
        return DataSeriesHandle(
            obj['column'],
            label=obj['label'] if 'label' in obj else None,
            range_start=obj['rangeStart'] if 'rangeStart' in obj else None,
            range_end=obj['rangeEnd'] if 'rangeEnd' in obj else None
        )

    def to_dict(self):
        """Create dictionary serialization for data series handle.

        Returns
        -------
        dict
        """
        obj = {'column': self.column}
        if not self.label is None:
            obj['label'] = self.label
        if not self.range_start is None:
            obj['rangeStart'] = self.range_start
        if not self.range_end is None:
            obj['rangeEnd'] = self.range_end
        return obj


class ChartViewHandle(object):
    """Handle containing information about a user-defied chart view. This type
    of chart is a view on top of a dataset that is associated with a curation
    workflow.

    The dataset is identified by the name under which it is known in the
    workflow. Thus, the view can be evaluated for different version of the
    same dataset.

    Information about columns in the data series (and optional x-axis) currently
    reference columns by their name. This may change in future to make the
    view more robust against renaming of columns.
    """
    def __init__(
        self, dataset_name, identifier=None, chart_name=None, data=None,
        x_axis=None, chart_type=None, grouped_chart=True
    ):
        """Initialize the view handle.

        Parameters
        ----------
        identifier: string
            Unique view identifier
        dataset_name: string
            Name used toreference a dataset in the curation workflow within
            which this view is defined
        chart_name: string
            Unique chart name for reference
        data: list(vizier.plot.view.DataSeriesHandle), optional
            List of data series handles defining the data series in the chart
        x_axis: int, optional
            Optional index of the data series that is used for x-axis labels
        chart_type: string, optional
            Type of chart that is being displayed
        grouped_chart: bool, optional
            Flag indicating whether data series are grouped into single chart
        """
        self.dataset_name = dataset_name
        self.identifier = identifier if not identifier is None else get_unique_identifier()
        self.chart_name = chart_name if not chart_name is None else 'Chart'
        self.data = data if not data is None else list()
        self.x_axis = x_axis
        self.chart_type = chart_type if not chart_type is None else 'Bar Chart'
        self.grouped_chart = grouped_chart

    def add_series(self, column, label=None, range_start=None, range_end=None):
        """Append a data series to the chart view.

        Parameters
        ----------
        column: string
            Column containing the series data
        label: string, optional
            Data series label
        range_start: int, optional
            Series range start index. Default is 0.
        range_end: int, optional
            Series range end index. Defautl is number of rows. Note that the
            end index is inclusive

        Returns
        -------
        vizier.plot.view.DataSeriesHandle
        """
        series = DataSeriesHandle(
            column,
            label=label,
            range_start=range_start,
            range_end=range_end
        )
        self.data.append(series)
        return series

    @staticmethod
    def from_dict(obj):
        """Create chart handle from dictionary serialization.

        Parameters
        ----------
        obj: dict
            Dictionaty serialization of a chart handle

        Returns
        -------
        vizier.plot.view.ChartViewHandle
        """
        # Check if x_axis information is present
        if 'xAxis' in obj:
            x_axis = obj['xAxis']
        else:
            x_axis = None
        return ChartViewHandle(
            identifier=obj['id'],
            dataset_name=obj['dataset'],
            chart_name=obj['name'],
            data=[DataSeriesHandle.from_dict(s) for s in obj['data']],
            x_axis=x_axis,
            chart_type=obj['chartType'],
            grouped_chart=obj['groupedChart']
        )

    def schema(self):
        """Get a dictionary serialization of the schema information for the
        chart view.

        Returns
        -------
        dict
        """
        schema = dict({'series': list()})
        if not self.x_axis is None:
            schema['xAxis'] = self.x_axis
        for s in self.data:
            schema['series'].append({
                'label': s.label,
                'index': len(schema['series'])
            })
        return schema

    def to_dict(self):
        """Create dictionary serialization for this view handle.

        Returns
        -------
        dict
        """
        # Basic information that should always be present
        obj = {
            'id': self.identifier,
            'dataset': self.dataset_name,
            'name': self.chart_name,
            'data': [s.to_dict() for s in self.data],
            'chartType': self.chart_type,
            'groupedChart': self.grouped_chart
        }
        # X-Axis information is optional
        if not self.x_axis is None:
            obj['xAxis'] = self.x_axis
        # Return dictionary
        return obj
