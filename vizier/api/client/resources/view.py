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

"""Objects representing chart view resources at the wen service API."""


class DataSeries(object):
    """Data series in a chart view. Each series has a name and a list of
    values.
    """
    def __init__(self, name, values):
        """Initialize the data series components.

        Parameters
        ----------
        name: string
            Data series name
        values: list
            List of value sin the data series
        """
        self.name = name
        self.values = values

    @staticmethod
    def from_dict(obj):
        """Create data series instance from default dictionary serialization
        returned by the web service API.

        Parameters
        ----------
        obj: dict
            Data series in default dictionary serialization format

        Returns
        -------
        vizier.api.client.resources.view.DataSeries
        """
        return DataSeries(name=obj['label'], values=obj['data'])

    @property
    def length(self):
        """Number of values in the data series.

        Returns
        -------
        int
        """
        return len(self.values)


class ChartView(object):
    """A chart view is a list of data series."""
    def __init__(self, data):
        """Initialize the data series.

        Parameters
        ----------
        data: list(vizier.api.client.resources.view.DataSeries)
            List of data series that define the chart view
        """
        self.data = data

    @staticmethod
    def from_dict(obj):
        """Create chart view instance from dictionary serialization returned
        by the web service API.

        Parameters
        ----------
        obj: dict
            Chart view in default dictionary serialization format

        Returns
        -------
        vizier.api.client.resources.view.ChartView
        """
        data = list()
        for series in obj['data']['series']:
            data.append(DataSeries.from_dict(series))
        return ChartView(data)
