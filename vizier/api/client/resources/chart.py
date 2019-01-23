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

"""Objects representing descriptors dataset chart views."""

import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.labels as labels


class ChartHandle(object):
    """Handle for chart in a workflow module. Contains the chart name and the
    HATEOAS links with the self reference to retrieve the chart data.
    """
    def __init__(self, name, links):
        """Initialize the handle components."""
        self.name = name
        self.links = links

    @staticmethod
    def from_dict(obj):
        """Create chart handle instance from dictionary serialization returned
        by the web service API.

        Parameters
        ----------
        obj: dict
            Dictionary serialization for a chart handle in a workflow module

        Returns
        -------
        vizier.api.client.resources.chart.ChartHandle
        """
        return ChartHandle(
            name=obj[labels.NAME],
            links=deserialize.HATEOAS(links=obj[labels.LINKS])
        )
