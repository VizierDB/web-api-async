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

"""Resource object representing a module in a workflow that is available at a
remote vizier instance.
"""

from vizier.api.client.resources.chart import ChartHandle
from vizier.api.client.resources.dataset import DatasetDescriptor
from vizier.core.timestamp import to_datetime
from vizier.viztrail.module.output import OUTPUT_HTML, OUTPUT_TEXT
from vizier.viztrail.module.timestamp import ModuleTimestamp

import vizier.viztrail.module.base as states
import vizier.api.serialize.deserialize as deserialize


class ModuleResource(object):
    """A workflow module in a remote vizier instance."""
    def __init__(
        self, identifier, state, external_form, outputs, datasets, charts, timestamp,
        links
    ):
        """Initialize the branch attributes."""
        self.identifier = identifier
        self.state = state
        self.external_form = external_form
        self.outputs = outputs
        self.datasets = datasets
        self.charts = charts
        self.timestamp= timestamp
        self.links = links

    @staticmethod
    def from_dict(obj):
        """Get a workflow module resource instance from the dictionary
        representation returned by the vizier web service.

        Parameters
        ----------
        obj: dict
            Dictionary serialization of a workflow module handle

        Returns
        -------
        vizier.api.client.resources.module.ModuleResource
        """
        # Create a list of outputs.
        outputs = list()
        if 'outputs' in obj:
            for out in obj['outputs']['stdout']:
                if out['type'] in [OUTPUT_TEXT, OUTPUT_HTML]:
                    outputs.append(out['value'])
            for out in obj['outputs']['stderr']:
                outputs.append(out['value'])
        # Create the timestamp
        ts = obj['timestamps']
        timestamp = ModuleTimestamp(created_at=to_datetime(ts['createdAt']))
        if 'startedAt' in ts:
            timestamp.started_at = to_datetime(ts['startedAt'])
        if 'finishedAt' in ts:
            timestamp.finished_at = to_datetime(ts['finishedAt'])
        # Create dictionary of available datasets
        datasets = dict()
        if 'datasets' in obj:
            for ds in obj['datasets']:
                datasets[ds['name']] = ds['id']
        charts = dict()
        if 'charts' in obj:
            for ch in obj['charts']:
                c_handle = ChartHandle.from_dict(ch)
                charts[c_handle.name] = c_handle
        return ModuleResource(
            identifier=obj['id'],
            state=to_external_form(obj['state']),
            external_form=obj['text'],
            outputs=outputs,
            datasets=datasets,
            charts=charts,
            timestamp=timestamp,
            links=deserialize.HATEOAS(links=obj['links'])
        )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def to_external_form(state):
    """Convert a given state identifier to its external form.

    Parameters
    ----------
    state: int
         Unique module state identifier

    Returns
    -------
    string
    """
    if state == states.MODULE_PENDING:
        return 'pending'
    elif state == states.MODULE_RUNNING:
        return 'running'
    elif state == states.MODULE_CANCELED:
        return 'canceled'
    elif state == states.MODULE_ERROR:
        return 'error'
    elif state == states.MODULE_SUCCESS:
        return 'success'
    else:
        return 'unknown'
