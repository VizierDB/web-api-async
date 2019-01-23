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

"""Methods for serializing chart view data."""

import vizier.api.serialize.base as serialize
import vizier.api.serialize.hateoas as ref
import vizier.api.serialize.labels as labels


def CHART_VIEW(project_id, branch_id, workflow_id, module_id, chart_id, name, data, urls):
    """Get dictionary serialization for a dataset chart view result.

    Parameters
    ----------
    project_id: string
        Unique project identifier
    branch_id: string
        Unique branch identifier
    workflow_id: string
        Unique workflow identifier
    module_id: string
        Unique module identifier
    chart_id: string
        Unique chart identifier
    name:
        Chart name
    data: dict
        Dictinary serialization for data series
    urls: vizier.api.routes.base.UrlFactory
        Factory for web service resource urls

    Returns
    -------
    dict
    """
    return {
        labels.NAME: name,
        labels.DATA: data,
        labels.LINKS: serialize.HATEOAS({
            ref.SELF: urls.get_chart_view(
                project_id=project_id,
                branch_id=branch_id,
                workflow_id=workflow_id,
                module_id=module_id,
                chart_id=chart_id
            )
        })
    }
