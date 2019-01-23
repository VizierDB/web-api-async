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

"""Query workflow states to get results for a dataset chart view."""

from vizier.engine.packages.plot.query import ChartQuery
from vizier.viztrail.module.output import CHART_VIEW_DATA

import vizier.api.serialize.view as serialize


class VizierDatasetViewApi(object):
    """The vizier dataset view API implements the methods that query a vizier
    workflow module state to get the results for a dataset chart view.
    """
    def __init__(self, projects, urls):
        """Initialize the API components.

        Parameters
        ----------
        projects: vizier.engine.project.cache.base.ProjectCache
            Cache for project handles
        urls: vizier.api.routes.base.UrlFactory
            Factory for resource urls
        """
        self.projects = projects
        self.urls = urls

    def get_dataset_chart_view(self, project_id, branch_id, workflow_id, module_id, chart_id):
        """Get chart view data for a given workflow module. Returns None if
        either of the specified resources does not exist.

        Raises a ValueError if the chart exists but the specified datasets does
        not exist in the workflow module.

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

        Returns
        -------
        dict
        """
        # Retrieve the project, branch and workflow to ensure that they exist.
        # Return None if either is unknown.
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        branch = project.viztrail.get_branch(branch_id)
        if branch is None:
            return None
        workflow = branch.get_workflow(workflow_id)
        if workflow is None:
            return None
        # Get the specified module. Keep track of the available charts while
        # searching for the module.
        module = None
        charts = dict()
        for m in workflow.modules:
            if not m.provenance.charts is None:
                for c in m.provenance.charts:
                    charts[c.chart_name] = c
            if m.identifier == module_id:
                module = m
                break
        if module is None:
            return None
        # Find the specified chart
        chart = None
        for c in charts.values():
            if c.identifier == chart_id:
                chart = c
                break
        if chart is None:
            return None
        # Get the dataset that is referenced in the chart handle. Raise an
        # exception if the dataset does not exist in the module. If the chart
        # was defined in the module we do not need to execute the query but
        # can take the result directly from the module output.
        if not chart.dataset_name in module.datasets:
            raise ValueError('unknown dataset \'' + chart.dataset_name + '\'')
        if not module.provenance.charts is None and chart.chart_name in module.provenance.charts:
            data = module.outputs.stdout[0].value
        else:
            dataset_id = module.datasets[chart.dataset_name].identifier
            dataset = project.datastore.get_dataset(dataset_id)
            rows = ChartQuery.exec_query(dataset=dataset, view=chart)
            data = CHART_VIEW_DATA(view=chart, rows=rows)
        return serialize.CHART_VIEW(
            project_id=project_id,
            branch_id=branch_id,
            workflow_id=workflow_id,
            module_id=module_id,
            chart_id=chart_id,
            name=chart.chart_name,
            data=data,
            urls=self.urls
        )
