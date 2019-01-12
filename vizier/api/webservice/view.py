# Copyright (C) 2019 New York University,
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

"""Query workflow states to get results for a dataset chart view."""


class VizierDatasetViewApi(object):
    """The vizier dataset view API implements the methods that query a vizier
    workflow module state to get the results for a dataset chart view.
    """
    def __init__(self, engine, urls):
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

    def get_dataset_chart_view(self, project_id, branch_id, version, module_id, chart_id):
        """
        """
        # Retrieve the project to ensure that it exists.
        project = self.projects.get_project(project_id)
        if project is None:
            return None
        # Retrieve workflow from repository. The result is None if the branch
        # does not exist.
        workflow = self.viztrails.get_workflow(
            viztrail_id=project_id,
            branch_id=branch_id,
            workflow_version=version
        )
        if workflow is None:
            return None
        # Find the workflow module and ensure that the referenced view is
        # defined for the module.
        datasets = None
        v_handle = None
        for module in workflow.modules:
            for obj in module.stdout:
                if obj['type'] == serialize.O_CHARTVIEW:
                    view = ChartViewHandle.from_dict(obj['data'])
                    if view.identifier == view_id:
                        v_handle = view
            if module.identifier == module_id:
                datasets = module.datasets
                break
        if not datasets is None and not v_handle is None:
            if not v_handle.dataset_name in datasets:
                raise ValueError('unknown dataset \'' + v_handle.dataset_name + '\'')
            dataset_id = datasets[v_handle.dataset_name]
            rows = self.datastore.get_dataset_chart(dataset_id, v_handle)
            ref = self.urls.workflow_module_view_url(project_id, branch_id,  version, module_id,  view_id)
            return serialize.DATASET_CHART_VIEW(
                view=v_handle,
                rows=rows,
                self_ref=self.urls.workflow_module_view_url(
                    project_id,
                    branch_id,
                    version,
                    module_id,
                    view_id
                )
            )
