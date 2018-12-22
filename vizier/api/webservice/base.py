# Copyright (C) 2018 New York University,
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

"""Vizier API - Implements all methods of the API to interact with a running
Vizier instance.

The API orchestrates the interplay
between different components such as the viztrail repository that manages
viztrails and the workflow engine that executes modules in viztrail workflow
versions.

Internally the API is further divided into four parts that deal with the file
store, data store, viztrail repository and the workflow execution engine.
"""

from vizier.api.webservice.branch import VizierBranchApi
from vizier.api.webservice.datastore import VizierDatastoreApi
from vizier.api.webservice.filestore import VizierFilestoreApi
from vizier.api.webservice.project import VizierProjectApi
from vizier.api.webservice.task import VizierTaskApi
from vizier.api.webservice.workflow import VizierWorkflowApi
from vizier.api.routes.base import UrlFactory
from vizier.core import VERSION_INFO
from vizier.core.timestamp import get_current_time

import vizier.api.serialize.base as serialize
import vizier.api.serialize.labels as labels


class VizierApi(object):
    """The Vizier API implements the methods that correspond to requests that
    are supported by the Vizier Web Service. the API, however, can also be used
    in a stand-alone manner, e.g., via the command line interpreter tool.

    This class is a wrapper around the different components of the Vizier system
    that are necessary for the Web Service, i.e., file store, data store,
    viztrail repository, and workflow execution engine.
    """
    def __init__(self, config):
        """Initialize the API components.

        Parameters
        ----------
        config: vizier.config.app.AppConfig
            Application configuration object
        """
        self.engine = config.engine
        self.urls = UrlFactory(base_url=config.app_base_url, api_doc_url=config.webservice.doc_url)
        # Set the API components
        self.branches = VizierBranchApi(engine=self.engine, urls=self.urls)
        self.datasets = VizierDatastoreApi(
            engine=self.engine,
            urls=self.urls,
            defaults=config.webservice.defaults
        )
        self.files = VizierFilestoreApi(engine=self.engine, urls=self.urls)
        self.projects = VizierProjectApi(engine=self.engine, urls=self.urls)
        self.tasks = VizierTaskApi(engine=self.engine)
        self.workflows = VizierWorkflowApi(engine=self.engine, urls=self.urls)
        # Initialize the service descriptor
        self.service_descriptor = {
            'name': config.webservice.name,
            'version': VERSION_INFO,
            'startedAt': get_current_time().isoformat(),
            'defaults': {
                'maxFileSize': config.webservice.defaults.max_file_size
            },
            'environment': {
                'name': self.engine.name,
                'version': self.engine.version
            },
            labels.LINKS: serialize.HATEOAS({
                'self': self.urls.service_descriptor(),
                'doc': self.urls.api_doc(),
                'project:create': self.urls.create_project(),
                'project:list': self.urls.list_projects()
            })
        }

    def init(self):
        """Initialize the API before the first request."""
        self.engine.init()

    # --------------------------------------------------------------------------
    # Service
    # --------------------------------------------------------------------------
    def service_overview(self):
        """Returns a dictionary containing essential information about the web
        service including HATEOAS links to access resources and interact with
        the service.

        Returns
        -------
        dict
        """
        return self.service_descriptor

    # ------------------------------------------------------------------------------
    # Views
    # ------------------------------------------------------------------------------
    def get_dataset_chart_view(self, project_id, branch_id, version, module_id, view_id):
        """
        """
        # Get viztrail to ensure that it exist.
        viztrail = self.viztrails.get_viztrail(viztrail_id=project_id)
        if viztrail is None:
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
