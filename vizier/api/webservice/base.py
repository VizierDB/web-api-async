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
from vizier.config.engine.factory import VizierEngineFactory
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
    def __init__(self, config, init=False):
        """Initialize the API components.

        Parameters
        ----------
        config: vizier.config.app.AppConfig
            Application configuration object
        init: bool, optional
            Defer initialization if False
        """
        self.config = config
        # Set the API components to None for now. It is assumed that the .init()
        # method is called before any of the components are accessed for the
        # first time
        self.engine = None
        self.branches = None
        self.datasets = None
        self.files = None
        self.projects = None
        self.tasks = None
        self.workflows = None
        self.urls = None
        self.service_descriptor = None
        self.views = None
        if init:
            self.init()

    def init(self):
        """Initialize the API before the first request."""
        # Initialize the API compinents
        self.urls = UrlFactory(
            base_url=self.config.app_base_url,
            api_doc_url=self.config.webservice.doc_url
        )
        self.engine = VizierEngineFactory.get_engine(
            identifier=self.config.engine['identifier'],
            properties=self.config.engine['properties']
        )
        self.branches = VizierBranchApi(
            projects=self.engine.projects,
            urls=self.urls
        )
        self.datasets = VizierDatastoreApi(
            projects=self.engine.projects,
            urls=self.urls,
            defaults=self.config.webservice.defaults
        )
        self.views = VizierDatasetViewApi(
            projects=self.engine.projects,
            urls=self.urls
        )
        self.files = VizierFilestoreApi(
            projects=self.engine.projects,
            urls=self.urls
        )
        self.projects = VizierProjectApi(
            projects=self.engine.projects,
            packages=self.engine.packages,
            urls=self.urls
        )
        self.tasks = VizierTaskApi(engine=self.engine)
        self.workflows = VizierWorkflowApi(engine=self.engine, urls=self.urls)
        # Initialize the service descriptor
        self.service_descriptor = {
            'name': self.config.webservice.name,
            'version': VERSION_INFO,
            'startedAt': get_current_time().isoformat(),
            'defaults': {
                'maxFileSize': self.config.webservice.defaults.max_file_size
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
