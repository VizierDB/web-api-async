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

from vizier.api.routes.container import ContainerUrlFactory
from vizier.api.webservice.datastore import VizierDatastoreApi
from vizier.api.webservice.filestore import VizierFilestoreApi
from vizier.config.engine.factory import VizierEngineFactory
from vizier.core import VERSION_INFO
from vizier.core.timestamp import get_current_time
from vizier.engine.project.base import ProjectHandle
from vizier.engine.project.cache.container import ContainerProjectCache
from vizier.viztrail.base import ViztrailHandle

import vizier.api.serialize.base as serialize
import vizier.api.serialize.labels as labels


class VizierContainerApi(object):
    """
    """
    def __init__(self, config, init=False):
        """Initialize the API components.

        Parameters
        ----------
        config: vizier.config.app.ContainerAppConfig
            Container application configuration object
        init: bool, optional
            Defer initialization if False
        """
        self.config = config
        # Set the API components to None for now. It is assumed that the .init()
        # method is called before any of the components are accessed for the
        # first time
        self.engine = None
        self.datasets = None
        self.files = None
        self.tasks = None
        self.urls = None
        self.service_descriptor = None
        if init:
            self.init()

    def init(self):
        """Initialize the API before the first request."""
        # Initialize the API compinents
        self.urls = ContainerUrlFactory(
            base_url=self.config.app_base_url,
            api_doc_url=self.config.webservice.doc_url
        )
        self.engine = VizierEngineFactory.get_container_engine(
            identifier=self.config.engine['identifier'],
            properties=self.config.engine['properties']
        )
        self.projects = ContainerProjectCache(
            ProjectHandle(
                viztrail=ViztrailHandle(identifier=self.config.project_id),
                datastore=self.engine.datastore,
                filestore=self.engine.filestore
            )
        )
        self.datasets = VizierDatastoreApi(
            projects=self.projects,
            urls=self.urls,
            defaults=self.config.webservice.defaults
        )
        self.files = VizierFilestoreApi(
            projects=self.projects,
            urls=self.urls
        )
        # Initialize the service descriptor
        self.service_descriptor = {
            'name': self.config.webservice.name,
            'version': VERSION_INFO,
            'startedAt': get_current_time().isoformat(),
            'defaults': {
                'maxFileSize': self.config.webservice.defaults.max_file_size
            },
            'environment': {
                'identifier': self.config.engine['identifier']
            },
            labels.LINKS: serialize.HATEOAS({
                'self': self.urls.service_descriptor(),
                'doc': self.urls.api_doc()
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
