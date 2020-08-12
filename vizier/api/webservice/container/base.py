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

from typing import Dict, Any

from vizier.api.routes.container import ContainerApiUrlFactory
from vizier.api.webservice.container.task import VizierContainerTaskApi
from vizier.api.webservice.datastore import VizierDatastoreApi
from vizier.api.webservice.filestore import VizierFilestoreApi
from vizier.api.webservice.view import VizierDatasetViewApi
from vizier.core import VERSION_INFO
from vizier.core.timestamp import get_current_time
from vizier.engine.base import VizierEngine
from vizier.config.container import ContainerConfig
import vizier.api.serialize.base as serialize
import vizier.api.serialize.labels as labels
from vizier.api.webservice.base import get_engine


class VizierContainerApi(object):
    """
    """
    def __init__(self, config: ContainerConfig, init: bool = False):
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
        self.engine: VizierEngine
        self.datasets: VizierDatastoreApi
        self.files: VizierFilestoreApi
        self.tasks: VizierContainerTaskApi
        self.urls: ContainerApiUrlFactory
        self.service_descriptor: Dict[str, Any]
        self.views: VizierDatasetViewApi
        if init:
            self.init()

    def init(self) -> None:
        """Initialize the API before the first request."""
        # Initialize the API compinents
        self.urls = ContainerApiUrlFactory(
            base_url=self.config.app_base_url,
            api_doc_url=self.config.webservice.doc_url
        )
        self.engine = get_engine(self.config)
        self.projects =self.engine.projects
        self.datasets = VizierDatastoreApi(
            projects=self.projects,
            urls=self.urls,
            defaults=self.config.webservice.defaults
        )
        self.views = VizierDatasetViewApi(
            projects=self.projects,
            urls=self.urls
        )
        self.files = VizierFilestoreApi(
            projects=self.projects,
            urls=self.urls
        )
        self.tasks = VizierContainerTaskApi(
            engine=self.engine,
            controller_url=self.config.controller_url
        )
        # Initialize the service descriptor
        self.service_descriptor = {
            'name': self.config.webservice.name,
            'startedAt': get_current_time().isoformat(),
            'defaults': {
                'maxFileSize': self.config.webservice.defaults.max_file_size
            },
            'environment': {
                'name': self.engine.name,
                'version': VERSION_INFO,
                'backend': self.config.engine.backend.identifier,
                'packages': list(self.engine.packages.keys())
            },
            labels.LINKS: serialize.HATEOAS({
                'self': self.urls.service_descriptor(),
                'doc': self.urls.api_doc()
            })
        }


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

# def get_engine(config: ContainerConfig) -> VizierEngine:
#     """Create instance of vizier engine using the default datastore, filestore
#     and viztrails factories. The default engine may use a multi-process backend
#     or a celery backend.

#     Parameters
#     ----------
#     config: vizier.config.app.AppConfig
#         Application configuration object

#     Returns
#     -------
#     vizier.engine.base.VizierEngine
#     """
#     # Get backend identifier. Raise ValueError if value does not identify
#     # a valid backend.
#     backend_id = config.engine.backend.identifier
#     if not backend_id in base.BACKENDS:
#         raise ValueError('unknown backend \'' + str(backend_id) + '\'')
#     # By default the vizier engine uses the objectstore implementation for
#     # the viztrails repository. The datastore and filestore factories depend
#     # on the values of engine identifier (DEV or MIMIR).
#     base_dir = config.engine.data_dir
#     if config.engine.identifier in [base.DEV_ENGINE, base.MIMIR_ENGINE]:
#         filestores_dir = os.path.join(base_dir, app.DEFAULT_FILESTORES_DIR)
#         filestore_factory=FileSystemFilestoreFactory(filestores_dir)
#         datastores_dir = os.path.join(base_dir, app.DEFAULT_DATASTORES_DIR)
#         datastore_factory: DatastoreFactory
#         if config.engine.identifier == base.DEV_ENGINE:
#             datastore_factory = FileSystemDatastoreFactory(datastores_dir)
#         else:
#             datastore_factory = MimirDatastoreFactory(datastores_dir)
#     else:
#         raise ValueError('unknown vizier engine \'' + str(config.engine.identifier) + '\'')
#     # The default engine uses a common project cache.
#     projects = SingleProjectCache(
#         ProjectHandle(
#             viztrail=ViztrailHandle(identifier=config.project_id),
#             datastore=datastore_factory.get_datastore(config.project_id),
#             filestore=filestore_factory.get_filestore(config.project_id)
#         )
#     )
#     # Create workflow execution backend and processor for synchronous task
#     packages = load_packages(config.engine.package_path)
#     processors = load_processors(config.engine.processor_path)
#     # Create the backend
#     if backend_id == base.BACKEND_MULTIPROCESS:
#         backend = MultiProcessBackend(
#             processors=processors,
#             projects=projects,
#             synchronous=None
#         )
#     elif backend_id == base.BACKEND_CELERY:
#         # Create and configure routing information (if given)
#         from vizier.config.celery import config_routes
#         backend = CeleryBackend(
#             routes=config_routes(config),
#             synchronous=None
#         )
#     else:
#         # For completeness. Validity of the backend id is be checked before.
#         raise ValueError('unknown backend \'' + str(backend_id) + '\'')
#     return VizierEngine(
#         name=config.engine.identifier + ' (' + backend_id + ')',
#         projects=projects,
#         backend=backend,
#         packages=packages
#     )
