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

import os

from vizier.api.webservice.branch import VizierBranchApi
from vizier.api.webservice.datastore import VizierDatastoreApi
from vizier.api.webservice.filestore import VizierFilestoreApi
from vizier.api.webservice.project import VizierProjectApi
from vizier.api.webservice.task import VizierTaskApi
from vizier.api.webservice.view import VizierDatasetViewApi
from vizier.api.webservice.workflow import VizierWorkflowApi
from vizier.api.routes.base import UrlFactory
from vizier.config.celery import config_routes
from vizier.core import VERSION_INFO
from vizier.core.io.base import DefaultObjectStore
from vizier.core.timestamp import get_current_time
from vizier.core.util import get_short_identifier, get_unique_identifier
from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.backend.remote.celery.base import CeleryBackend
from vizier.engine.backend.synchron import SynchronousTaskEngine
from vizier.engine.base import VizierEngine
from vizier.engine.packages.load import load_packages
from vizier.engine.project.cache.common import CommonProjectCache
from vizier.engine.task.processor import load_processors
from vizier.filestore.fs.factory import FileSystemFilestoreFactory
from vizier.viztrail.objectstore.repository import OSViztrailRepository

import vizier.api.serialize.base as serialize
import vizier.api.serialize.labels as labels
import vizier.config.app as app
import vizier.config.base as base


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
        self.engine = get_engine(self.config)
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


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_engine(config):
    """Create instance of development engine using the default datastore,
    filestore and viztrails factories.  The default engine may use a
    multi-process backend or a celery backend.

    Parameters
    ----------
    config: vizier.config.app.AppConfig
        Application configuration object

    Returns
    -------
    vizier.engine.base.VizierEngine
    """
    # We ignore the identifier here since there is only one dev engine at
    # this time.
    default_values = {
        app.VIZIERENGINE_BACKEND: base.BACKEND_MULTIPROCESS,
        app.VIZIERENGINE_USE_SHORT_IDENTIFIER: True,
        app.VIZIERENGINE_DATA_DIR: base.ENV_DIRECTORY,
        app.VIZIERENGINE_SYNCHRONOUS: None
    }
    # Get backend identifier. Raise ValueError if value does not identify
    # a valid backend.
    backend_id = base.get_config_value(
        env_variable=app.VIZIERENGINE_BACKEND,
        default_values=default_values
    )
    if not backend_id in base.BACKENDS:
        raise ValueError('unknown backend \'' + str(backend_id) + '\'')
    # Get the identifier factory for the viztrails repository and create
    # the object store. At this point we use the default object store only.
    # We could add another environment variable to use different object
    # stores (once implemented).
    use_short_ids = base.get_config_value(
        env_variable=app.VIZIERENGINE_USE_SHORT_IDENTIFIER,
        attribute_type=base.BOOL,
        default_values=default_values
    )
    if use_short_ids:
        id_factory = get_short_identifier
    else:
        id_factory = get_unique_identifier
    object_store = DefaultObjectStore(
        identifier_factory=id_factory
    )
    # By default the vizier engine uses the objectstore implementation for
    # the viztrails repository. The datastore and filestore factories depend
    # on the values of engine identifier (DEV or MIMIR).
    base_dir = base.get_config_value(
        env_variable=app.VIZIERENGINE_DATA_DIR,
        default_values=default_values
    )
    viztrails_dir = os.path.join(base_dir, app.DEFAULT_VIZTRAILS_DIR)
    if config.engine.identifier == base.DEV_ENGINE:
        datastores_dir = os.path.join(base_dir, app.DEFAULT_DATASTORES_DIR)
        filestores_dir = os.path.join(base_dir, app.DEFAULT_FILESTORES_DIR)
        datastore_factory=FileSystemDatastoreFactory(datastores_dir)
        filestore_factory=FileSystemFilestoreFactory(filestores_dir)
    else:
        raise ValueError('unknown vizier engine \'' + str(config.engine.identifier) + '\'')
    # The default engine uses a common project cache.
    projects = CommonProjectCache(
        datastores=datastore_factory,
        filestores=filestore_factory,
        viztrails=OSViztrailRepository(
            base_path=viztrails_dir,
            object_store=object_store
        )
    )
    # Create workflow execution backend and processor for synchronous task
    packages = load_packages(config.engine.package_path)
    processors = load_processors(config.engine.processor_path)
    # Create an optional task processor for synchronous tasks if given
    synchronous = None
    sync_commands_list = base.get_config_value(
        env_variable=app.VIZIERENGINE_SYNCHRONOUS,
        default_values=default_values
    )
    if not sync_commands_list is None:
        commands = dict()
        for el in sync_commands_list.split(':'):
            package_id, command_id = el.split('.')
            if not package_id in commands:
                commands[package_id] = dict()
            commands[package_id][command_id] = processors[package_id]
        synchronous = SynchronousTaskEngine(
            commands=commands,
            projects=projects
        )
    # Create the backend
    if backend_id == base.BACKEND_MULTIPROCESS:
        backend = MultiProcessBackend(
            processors=processors,
            projects=projects,
            synchronous=synchronous
        )
    elif backend_id == base.BACKEND_CELERY:
        # Create and configure routing information (if given)
        backend = CeleryBackend(
            routes=config_routes(),
            synchronous=synchronous
        )
    else:
        # For completeness. Validity of the backend id is be checked before.
        raise ValueError('unknown backend \'' + str(backend_id) + '\'')
    return VizierEngine(
        name='Development Engine',
        version='0.0.1',
        projects=projects,
        backend=backend,
        packages=packages
    )
