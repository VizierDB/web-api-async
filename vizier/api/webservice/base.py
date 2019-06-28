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
from vizier.api.routes.container import ContainerEngineUrlFactory
from vizier.config.celery import config_routes
from vizier.core import VERSION_INFO
from vizier.core.io.base import DefaultObjectStore
from vizier.core.timestamp import get_current_time
from vizier.core.util import get_short_identifier, get_unique_identifier
from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.datastore.mimir.factory import MimirDatastoreFactory
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.backend.remote.celery.base import CeleryBackend
from vizier.engine.backend.remote.container import ContainerBackend
from vizier.engine.backend.synchron import SynchronousTaskEngine
from vizier.engine.base import VizierEngine
from vizier.engine.packages.load import load_packages
from vizier.engine.project.cache.common import CommonProjectCache
from vizier.engine.project.cache.container import ContainerProjectCache
from vizier.engine.task.processor import load_processors
from vizier.filestore.fs.factory import FileSystemFilestoreFactory
from vizier.viztrail.objectstore.repository import OSViztrailRepository

import vizier.api.serialize.base as serialize
import vizier.api.serialize.hateoas as ref
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
        self.engine = get_engine(self.config)
        self.urls = get_url_factory(
            config=self.config,
            projects=self.engine.projects
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
            urls=self.urls
        )
        self.tasks = VizierTaskApi(engine=self.engine)
        self.workflows = VizierWorkflowApi(engine=self.engine, urls=self.urls)
        # Initialize the service descriptor. The service descriptor contains
        # the list of packages and commands that are supported by the engine
        package_listing = list()
        for pckg in self.engine.packages.values():
            pckg_obj = {'id': pckg.identifier, 'name': pckg.name}
            if not pckg.description is None:
                pckg_obj['description'] = pckg.description
            pckg_commands = list()
            for cmd in pckg.commands.values():
                cmd_obj = {'id': cmd.identifier, 'name': cmd.name}
                if not cmd.description is None:
                    cmd_obj['description'] = cmd.description
                cmd_obj['parameters'] = cmd.parameters.values()
                pckg_commands.append(cmd_obj)
            pckg_obj['commands'] = pckg_commands
            package_listing.append(pckg_obj)
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
                'packages': package_listing
            },
            labels.LINKS: serialize.HATEOAS({
                ref.SELF: self.urls.service_descriptor(),
                ref.API_DOC: self.urls.api_doc(),
                ref.PROJECT_CREATE: self.urls.create_project(),
                ref.PROJECT_LIST: self.urls.list_projects(),
                ref.PROJECT_IMPORT: self.urls.import_project()
            })
        }


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_engine(config):
    """Create instance of the default vizual engine using the default datastore,
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
    # Get backend identifier. Raise ValueError if value does not identify
    # a valid backend.
    backend_id = config.engine.backend.identifier
    if not backend_id in base.BACKENDS:
        raise ValueError('unknown backend \'' + str(backend_id) + '\'')
    # Get the identifier factory for the viztrails repository and create
    # the object store. At this point we use the default object store only.
    # We could add another environment variable to use different object
    # stores (once implemented).
    if config.engine.use_short_ids:
        id_factory = get_short_identifier
    else:
        id_factory = get_unique_identifier
    object_store = DefaultObjectStore(
        identifier_factory=id_factory
    )
    # Create index of supported packages
    packages = load_packages(config.engine.package_path)
    # By default the vizier engine uses the objectstore implementation for
    # the viztrails repository. The datastore and filestore factories depend
    # on the values of engine identifier (DEV or MIMIR).
    base_dir = config.engine.data_dir
    # Create the local viztrails repository
    viztrails = OSViztrailRepository(
        base_path=os.path.join(base_dir, app.DEFAULT_VIZTRAILS_DIR),
        object_store=object_store
    )
    if config.engine.identifier in [base.DEV_ENGINE, base.MIMIR_ENGINE]:
        filestores_dir = os.path.join(base_dir, app.DEFAULT_FILESTORES_DIR)
        filestore_factory=FileSystemFilestoreFactory(filestores_dir)
        datastores_dir = os.path.join(base_dir, app.DEFAULT_DATASTORES_DIR)
        if config.engine.identifier == base.DEV_ENGINE:
            datastore_factory = FileSystemDatastoreFactory(datastores_dir)
        else:
            datastore_factory = MimirDatastoreFactory(datastores_dir)
        # The default engine uses a common project cache.
        projects = CommonProjectCache(
            datastores=datastore_factory,
            filestores=filestore_factory,
            viztrails=viztrails
        )
        # Get set of task processors for supported packages
        processors = load_processors(config.engine.processor_path)
        # Create an optional task processor for synchronous tasks if given
        synchronous = None
        sync_commands_list = config.engine.sync_commands
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
                routes=config_routes(config),
                synchronous=synchronous
            )
        else:
            # Not all combinations of engine identifier and backend identifier
            # are valid.
            raise ValueError('invalid backend \'' + str(backend_id) + '\'')
    elif config.engine.identifier == base.CONTAINER_ENGINE:
        if backend_id == base.BACKEND_CONTAINER:
            projects = ContainerProjectCache(
                viztrails=viztrails,
                container_file=os.path.join(base_dir, app.DEFAULT_CONTAINER_FILE),
                config=config
            )
            backend = ContainerBackend(projects=projects)
        else:
            # The container engine only supports a single backend type.
            raise ValueError('invalid backend \'' + str(backend_id) + '\'')
    else:
        raise ValueError('unknown vizier engine \'' + str(config.engine.identifier) + '\'')
    return VizierEngine(
        name=config.engine.identifier + ' (' + backend_id + ')',
        projects=projects,
        backend=backend,
        packages=packages
    )


def get_url_factory(config, projects):
    """Get the url factory for a given configuration. In most cases we use the
    default url factory. Only for the configuration where each project is
    running in a separate container we need a different factory.

    Parameter
    ---------
    config: vizier.config.app.AppConfig
        Application configuration object
    projects: vizier.engine.project.cache.base.ProjectCache
        Cache for projects (only used for container engine)

    Returns
    -------
    vizier.api.routes.base.UrlFactory
    """
    if config.engine.identifier == base.CONTAINER_ENGINE:
        return ContainerEngineUrlFactory(
            base_url=config.app_base_url,
            api_doc_url=config.webservice.doc_url,
            projects=projects
        )
    else:
        return UrlFactory(
            base_url=config.app_base_url,
            api_doc_url=config.webservice.doc_url
        )
