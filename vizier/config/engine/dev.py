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

"""Factory for the development engine. The engine uses the default file system
datastore and filestore factory and the objectstore viztrail repository.

The structure of the configuration parameters is as follows:

backend: Name of the backend: MULTIPROCESS or CELERY
datastores: Path to datastores base directory
filestores: Path to filestores base directory
packages: List of packages
    - declaration: Path to package declaration file
      engine: Class loader for package processor
options:
    useShortIdentifier: Use eight character identifier if True
routing: Routing information for the Celery backend
    - packageId
      commandId
      queue: Name of the queue
synchronous: List of commands that are executed synchronously
    - packageId: Unique package identifier
      commandId: Unique command identifier
viztrails: Path to viztrails repository base directory

The name of the backend is optional. By default, the multi-process backend is
used.

The list synchronous commands is optional. If specified that same processors
will be used to execute synchronous and assunchronous commands.
"""
from vizier.config.engine.base import load_packages
from vizier.config.engine.celery import config_routes
from vizier.core.io.base import DefaultObjectStore
from vizier.core.util import get_short_identifier, get_unique_identifier
from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.base import VizierEngine
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.backend.remote.celery.base import CeleryBackend
from vizier.engine.backend.synchron import SynchronousTaskEngine
from vizier.engine.project.cache.common import CommonProjectCache
from vizier.filestore.fs.factory import FileSystemFilestoreFactory
from vizier.viztrail.driver.objectstore.repository import OSViztrailRepository


"""Unique factory identifier."""
DEV_ENGINE = 'DEV'

"""Configuration parameters."""
PARA_BACKEND = 'backend'
PARA_DATASTORES = 'datastores'
PARA_FILESTORES = 'filestores'
PARA_OPTIONS = 'options'
PARA_OPTIONS_USESHORTID = 'useShortIdentifier'
PARA_PACKAGES = 'packages'
PARA_ROUTING = 'routing'
PARA_SYNCHRONOUS = 'synchronous'
PARA_SYNCHRONOUS_COMMAND = 'commandId'
PARA_SYNCHRONOUS_PACKAGE = 'packageId'
PARA_VIZTRAILS = 'viztrails'

MANDATORY_PARAMETERS = [
    PARA_DATASTORES,
    PARA_FILESTORES,
    PARA_PACKAGES,
    PARA_VIZTRAILS
]

"""Supported backends."""
BACKEND_CELERY = 'CELERY'
BACKEND_MULTIPROCESS = 'MULTIPROCESS'
DEFAULT_BACKEND = BACKEND_MULTIPROCESS


class DevEngineFactory(object):
    """Factory for the vizier engine that is used for development and testing.
    """
    @staticmethod
    def get_engine(properties):
        """Create instance of development engine using the default datastore,
        filestore and viztrails factories. The development engine uses a
        multi-process backend.

        Parameters
        ----------
        properties: dict, optional
            Configuration-specific parameters

        Returns
        -------
        vizier.engine.base.VizierEngine
        """
        # Ensure that configuration parameters are given
        if properties is None:
            raise ValueError('missing configuration parameters')
        # Ensure that mandatory parameters are given
        for key in MANDATORY_PARAMETERS:
            if not key in properties:
                raise ValueError('missing patameter \'' + key + '\'')
        # Load package information
        packages, processors = load_packages(properties[PARA_PACKAGES])
        # The development engine uses a common project cache with file system
        # based factories for datastores, filestores, and viztrails
        datastores_dir = properties[PARA_DATASTORES]
        filestores_dir = properties[PARA_FILESTORES]
        viztrails_dir = properties[PARA_VIZTRAILS]
        id_factory = get_unique_identifier
        if PARA_OPTIONS in properties:
            if PARA_OPTIONS_USESHORTID in properties[PARA_OPTIONS]:
                if properties[PARA_OPTIONS][PARA_OPTIONS_USESHORTID]:
                    id_factory = get_short_identifier
        projects = CommonProjectCache(
            datastores=FileSystemDatastoreFactory(datastores_dir),
            filestores=FileSystemFilestoreFactory(filestores_dir),
            viztrails=OSViztrailRepository(
                base_path=viztrails_dir,
                object_store=DefaultObjectStore(
                    identifier_factory=id_factory
                )
            )
        )
        # Create an optional task processor for synchronous tasks if given
        synchronous = None
        if PARA_SYNCHRONOUS in properties:
            commands = dict()
            for el in properties[PARA_SYNCHRONOUS]:
                for key in [PARA_SYNCHRONOUS_COMMAND, PARA_SYNCHRONOUS_PACKAGE]:
                    if not key in el:
                        raise ValueError('missing element \'' + key + '\' for synchronous command')
                package_id = el[PARA_SYNCHRONOUS_PACKAGE]
                command_id = el[PARA_SYNCHRONOUS_COMMAND]
                if not package_id in commands:
                    commands[package_id] = dict()
                commands[package_id][command_id] = processors[package_id]
            synchronous = SynchronousTaskEngine(
                commands=commands,
                projects=projects
            )
        # Create the backend
        if PARA_BACKEND in properties:
            backend_name = properties[PARA_BACKEND]
        else:
            backend_name = DEFAULT_BACKEND
        if backend_name == BACKEND_MULTIPROCESS:
            backend = MultiProcessBackend(
                processors=processors,
                projects=projects,
                synchronous=synchronous
            )
        elif backend_name == BACKEND_CELERY:
            # Create and configure routing information (if given)
            routes = None
            if PARA_ROUTING in properties:
                routes = config_routes(properties[PARA_ROUTING])
            backend = CeleryBackend(
                routes=routes,
                synchronous=synchronous
            )
        else:
            raise ValueError('unknown backend \'' + str(backend_name) + '\'')
        return VizierEngine(
            name='Development Engine',
            version='0.0.1',
            projects=projects,
            backend=backend,
            packages=packages
        )
