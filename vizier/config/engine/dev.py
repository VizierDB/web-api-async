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

import os

from vizier.config.engine.celery import config_routes
from vizier.core.io.base import DefaultObjectStore
from vizier.core.util import get_short_identifier, get_unique_identifier
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.base import VizierEngine
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.backend.remote.celery.base import CeleryBackend
from vizier.engine.backend.synchron import SynchronousTaskEngine
from vizier.engine.project.cache.common import CommonProjectCache
from vizier.filestore.fs.base import FileSystemFilestore
from vizier.filestore.fs.factory import FileSystemFilestoreFactory
from vizier.viztrail.objectstore.repository import OSViztrailRepository

import vizier.config.base as base


"""Unique factory identifier."""
DEV_ENGINE = 'DEV'

ENGINES = [DEV_ENGINE]


"""Environment variables that contain additinaal Configuration parameters."""
# Base data directory for the development engine
VIZIERENGINE_DEV_DIR = 'VIZIERENGINE_DEV_DIR'


class DevEngineFactory(object):
    """Factory for the vizier engine that is used for development and testing.
    """
    @staticmethod
    def get_engine(identifier, config):
        """Create instance of development engine using the default datastore,
        filestore and viztrails factories.  The development engine may use a
        multi-process backend or a celery backend.

        Parameters
        ----------
        identifier: string
            Unique configuration identifier
        config: vizier.config.base.ServerConfig
            Server configuration object

        Returns
        -------
        vizier.engine.base.VizierEngine
        """
        # We ignore the identifier here since there is only one dev engine at
        # this time.
        default_values = {
            base.VIZIERENGINE_BACKEND: base.BACKEND_MULTIPROCESS,
            base.VIZIERENGINE_USE_SHORT_IDENTIFIER: True,
            VIZIERENGINE_DEV_DIR: base.ENV_DIRECTORY,
            base.VIZIERENGINE_SYNCHRONOUS: None
        }
        backend_id = base.get_config_value(
            env_variable=base.VIZIERENGINE_BACKEND,
            default_values=default_values
        )
        if not backend_id in base.BACKENDS:
            raise ValueError('unknown backend \'' + str(backend_id) + '\'')
        dev_dir = base.get_config_value(
            env_variable=VIZIERENGINE_DEV_DIR,
            default_values=default_values
        )
        # The development engine uses a common project cache with file system
        # based factories for datastores, filestores, and viztrails
        datastores_dir = os.path.join(dev_dir, 'ds')
        filestores_dir = os.path.join(dev_dir, 'fs')
        viztrails_dir = os.path.join(dev_dir, 'vt')
        use_short_ids = base.get_config_value(
            env_variable=base.VIZIERENGINE_USE_SHORT_IDENTIFIER,
            attribute_type=base.BOOL,
            default_values=default_values
        )
        if use_short_ids:
            id_factory = get_short_identifier
        else:
            id_factory = get_unique_identifier
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
        packages = config.load_packages()
        processors = config.load_processors()
        # Create an optional task processor for synchronous tasks if given
        synchronous = None
        sync_commands_list = base.get_config_value(
            env_variable=base.VIZIERENGINE_SYNCHRONOUS,
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
