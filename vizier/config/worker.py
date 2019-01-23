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

"""Remote worker configuration object. Contains all settings to create instances
of the local filestore and datastore as well as the list of available packages.

packages: List of package processors
    - declaration: Path to package declaration file
      engine: Class loader for package processor
filestores:
    moduleName: Name of the module containing the used engine
    className: Class name of the used engine
    properties: Dictionary of engine specific configuration properties
datastores:
    moduleName: Name of the module containing the used engine
    className: Class name of the used engine
    properties: Dictionary of engine specific configuration properties
controller:
    url : Base Url for the controlling web service
logs:
    server: Log file for Web Server
"""

import os

import vizier.config.base as base


"""Environment variable used to configure remote celery workers."""
# Identifier for environment in which the worker operates (supported values are DEV or REMOTE) (DEFAULT: *DEV*)
VIZIERWORKER_ENV = 'VIZIERWORKER_ENV'
# Path to the task processor definitions for supported packages (DEFAULT: *./resources/processors*)
VIZIERWORKER_PROCESSOR_PATH = 'IZIERWORKER_PROCESSOR_PATH'
# Url of the controlling web service (DEFAULT: *http://localhost:5000/vizier-db/api/v1*)
VIZIERWORKER_CONTROLLER_URL = 'VIZIERWORKER_CONTROLLER_URL'
# Log file directory used by the worker (DEFAULT: ./.vizierdb/logs/worker)
VIZIERWORKER_LOG_DIR = 'VIZIERWORKER_LOG_DIR'

"""Dictionary of default worker configuration values."""
DEFAULT_SETTINGS = {
    VIZIERWORKER_LOG_DIR: os.path.join(base.ENV_DIRECTORY, 'logs', 'worker'),
    VIZIERWORKER_CONTROLLER_URL: 'http://localhost:5000/vizier-db/api/v1',
    VIZIERWORKER_ENV: base.DEV_ENGINE,
    VIZIERWORKER_PROCESSOR_PATH: './resources/processors/common:./resources/processors/dev'
}


class WorkerConfig(object):
    """Remote worker configuration object. This object contains all settings to
    create instances of the local filestore and datastore and the workflow
    controller.

    The object schema is as follows:

    controller:
        url
    env:
        identifier
        processor_path
    logs:
        worker
    """
    def __init__(self, default_values=None):
        """Create object instance from environment variables and optional set
        of default values.

        Parameters
        ----------
        default_values: dict, optional
            Dictionary of default values
        """
        if default_values is None:
            default_values = DEFAULT_SETTINGS
        # controller
        self.controller = base.ConfigObject(
            attributes=[('url', VIZIERWORKER_CONTROLLER_URL, base.STRING)],
            default_values=default_values
        )
        # env
        self.env = base.ConfigObject(
            attributes=[
                ('identifier', VIZIERWORKER_ENV, base.STRING),
                ('processor_path', VIZIERWORKER_PROCESSOR_PATH, base.STRING)
            ],
            default_values=default_values
        )
        # logs
        self.logs = base.ConfigObject(
            attributes=[('worker', VIZIERWORKER_LOG_DIR, base.STRING)],
            default_values=default_values
        )
