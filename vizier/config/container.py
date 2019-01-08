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

"""Container server configuration object. Contains all application settings and
the configuration parameters for the vizier engine.

The container app configuration is similar to the app config. It has four main
parts: API and Web service configuration, the vizier engine configuration, the
path to the server log, and the debug flag for running the server in debugging
mode. The structure of the configuration object is as follows:

webservice:
    server_url: Url of the server (e.g., http://localhost)
    server_port: Public server port (e.g., 5000)
    server_local_port: Locally bound server port (e.g., 5000)
    app_path: Application path for Web API (e.g., /vizier-db/api/v1)
    app_base_url: Concatenation of server_url, server_port and app_path
    doc_url: Url to API documentation
    name: Web Service name
    defaults:
        row_limit: Default row limit for requests that read datasets
        max_row_limit: Maximum row limit for requests that read datasets (-1 = all)
        max_file_size: Maximum size for file uploads (in byte)
engine:
    identifier: Unique engine configuration identifier
    properties: Configuration specific parameters
debug: Flag indicating whether server is started in debug mode
logs:
    server: Log file for Web Server

The main difference to the app config is the format of the engine dictionary
properties.
"""

import os

from vizier.config.base import ConfigObject, ServerConfig
from vizier.config.base import ENV_DIRECTORY


"""Environment Variable containing path to config file."""
ENV_CONFIG = 'VIZIERCONTAINERSERVER_CONFIG'

"""Default settings. The default settings do not include the configuration for
the vizier engine.
"""
DEFAULT_SETTINGS = {
    'webservice': {
        'server_url': 'http://localhost',
        'server_port': 5000,
        'server_local_port': 5000,
        'app_path': '/vizier-db/api/v1',
        'doc_url': 'http://cds-swg1.cims.nyu.edu/doc/vizier-db-container/',
        'name': 'Vizier Container Web API',
        'defaults': {
            'row_limit': 25,
            'max_row_limit': -1,
            'max_file_size': 16 * 1024 * 1024
        }
    },
    'logs': {
        'server': os.path.join(ENV_DIRECTORY, 'logs')
    },
    'debug': True,
}


class ContainerAppConfig(ServerConfig):
    """Application configuration object. This object contains all configuration
    settings for the Vizier instance.

    Configuration is read from a configuration file (in Yaml or Json format).
    The file is expected to contain follow the structure of the configuration
    object as described above. The configuration file can either be specified
    using the environment variable VIZIERSERVER_CONFIG or be located (as file
    config.yaml) in the current working directory.
    """
    def __init__(self, configuration_file=None):
        """Read configuration parameters from a configuration file. The file is
        expected to be in Json or Yaml format, having the same structure as
        shown above.

        If no file is specified attempts will be made to read the following
        configuration files:

        (1) The file referenced in the environment variable
            VIZIERCONTAINERSERVER_CONFIG
        (2) The file config.json or config.yaml in the current working directory

        If the specified files are not found a ValueError is raised.

        Parameters
        ----------
        configuration_file: string, optional
            Optional path to configuration file
        """
        super(ContainerAppConfig, self).__init__(
            env=ENV_CONFIG,
            default_settings=DEFAULT_SETTINGS,
            configuration_file=configuration_file
        )
        # Expects an additional configuration element containing the project
        # identifier
        if not 'project_id' in self.doc:
            raise ValueError('missing project identifier')
        self.project_id = self.doc['project_id']
