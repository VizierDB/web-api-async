# Copyright (C) 2018 New York University
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

"""Application configuration object. Contains all application settings and the
configuration parameters for the vizier engine.

The app configuration has four main parts: API and Web service configuration,
the vizier engine configuration, the path to the server log, and the
debug flag for running the server in debugging mode. The structure of the
configuration object is as follows:

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
"""

import os

from vizier.config.base import ConfigObject, read_object_from_file
from vizier.config.base import ENV_DIRECTORY
from vizier.core.loader import ClassLoader


"""Environment Variable containing path to config file."""
ENV_CONFIG = 'VIZIERSERVER_CONFIG'

"""Default settings. The default settings do not include the configuration for
the vizier engine.
"""
DEFAULT_SETTINGS = {
    'webservice': {
        'server_url': 'http://localhost',
        'server_port': 5000,
        'server_local_port': 5000,
        'app_path': '/vizier-db/api/v1',
        'doc_url': 'http://cds-swg1.cims.nyu.edu/doc/vizier-db/',
        'name': 'Vizier Web API',
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


class AppConfig(object):
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

        (1) The file referenced by the environment variable VIZIERSERVER_CONFIG
        (2) The file config.yaml in the current working directory

        If the specified files are not found the default configuration is used.

        Parameters
        ----------
        configuration_file: string, optional
            Optional path to configuration file
        """
        # Read configuration from given configuration file or one of the
        # options: value oin environment variable, local config.json file, or
        # local config.yaml file. Use the default settings if no configuration
        # file is found.
        doc = None
        if configuration_file != None:
            if os.path.isfile(configuration_file):
                doc = read_object_from_file(configuration_file)
            else:
                raise ValueError('unknown file \'' + str(configuration_file) + '\'')
        else:
            files = [
                os.getenv(ENV_CONFIG),
                './config.json',
                './config.yaml'
            ]
            for config_file in files:
                if not config_file is None and os.path.isfile(config_file):
                    doc = read_object_from_file(config_file)
                    break
        if doc is None:
            raise ValueError('no configuration object found')
        # Create the vizier engine
        if 'engine' in doc:
            for key in ['identifier', 'properties']:
                if not key in doc['engine']:
                    raise ValueError('missing key \'' + key + '\' for engine configuration')
            setattr(self, 'engine', doc['engine'])
        else:
            raise ValueError('missing engine configuration')
        # Create objects for configuration of log files and web service.
        for key in ['logs', 'webservice']:
            if key in doc:
                obj = ConfigObject(
                    values=doc[key],
                    default_values=DEFAULT_SETTINGS[key]
                )
            else:
                obj = ConfigObject(values=DEFAULT_SETTINGS[key])
            setattr(self, key, obj)
        # Set debug flag if given
        if 'debug' in doc:
            self.debug = doc['debug']
        else:
            self.debug = DEFAULT_SETTINGS['debug']

    @property
    def app_base_url(self):
        """Full Url (prefix) for all resources that are available on the Web
        Service. this is a concatenation of the service Url, port number and
        application path.

        Returns
        -------
        string
        """
        base_url = self.webservice.server_url
        if self.webservice.server_port != 80:
            base_url += ':' + str(self.webservice.server_port)
        base_url += self.webservice.app_path
        return base_url
