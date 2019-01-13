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

from vizier.config.base import ConfigObject, read_object_from_file
from vizier.config.base import ENV_DIRECTORY
from vizier.config.engine.base import load_packages
from vizier.core.loader import ClassLoader


"""Environment Variable containing path to config file."""
ENV_CONFIG = 'VIZIERWORKER_CONFIG'

"""Default settings."""
DEFAULT_SETTINGS = {
    'controller': {
        'url': 'http://localhost:5000/vizier-db/api/v1'
    },
    'logs': {
        'worker': os.path.join(ENV_DIRECTORY, 'logs')
    }
}


class WorkerConfig(object):
    """Remote worker configuration object. This object contains all settings to
    create instances of the local filestore and datastore as well as the list
    of available packages.

    Configuration is read from a configuration file (in Yaml or Json format).
    The file is expected to contain follow the structure of the configuration
    object as described above. The configuration file can either be specified
    using the environment variable VIZIERWORKER_CONFIG or be located (as file
    config.yaml) in the current working directory.
    """
    def __init__(self, configuration_file=None):
        """Read configuration parameters from a configuration file. The file is
        expected to be in Json or Yaml format, having the same structure as
        shown above.

        If no file is specified attempts will be made to read the following
        configuration files:

        (1) The file referenced by the environment variable VIZIERWORKER_CONFIG
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
        # Create registry of available packages.
        if 'packages' in doc:
            _, processors = load_packages(doc['packages'])
            self.processors = processors
        else:
            raise ValueError('missing package information')
        # Create a class loader instance for the engine.
        for key in ['filestores', 'datastores']:
            if key in doc:
                loader = ClassLoader(values=doc[key])
                setattr(self, key, loader)
            else:
                raise ValueError('missing configuration information \'' + key + '\'')
        # Create object for controlling web service.
        if 'controller' in doc:
            obj = ConfigObject(
                values=doc['controller'],
                default_values=DEFAULT_SETTINGS['controller']
            )
        else:
            obj = ConfigObject(values=DEFAULT_SETTINGS['controller'])
        setattr(self, 'controller', obj)
        # Create object for configuration of log files.
        if 'logs' in doc:
            obj = ConfigObject(
                values=doc['logs'],
                default_values=DEFAULT_SETTINGS['logs']
            )
        else:
            obj = ConfigObject(values=DEFAULT_SETTINGS['logs'])
        setattr(self, 'logs', obj)
