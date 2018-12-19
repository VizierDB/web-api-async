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

packages: # List of package declarations
    - declarations: File containing declaration of package commands
      parameters: Package specific configuration parameters
filestore:
    moduleName: Name of the module containing the used engine
    className: Class name of the used engine
    properties: Dictionary of engine specific configuration properties
datastore:
    moduleName: Name of the module containing the used engine
    className: Class name of the used engine
    properties: Dictionary of engine specific configuration properties
logs:
    server: Log file for Web Server
"""

import os

from vizier.config.app import ENV_DIRECTORY
from vizier.config.base import ConfigObject, read_object_from_file
from vizier.core.loader import ClassLoader
from vizier.engine.packages.base import PackageIndex
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL, VIZUAL_COMMANDS
from vizier.engine.packages.vizual.processor import PROPERTY_API

import vizier.datastore.fs.base as ds
import vizier.filestore.fs.base as fs
import vizier.viztrail.driver.objectstore.repository as vt


"""Environment Variable containing path to config file."""
ENV_CONFIG = 'VIZIERWORKER_CONFIG'

"""Default settings."""
DEFAULT_SETTINGS = {
    'datastores': ClassLoader.to_dict(
        module_name='vizier.datastore.fs.factory',
        class_name='FileSystemDatastoreFactory',
        properties={
            ds.PARA_DIRECTORY: os.path.join(ENV_DIRECTORY, 'ds')
        }
    ),
    'filestores': ClassLoader.to_dict(
        module_name='vizier.filestore.fs.factory',
        class_name='DefaultFilestoreFactory',
        properties={
            fs.PARA_DIRECTORY: os.path.join(ENV_DIRECTORY, 'fs')
        }
    ),
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
            doc = DEFAULT_SETTINGS
        # Registry of available packages. By default, only the VizUAL package is
        # pre-loaded. Additional packages are loaded from files specified in the
        # 'packages' element (optional). Note that the list may contain a
        # reference to a VizUAL package which overrides the default package
        # declaration.
        self.packages = {
            PACKAGE_VIZUAL: PackageIndex(
                package=VIZUAL_COMMANDS,
                properties={
                    PROPERTY_API: ClassLoader.to_dict(
                        module_name='vizier.engine.packages.vizual.api.fs',
                        class_name='DefaultVizualApi'
                    )
                }
            )
        }
        if 'packages' in doc:
            for pckg_file in doc['packages']:
                if not 'declarations' in pckg_file:
                    raise ValueError('missing key \'declarations\' for package')
                pckg = read_object_from_file(pckg_file['declarations'])
                for key in pckg:
                    if 'parameters' in pckg_file:
                        package_parameters = pckg_file['parameters']
                    else:
                        package_parameters = None
                    self.packages[key] = PackageIndex(
                        package=pckg[key],
                        properties=package_parameters
                    )
        # Create processors for the loaded packages
        self.processors = dict()
        for key in self.packages:
            loader = ClassLoader(self.packages[key].engine)
            self.processors[key] = loader.get_instance()
        # Create a class loader instance for the engine.
        for key in ['filestores', 'datastores']:
            if key in doc:
                loader = ClassLoader(values=doc[key])
            else:
                loader = ClassLoader(values=DEFAULT_SETTINGS[key])
            setattr(self, key, loader.get_instance())
        # Create object for configuration of log files.
        if 'logs' in doc:
            obj = ConfigObject(
                values=doc['logs'],
                default_values=DEFAULT_SETTINGS['logs']
            )
        else:
            obj = ConfigObject(values=DEFAULT_SETTINGS['logs'])
        setattr(self, 'logs', obj)
