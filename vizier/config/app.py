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
list of available packages.

The app configuration has three main parts: API and Web service configuration,
list of available packages, and the workflow execution environment. The
structure of the configuration object is as follows:

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
packages: # List of package declarations
    - declarations: File containing declaration of package commands
      parameters: Package specific configuration parameters
engine:
    moduleName: Name of the module containing the used engine
    className: Class name of the used engine
    properties: Dictionary of engine specific configuration properties
debug: Flag indicating whether server is started in debug mode
logs:
    server: Log file for Web Server
"""

import os

from vizier.config.base import ConfigObject, read_object_from_file
from vizier.core.io.base import PARA_LONG_IDENTIFIER
from vizier.core.loader import ClassLoader
from vizier.engine.packages.base import PackageIndex
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL, VIZUAL_COMMANDS
from vizier.engine.packages.vizual.processor import PROPERTY_API

import vizier.datastore.fs.base as ds
import vizier.filestore.fs.base as fs
import vizier.viztrail.driver.objectstore.repository as vt


"""Environment Variable containing path to config file."""
ENV_CONFIG = 'VIZIERSERVER_CONFIG'

"""Default directory for API data."""
ENV_DIRECTORY = './.vizierdb'

"""Default settings."""
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
    'engine' : ClassLoader.to_dict(
        module_name='vizier.engine.environments.local',
        class_name='DefaultLocalEngine',
        properties={
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
            'viztrails': ClassLoader.to_dict(
                module_name='vizier.viztrail.driver.objectstore.repository',
                class_name='OSViztrailRepository',
                properties={
                    vt.PARA_DIRECTORY: os.path.join(ENV_DIRECTORY, 'vt'),
                    PARA_LONG_IDENTIFIER: False
                }
            )
        }
    ),
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
        # Create a class loader instance for the engine.
        if 'engine' in doc:
            engine = ClassLoader(values=doc['engine'])
        else:
            engine = ClassLoader(values=DEFAULT_SETTINGS['engine'])
        setattr(self, 'engine', engine)
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

    def get_api_engine(self):
        """Get the instance of the defined API engine.

        Returns
        -------
        vizier.engine.base.VizierEngine
        """
        return self.engine.get_instance(packages=self.packages)
