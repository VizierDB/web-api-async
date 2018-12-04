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
file server, and the workflow execution environment. The structure of the
configuration object is as follows:

datastore:
    module: Name of the Python module containing data store class
    class: Class name of data store
    properties: Dictionary of data store specific properties
debug: Flag indicating whether server is started in debug mode
filestore:
    module: Name of the Python module containing file store class
    class: Class name of file store
    properties: Dictionary of file store specific properties
logs:
    server: Log file for Web Server
packages: List of files, each containing the declaration of commands for a supported package
viztrails:
    module: Name of the Python module containing repository class
    class: Class name of viztrails repository
    properties: Dictionary of repository specific properties
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
"""

import importlib
import json
import os
import yaml

from vizier.engine.packages.base import PackageIndex
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL, VIZUAL_COMMANDS

import vizier.datastore.fs as ds
import vizier.filestore.fs.base as fs
import vizier.viztrail.driver.objectstore.repository as vt


"""Environment Variable containing path to config file."""
ENV_CONFIG = 'VIZIERSERVER_CONFIG'

"""Default directory for API data."""
ENV_DIRECTORY = '../.env'

"""Default execution environment."""
DEFAULT_SETTINGS = {
    'datastore': {
        'module': 'vizier.datastore.fs',
        'class': 'FileSystemDatastore',
        'properties': {
            ds.PARA_DIRECTORY: os.path.join(ENV_DIRECTORY, 'ds')
        }
    },
    'debug': True,
    'filestore': {
        'module': 'vizier.filestore.fs.base',
        'class': 'DefaultFilestore',
        'properties': {
            fs.PARA_DIRECTORY: os.path.join(ENV_DIRECTORY, 'fs')
        }
    },
    'logs': {
        'server': os.path.join(ENV_DIRECTORY, 'logs')
    },
    'viztrails': {
        'module': 'vizier.viztrail.driver.objectstore.repository',
        'class': 'OSViztrailRepository',
        'properties': {
            vt.PARA_DIRECTORY: os.path.join(ENV_DIRECTORY, 'vt')
        }
    },
    'webservice': {
        'server_url': 'http://localhost',
        'server_port': 5000,
        'server_local_port': 5000,
        'app_path': '/vizier-db/api/v1',
        'doc_url': 'http://cds-swg1.cims.nyu.edu/vizier-db/doc/api/v1',
        'name': 'Vizier Web API',
        'defaults': {
            'row_limit': 25,
            'max_row_limit': -1,
            'max_file_size': 16 * 1024 * 1024
        }
    }
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
        expected to be in Yaml format, having the same structure as shown above.

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
        # Read configuration from either of the following:
        doc = None
        if configuration_file != None:
            if os.path.isfile(configuration_file):
                doc = read_object_from_file(configuration_file)
            else:
                raise ValueError('unknown file \'' + str(configuration_file) + '\'')
        else:
            files = [
                configuration_file,
                os.getenv(ENV_CONFIG),
                './config.yaml'
            ]
            for config_file in files:
                if not config_file is None and os.path.isfile(config_file):
                    doc = read_object_from_file(config_file)
                    break
        if doc is None:
            raise RuntimeError('no configuration file found')
        # Registry of available packages. By default, only the VizUAL package is
        # pre-loaded. Additional packages are loaded from files specified in the
        # 'packages' element (optional). Note that the list may contain a
        # reference to a VizUAL package which overrides the default package
        # declaration.
        self.packages = {PACKAGE_VIZUAL: PackageIndex(VIZUAL_COMMANDS)}
        if 'packages' in doc:
            for pckg_file in doc['packages']:
                pckg = read_object_from_file(pckg_file)
                for key in pckg:
                    self.packages[key] = PackageIndex(pckg[key])
        # Create configuration object for system components data store, file
        # store, viztrail registry, and workflow execution engine.
        for key in ['datastore', 'filestore', 'viztrails']:
            if key in doc:
                obj = ComponentConfig(
                    values=doc[key],
                    default_values=DEFAULT_SETTINGS[key]
                )
            else:
                obj = ComponentConfig(values=DEFAULT_SETTINGS[key])
            setattr(self, key, obj)
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


class ComponentConfig(object):
    """Configuration object for system component. All components are specified
    by the Python module and class. Component initialization is done via a
    dictionary of configuration arguments (properties).
    """
    def __init__(self, values, default_values=dict()):
        """Set the component module, class and configuration arguments
        dictionary.

        Parameters
        ----------
        values: dict
            Dictionary containing component specification and arguments.
        default_values: dict
            Dictionary of default values.
        """
        if 'module' in values:
            self.module_name = values['module']
        else:
            self.module_name = default_values['module']
        if 'class' in values:
            self.class_name = values['class']
        else:
            self.class_name = default_values['class']
        if 'properties' in values:
            self.properties = values['properties']
        else:
            self.properties = default_values['properties']

    def create_instance(self):
        """Create an instance of the specified system component."""
        module = importlib.import_module(self.module_name)
        return getattr(module, self.class_name).init(self.properties)


class ConfigObject(object):
    """Create a Python class instance from a given dictionary of variables and
    a dictionary of default values. Performs a nested merge of dictionaries in
    values and default values.

    For lists the policy is all or nothing. List elements are not merged. If the
    configuration object contains a list value all items in the list are copied
    to the configuration as is.
    """
    def __init__(self, values, default_values=dict()):
        """Initialize the object from the given dictionary of values and default
        values.

        Parameters
        ----------
        values: dict
            Dictionary of object variables. Dictionary keys are used as variable
            names.
        default_values: dict
            Dictionary of default values.
        """
        #print json.dumps(values, sort_keys=True, indent=4)
        #print json.dumps(default_values, sort_keys=True, indent=4)
        self.keys = set()
        for key in values:
            if isinstance(values[key], dict):
                if key in default_values:
                    def_val = default_values[key]
                    if not isinstance(def_val, dict):
                        def_val = dict()
                else:
                    def_val = dict()
                setattr(
                    self,
                    key,
                     ConfigObject(values[key], default_values=def_val)
                )
            elif isinstance(values[key], list):
                elements = list()
                for el in values[key]:
                    if isinstance(el, dict):
                        elements.append(ConfigObject(el))
                    else:
                        elements.append(el)
                setattr(self, key,  elements)
            else:
                setattr(self, key,  values[key])
            self.keys.add(key)
        for key in default_values:
            if not key in self.keys:
                if isinstance(default_values[key], dict):
                    setattr(
                        self,
                        key,
                        ConfigObject(dict(), default_values=default_values[key])
                    )
                elif isinstance(default_values[key], list):
                    elements = list()
                    for el in default_values[key]:
                        if isinstance(el, dict):
                            elements.append(ConfigObject(el))
                        else:
                            elements.append(el)
                    setattr(self, key,  elements)
                else:
                    setattr(self, key,  default_values[key])
                self.keys.add(key)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def read_object_from_file(filename):
    """Read dictionary serialization from file. The file format is expected to
    by Yaml unless the filename ends with .json.

    Parameters
    ----------
    filename: string
        Name of the input file

    Returns
    -------
    dict
    """
    with open(filename, 'r') as f:
        if filename.endswith('.json'):
            return json.loads(f.read())
        else:
            return yaml.load(f.read())
