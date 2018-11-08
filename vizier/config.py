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

api:
    server_url: Url of the server (e.g., http://localhost)
    server_port: Public server port (e.g., 5000)
    server_local_port: Locally bound server port (e.g., 5000)
    app_path: Application path for Web API (e.g., /vizier-db/api/v1)
    app_base_url: Concatenation of server_url, server_port and app_path
    doc_url: Url to API documentation
fileserver:
    directory: Path to base directory for file server
    max_file_size: Maximum size for file uploads (in byte)
envs:
    - identifier: Execution environment identifier (i.e., BASIC or MIMIR)
      name: Printable name of execution environment (used by UI)
      description: Descriptive text for execution environment (used by UI)
      default: Flag indicating if this is the default environment
      datastore:
          properties: Dictionary of data store specific properties
          type: Data store type ['DEFAULT', 'MIMIR']
      packages: [list of identifier for supported packages]
viztrails:
    directory: Base directory for storing viztrail information and meta-data
defaults:
    row_limit: Default row limit for requests that read datasets
    max_row_limit: Maximum row limit for requests that read datasets (-1 = all)
name: Web Service name
debug: Flag indicating whether server is started in debug mode
logs:
    server: Log file for Web Server
    engine: Flag to toggle loggin for workflow engine telemetry
"""

import json
import os
import yaml

from vizier.datastore.base import DATASTORE_DEFAULT
from vizier.datastore.fs import PARA_DIRECTORY
from vizier.workflow.packages.base import PackageIndex
from vizier.workflow.packages.sys import PACKAGE_SYS, SYSTEM_COMMANDS
from vizier.workflow.packages.vizual.base import PACKAGE_VIZUAL, VIZUAL_COMMANDS


"""Environment Variable containing path to config file."""
ENV_CONFIG = 'VIZIERSERVER_CONFIG'

"""Default directory for API data."""
ENV_DIRECTORY = '../.env'

"""Default execution environment."""
DEFAULT_ENV_ID = 'BASIC'
DEFAULT_ENV_NAME = 'Vizier (Lite)'
DEFAULT_ENV_DESC = 'Curation workflow with basic functionality'


DEFAULT_SETTINGS = {
    'api': {
        'server_url': 'http://localhost',
        'server_port': 5000,
        'server_local_port': 5000,
        'app_path': '/vizier-db/api/v1',
        'doc_url': 'http://cds-swg1.cims.nyu.edu/vizier-db/doc/api/v1'

    },
    'envs': [{
        'identifier': DEFAULT_ENV_ID,
        'name': DEFAULT_ENV_NAME,
        'description': DEFAULT_ENV_DESC,
        'default': True,
        'datastore': {
            'properties': {
                PARA_DIRECTORY: os.path.join(ENV_DIRECTORY, 'ds')
            },
            'type': DATASTORE_DEFAULT
        },
        'packages': [PACKAGE_VIZUAL]
    }],
    'fileserver': {
        'directory': os.path.join(ENV_DIRECTORY, 'fs'),
        'max_file_size': 16 * 1024 * 1024
    },
    'viztrails': {
        'directory': os.path.join(ENV_DIRECTORY, 'vt')
    },
    'defaults': {
        'row_limit': 25,
        'max_row_limit': -1
    },
    'name': 'Vizier Web API',
    'debug': True,
    'logs': {
        'server': os.path.join(ENV_DIRECTORY, 'logs'),
        'engine': False
    }
}

class AppConfig(object):
    """Application configuration object. This object contains all configuration
    settings for the Vizier DB Web API. The structture is expected to be as
    follows:

    api:
        server_url: Url of the server (e.g., http://localhost)
        server_port: Public server port (e.g., 5000)
        server_local_port: Locally bound server port (e.g., 5000)
        app_path: Application path for Web API (e.g., /vizier-db/api/v1)
        doc_url: Url to API documentation
    fileserver:
        directory: Path to base directory for file server
        max_file_size: Maximum size for file uploads (in byte)
    envs:
        - identifier: Execution environment identifier (i.e., BASIC or MIMIR)
          name: Printable name of execution environment (used by UI)
          description: Descriptive text for execution environment (used by UI)
          default: Flag indicating if this is the default environment
          datastore:
              properties: Dictionary of data store specific properties
              type: Data store type ['DEFAULT', 'MIMIR']
          packages: [list of identifier for supported packages]
    viztrails:
        directory: Base directory for storing viztrail information and meta-data
    defaults:
        row_limit: Default row limit for requests that read datasets
        max_row_limit: Maximum row limit for requests that read datasets
    name: Web Service name
    debug: Flag indicating whether server is started in debug mode
    logs:
        server: Log file for Web Server
        engine: Log file for workflow engine telemetry

    Configuration is read from a configuration file (in Yaml or Json format).
    The file is expected to contain two  objects: packages and settings.

    packages: List of file names that point to package definition files. By
    default only the ViZUAL and the system package are available to the user.

    settings: Application settings object. The structure of this object is
    expected to be the same as the structre shown above.

    The configuration file can either be specified using the environment
    variable VIZIERSERVER_CONFIG or be located (as file config.yaml) in the
    current working directory.
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
        # Registry of available packages. By default, only the system package
        # and the VizUAL package are supported. Additional packages are loaded
        # from files specified in the 'packages' element (optional)
        self.packages = {
            PACKAGE_SYS: PackageIndex(SYSTEM_COMMANDS),
            PACKAGE_VIZUAL: PackageIndex(VIZUAL_COMMANDS)
        }
        if 'packages' in doc:
            for pckg_file in doc['packages']:
                pckg = read_object_from_file(pckg_file)
                for key in pckg:
                    self.packages[key] = PackageIndex(pckg[key])
        # Initialize application settings from the settings element. We first
        # check the schema of all objects in the envs element (if present).
        if 'envs' in doc['settings']:
            for env in  doc['settings']['envs']:
                for key in ['identifier', 'name', 'description', 'datastore', 'packages']:
                    if not key in env:
                        raise ValueError('environment specification is missing \'' + key + '\'')
                # Check for the datastore directory
                for key in ['properties', 'type']:
                    if not key in env['datastore']:
                        raise ValueError('environment datastore is missing \'' + key + '\'')
        self.settings = ConfigObject(
            values=doc['settings'],
            default_values=DEFAULT_SETTINGS
        )
        # Create a dictionary of execution environments for convenience. Check
        # that all identifier are unique. Ensure that referenced packages are
        # available and that the sys package is not listed in the package list
        # for any of the user-defined execution environments.
        self.envs = dict()
        for exec_env in self.settings.envs:
            env_id = exec_env.identifier
            if env_id in self.envs:
                raise ValueError('duplicate environment definition \'' + env_id + '\'')
            for pckg_id in exec_env.packages:
                if pckg_id == PACKAGE_SYS:
                    raise ValueError('invalid package \'' + PACKAGE_SYS + '\' specified')
                elif not pckg_id in self.packages:
                    raise ValueError('unknown package \'' + pckg_id + '\'')
                self.envs[exec_env.identifier] = exec_env

    @property
    def app_base_url(self):
        """Full Url (prefix) for all resources that are available on the Web
        Service. this is a concatenation of the service Url, port number and
        application path.

        Returns
        -------
        string
        """
        base_url = self.settings.api.server_url
        if self.settings.api.server_port != 80:
            base_url += ':' + str(self.settings.api.server_port)
        base_url += self.settings.api.app_path
        return base_url


class ConfigObject(object):
    """Create a Python class instance from a given dictionary of variables and
    a dictionary of default values. Performs a nested merge of dictionaries in
    values and default values.

    For lists the policy is all or nothing. There are currently only two list
    objects in the configuration: envs and packages. List elements are not
    merged. thus, if the configuration object contains an envs list all objects
    in the list are copied to the configuration as is. If no envs element is
    present the default environments are copied.
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
