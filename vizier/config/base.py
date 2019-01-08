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

"""Classes and helper methods for configration objects."""

import json
import os
import yaml


"""Default directory for API data."""
ENV_DIRECTORY = './.vizierdb'


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


class ServerConfig(object):
    """Configuration for an API web server.

    Configuration is read from a configuration file (in Yaml or Json format).
    The file is expected to follow the given format:

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

    The configuration file can either be given, specified using the an
    envirnment variable, or be located (as file config.json or config.yaml) in
    the current working directory.
    """
    def __init__(self, env, default_settings, configuration_file=None):
        """Read configuration parameters from a configuration file. The file is
        expected to be in Json or Yaml format, having the same structure as
        shown above.

        If no file is specified attempts will be made to read the following
        configuration files:

        (1) The file referenced by the environment variable VIZIERSERVER_CONFIG
        (2) The file config.json or config.yaml in the current working directory

        If the specified files are not found a ValueError is raised.

        Parameters
        ----------
        env: string
            Name of the environment variable that contains the reference to the
            configuration file.
        default_settings: dict
            Default settings
        configuration_file: string, optional
            Optional path to configuration file
        """
        # Read configuration from configuration file. Raises ValueError if no
        # configuration file is found.
        self.doc = None
        if configuration_file != None:
            if os.path.isfile(configuration_file):
                self.doc = read_object_from_file(configuration_file)
            else:
                raise ValueError('unknown file \'' + str(configuration_file) + '\'')
        else:
            files = [
                os.getenv(env),
                './config.json',
                './config.yaml'
            ]
            for config_file in files:
                if not config_file is None and os.path.isfile(config_file):
                    self.doc = read_object_from_file(config_file)
                    break
        if self.doc is None:
            raise ValueError('no configuration object found')
        # Create the vizier engine
        if 'engine' in self.doc:
            for key in ['identifier', 'properties']:
                if not key in self.doc['engine']:
                    raise ValueError('missing key \'' + key + '\' for engine configuration')
            self.engine = self.doc['engine']
        else:
            raise ValueError('missing engine configuration')
        # Create objects for configuration of log files and web service.
        for key in ['logs', 'webservice']:
            if key in self.doc:
                obj = ConfigObject(
                    values=self.doc[key],
                    default_values=default_settings[key]
                )
            else:
                obj = ConfigObject(values=default_settings[key])
            setattr(self, key, obj)
        # Set debug flag if given
        if 'debug' in self.doc:
            self.debug = self.doc['debug']
        else:
            self.debug = default_settings['debug']

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
