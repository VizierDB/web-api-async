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

import os


"""Default directory for all server resources."""
ENV_DIRECTORY = './.vizierdb'


"""Environment variables used by the vizier API web service to configure server
and components.
"""

"""General"""
# Web service name
VIZIERSERVER_NAME = 'VIZIERSERVER_NAME'
# Log file directory used by the web server (DEFAULT: ./.vizierdb/logs)
VIZIERSERVER_LOG_DIR = 'VIZIERSERVER_LOG_DIR'
# Flag indicating whether server is started in debug mode (DEFAULT: True)
VIZIERSERVER_DEBUG = 'VIZIERSERVER_DEBUG'

"""Web service"""
# Url of the server (DEFAULT: http://localhost)
VIZIERSERVER_BASE_URL = 'VIZIERSERVER_BASE_URL'
# Public server port (DEFAULT: 5000)
VIZIERSERVER_SERVER_PORT = 'VIZIERSERVER_SERVER_PORT'
# Locally bound server port (DEFAULT: 5000)
VIZIERSERVER_SERVER_LOCAL_PORT = 'VIZIERSERVER_SERVER_LOCAL_PORT'
# Application path for Web API (DEFAULT: /vizier-db/api/v1)
VIZIERSERVER_APP_PATH = 'VIZIERSERVER_APP_PATH'

# Default row limit for requests that read datasets (DEFAULT: 25)
VIZIERSERVER_ROW_LIMIT = 'VIZIERSERVER_ROW_LIMIT'
# Maximum row limit for requests that read datasets (-1 returns all rows) (DEFAULT: 1000)
VIZIERSERVER_MAX_ROW_LIMIT = 'VIZIERSERVER_MAX_ROW_LIMIT'
# Maximum size for file uploads in byte (DEFAULT: 16777216)
VIZIERSERVER_MAX_UPLOAD_SIZE = 'VIZIERSERVER_MAX_UPLOAD_SIZE'

"""Workflow Execution Engine"""
# Name of the workflow execution engine (DEFAULT: DEV_LOCAL)
VIZIERSERVER_ENGINE = 'VIZIERSERVER_ENGINE'
# Path to the package declaration directory (DEFAULT: ./resources/packages)
VIZIERSERVER_PACKAGE_PATH = 'VIZIERSERVER_PACKAGE_PATH'
# Path to the task processor definitions for supported packages (DEFAULT: ./resources/processors)
VIZIERSERVER_PROCESSOR_PATH = 'VIZIERSERVER_PROCESSOR_PATH'


"""Dictionary of default confiburation settings. Note that there is no
environment variable defined for the link to the API documentation."""
DEFAULT_SETTINGS = {
    VIZIERSERVER_NAME: 'Vizier Web API',
    VIZIERSERVER_LOG_DIR: os.path.join(ENV_DIRECTORY, 'logs'),
    VIZIERSERVER_DEBUG: True,
    VIZIERSERVER_BASE_URL: 'http://localhost',
    VIZIERSERVER_SERVER_PORT: 5000,
    VIZIERSERVER_SERVER_LOCAL_PORT: 5000,
    VIZIERSERVER_APP_PATH: '/vizier-db/api/v1',
    VIZIERSERVER_ROW_LIMIT: 25,
    VIZIERSERVER_MAX_ROW_LIMIT: 1000,
    VIZIERSERVER_MAX_UPLOAD_SIZE: 16 * 1024 * 1024,
    VIZIERSERVER_ENGINE: 'DEV_LOCAL',
    VIZIERSERVER_PACKAGE_PATH: './resources/packages',
    VIZIERSERVER_PROCESSOR_PATH: './resources/processors',
    'doc_url': ''
}

"""Supported attribute types."""
BOOL = 'bool'
FLOAT = 'float'
INTEGER = 'int'
STRING = 'str'

ATTRIBUTE_TYPES = [BOOL, FLOAT, INTEGER, STRING]


class ConfigObject(object):
    """Object whose attributes contain the values of environment variables that
    are used by vizier for to configure API components.
    """
    def __init__(self, attributes, default_values=None):
        """Initialize the object from a list of 3-tuples containing (1) the
        attribute name, (2) the name of the environment variable that contains
        the attibute value, and (3) the attibute type (string, bool ,float, or
        int).

        If the environment variable is not set (or the name of the environemnt
        variable is None), the value will be taken from the default values
        dictionary. If the default values are not given the global default
        dictionary is used.

        Parameters
        ----------
        attributes: list
            List of tuples containing attribute name, name of the environment
            variable, and attribute type.
        default_values: dict, optional
            Dictionary of default values.
        """
        for attr in attributes:
            name, var, type = attr
            val = get_config_value(
                attribute_name=name,
                attribute_type=type,
                env_variable=var,
                default_values=default_values
            )
            setattr(self, name, val)


class ServerConfig(object):
    """Configuration for an API web server. The object schema is as follows:

    webservice:
        server_url
        server_port
        server_local_port
        app_path
        app_base_url
        doc_url
        name
        defaults:
            row_limit
            max_row_limit
            max_file_size
    engine:
        identifier
        packages_path
        processors_path
    run:
        debug
    logs:
        server
    """
    def __init__(self, default_values=None):
        """Create object instance from environment variables and optional set
        of default values.

        Parameters
        ----------
        default_values: dict, optional
            Dictionary of default values
        """
        # webservice
        self.webservice = ConfigObject(
            attributes=[
                ('name', VIZIERSERVER_NAME, STRING),
                ('server_url', VIZIERSERVER_BASE_URL, STRING),
                ('server_port', VIZIERSERVER_SERVER_PORT, INTEGER),
                ('server_local_port', VIZIERSERVER_SERVER_LOCAL_PORT, INTEGER),
                ('app_path', VIZIERSERVER_APP_PATH, STRING),
                ('doc_url', None, STRING)
            ],
            default_values=default_values
        )
        defaults = ConfigObject(
            attributes=[
                ('row_limit', VIZIERSERVER_ROW_LIMIT, INTEGER),
                ('max_row_limit', VIZIERSERVER_MAX_ROW_LIMIT, INTEGER),
                ('max_file_size', VIZIERSERVER_MAX_UPLOAD_SIZE, INTEGER)
            ],
            default_values=default_values
        )
        setattr(self.webservice, 'defaults', defaults)
        # engine
        self.engine = ConfigObject(
            attributes=[
                ('identifier', VIZIERSERVER_ENGINE, STRING),
                ('packages_path', VIZIERSERVER_PACKAGE_PATH, STRING),
                ('processors_path', VIZIERSERVER_PROCESSOR_PATH, STRING)
            ],
            default_values=default_values
        )
        # run
        self.run = ConfigObject(
            attributes=[('debug', VIZIERSERVER_DEBUG, BOOL)],
            default_values=default_values
        )
        # logs
        self.logs = ConfigObject(
            attributes=[('server', VIZIERSERVER_LOG_DIR, STRING)],
            default_values=default_values
        )

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

def get_config_value(attribute_name, attribute_type, env_variable, default_values):
    """Get the value for a configuration parameter. The value is expected to be
    set in the given environment variable. If the variable is not set the value
    is taken from the given default settings or the global default settings.

    If the environment variable name is None the attribute name is used to
    get the value from the default values or the global default settings.

    Raises ValueError if the attribute type is invalid.

    Parameters
    ----------
    attribute_name: string
        Name of the attibute that is being set
    attribute_type: string
        Expected type of the attribute value
    env_variable: string
        Name of the environment variable
    default_values: dict, optional
        Dictionary of default values.

    Returns
    -------
    scalar value
    """
    # Raise an exception if the attribute type is unknown
    if not attribute_type in ATTRIBUTE_TYPES:
        raise ValueError('unknown attribute type \'' + str(type) + '\' for \'' + attribute_name + '\'')
    # Get the value for the environment variable. If the variable is
    # None or if the variable is not set use the default settings
    if not env_variable is None:
        val = os.getenv(env_variable)
        if val is None:
            if not default_values is None and env_variable in default_values:
                val = default_values[env_variable]
            else:
                val = DEFAULT_SETTINGS[env_variable]
    elif not default_values is None and attribute_name in default_values:
        val = default_values[attribute_name]
        if val is None:
            val = DEFAULT_SETTINGS[attribute_name]
    else:
        val = DEFAULT_SETTINGS[attribute_name]
    if not val is None:
        if attribute_type == BOOL and not isinstance(val, bool):
            val = val.lower() == 'true'
        elif attribute_type == FLOAT and not isinstance(val, float):
            try:
                val = float(val)
            except ValueError as ex:
                raise ValueError('expected float value for \'' + env_variable + '\'')
        elif attribute_type == INTEGER and not isinstance(val, int):
            try:
                val = int(val)
            except ValueError as ex:
                raise ValueError('expected integer value for \'' + env_variable + '\'')
    return val
