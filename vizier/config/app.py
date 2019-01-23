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

"""Application configuration object. Contains all application settings.

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
    data_dir
    package_path: Path to folders containing package declarations
    processor_path: Path to folders containing processor definitions
    sync_commands
    use_short_ids
    backend:
        identifier: Unique backend identifier
        celery:
            routes: Optional routing infformation for celery workers
        container:
            ports: First port number for new project containers
            image: Identifier of the project container docker image
run:
    debug: Flag indicating whether server is started in debug mode
logs:
    server: Log file for Web Server
"""

import os

import vizier.config.base as base


"""Environment variables used by the vizier API web service to configure server
and components.
"""

"""General"""
# Web service name
VIZIERSERVER_NAME = 'VIZIERSERVER_NAME'
# Log file directory used by the web server (DEFAULT: ./.vizierdb/logs/server)
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

"""Definition of additional configuration environment variables that are used
to instantiate and configure the vizier engine.
"""
# Base data directory for the default engine
VIZIERENGINE_DATA_DIR = 'VIZIERENGINE_DATA_DIR'
# Name of the used backend (CELERY, MULTIPROCESS, or CONTAINER) (DEFAULT: MULTIPROCESS)
VIZIERENGINE_BACKEND = "VIZIERENGINE_BACKEND"
# Colon separated list of package.command strings that identify the commands
# that are executed synchronously
VIZIERENGINE_SYNCHRONOUS = 'VIZIERENGINE_SYNCHRONOUS'
# Flag indicationg whether short identifier are used by the viztrail repository
VIZIERENGINE_USE_SHORT_IDENTIFIER = 'VIZIERENGINE_USE_SHORT_IDENTIFIER'

"""Celery backend"""
# Colon separated list of package.command=queue strings that define routing
# information for individual commands
VIZIERENGINE_CELERY_ROUTES = 'VIZIERENGINE_CELERY_ROUTES'

"""Container backend"""
# First port number for new project containers. All following containers will
# have higher port numbers
VIZIERENGINE_CONTAINER_PORTS = 'VIZIERENGINE_CONTAINER_PORTS'
# Unique identifier of the project container docker image
VIZIERENGINE_CONTAINER_IMAGE = 'VIZIERENGINE_CONTAINER_IMAGE'


"""Dictionary of default configurations settings. Note that there is no
environment variable defined for the link to the API documentation."""
DEFAULT_SETTINGS = {
    VIZIERSERVER_NAME: 'Vizier Web API',
    VIZIERSERVER_LOG_DIR: os.path.join(base.ENV_DIRECTORY, 'logs', 'server'),
    VIZIERSERVER_DEBUG: True,
    VIZIERSERVER_BASE_URL: 'http://localhost',
    VIZIERSERVER_SERVER_PORT: 5000,
    VIZIERSERVER_SERVER_LOCAL_PORT: 5000,
    VIZIERSERVER_APP_PATH: '/vizier-db/api/v1',
    VIZIERSERVER_ROW_LIMIT: 25,
    VIZIERSERVER_MAX_ROW_LIMIT: base.DEFAULT_MAX_ROW_LIMIT,
    VIZIERSERVER_MAX_UPLOAD_SIZE: 16 * 1024 * 1024,
    VIZIERSERVER_ENGINE: base.DEV_ENGINE,
    VIZIERSERVER_PACKAGE_PATH: './resources/packages/common',
    VIZIERSERVER_PROCESSOR_PATH: './resources/processors/common:./resources/processors/dev',
    VIZIERENGINE_DATA_DIR: base.ENV_DIRECTORY,
    VIZIERENGINE_BACKEND: base.BACKEND_MULTIPROCESS,
    VIZIERENGINE_USE_SHORT_IDENTIFIER: True,
    VIZIERENGINE_SYNCHRONOUS: None,
    VIZIERENGINE_CELERY_ROUTES: None,
    VIZIERENGINE_CONTAINER_PORTS: range(20171, 20271),
    VIZIERENGINE_CONTAINER_IMAGE: 'heikomueller/vizierapi:container',
    'doc_url': 'http://cds-swg1.cims.nyu.edu/doc/vizier-db/'
}


"""Default file names and folder names for different resources in the engine
data directory.
"""
DEFAULT_DATASTORES_DIR = 'ds'
DEFAULT_FILESTORES_DIR = 'fs'
DEFAULT_VIZTRAILS_DIR = 'vt'

DEFAULT_CONTAINER_FILE = 'containers'


class AppConfig(object):
    """Application configuration object. This object contains all configuration
    settings for the Vizier instance.
    """
    def __init__(self, default_values=None):
        """Initialize the configuration object from the respective environment
        variables and the optional default values. The object schema is as
        follows:

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
            data_dir
            package_path
            processor_path
            sync_commands
            use_short_ids
            backend:
                identifier
                celery:
                    routes
                container:
                    ports
                    image
        run:
            debug
        logs:
            server

        Parameters
        ----------
        default_values: dict, optional
            Dictionary of default values
        """
        if default_values is None:
            default_values = DEFAULT_SETTINGS
        # webservice
        self.webservice = base.ConfigObject(
            attributes=[
                ('name', VIZIERSERVER_NAME, base.STRING),
                ('server_url', VIZIERSERVER_BASE_URL, base.STRING),
                ('server_port', VIZIERSERVER_SERVER_PORT, base.INTEGER),
                ('server_local_port', VIZIERSERVER_SERVER_LOCAL_PORT, base.INTEGER),
                ('app_path', VIZIERSERVER_APP_PATH, base.STRING),
                ('doc_url', None, base.STRING)
            ],
            default_values=default_values
        )
        defaults = base.ConfigObject(
            attributes=[
                ('row_limit', VIZIERSERVER_ROW_LIMIT, base.INTEGER),
                ('max_row_limit', VIZIERSERVER_MAX_ROW_LIMIT, base.INTEGER),
                ('max_file_size', VIZIERSERVER_MAX_UPLOAD_SIZE, base.INTEGER)
            ],
            default_values=default_values
        )
        setattr(self.webservice, 'defaults', defaults)
        # engine
        self.engine = base.ConfigObject(
            attributes=[
                ('identifier', VIZIERSERVER_ENGINE, base.STRING),
                ('data_dir', VIZIERENGINE_DATA_DIR, base.STRING),
                ('package_path', VIZIERSERVER_PACKAGE_PATH, base.STRING),
                ('processor_path', VIZIERSERVER_PROCESSOR_PATH, base.STRING),
                ('use_short_ids', VIZIERENGINE_USE_SHORT_IDENTIFIER, base.BOOL),
                ('sync_commands', VIZIERENGINE_SYNCHRONOUS, base.STRING)
            ],
            default_values=default_values
        )
        # engine.backend
        backend = base.ConfigObject(
            attributes=[('identifier', VIZIERENGINE_BACKEND, base.STRING)],
            default_values=default_values
        )
        # engine.backend.celery
        celery = base.ConfigObject(
            attributes=[('routes', VIZIERENGINE_CELERY_ROUTES, base.STRING)],
            default_values=default_values
        )
        setattr(backend, 'celery', celery)
        # engine.backend.container
        container = base.ConfigObject(
            attributes=[
                ('ports', VIZIERENGINE_CONTAINER_PORTS, base.LIST),
                ('image', VIZIERENGINE_CONTAINER_IMAGE, base.STRING)],
            default_values=default_values
        )
        setattr(backend, 'container', container)
        setattr(self.engine, 'backend', backend)
        # run
        self.run = base.ConfigObject(
            attributes=[('debug', VIZIERSERVER_DEBUG, base.BOOL)],
            default_values=default_values
        )
        # logs
        self.logs = base.ConfigObject(
            attributes=[('server', VIZIERSERVER_LOG_DIR, base.STRING)],
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
        return self.get_url(self.webservice.server_port)

    def get_url(self, port):
        """Full Url (prefix) for all resources that are available on a Web
        Service running on the given port.

        Parameters
        ----------
        port: int
            Port number the web service is running on

        Returns
        -------
        string
        """
        url = self.webservice.server_url
        if port != 80:
            url += ':' + str(port)
        url += self.webservice.app_path
        return url
