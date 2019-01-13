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
run:
    debug: Flag indicating whether server is started in debug mode
logs:
    server: Log file for Web Server
"""

import os

from vizier.config.base import ServerConfig


class AppConfig(ServerConfig):
    """Application configuration object. This object contains all configuration
    settings for the Vizier instance.
    """
    def __init__(self, default_values=None):
        """Initialize the configuration object from the respective environment
        variables and the optional default values.

        Parameters
        ----------
        default_values: dict, optional
            Dictionary of default values
        """
        super(AppConfig, self).__init__(default_values=default_values)
