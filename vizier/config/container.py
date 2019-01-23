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

"""Container server configuration object. Extends the application configuration
object with the project identifier and the url for the controlling web service.

The object schema is as follows:

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
    package_path
    processor_path
controller_url: Url of the controlling web service
project_id: Unique identifier for the project that the container maintains
run:
    debug
logs:
    server
"""

import os

from vizier.config.app import AppConfig
from vizier.config.base import get_config_value


"""Environment variables that are specific to the container server."""
# Unique identifier for the project that the container maintains
VIZIERCONTAINER_PROJECT_ID = 'VIZIERCONTAINER_PROJECT_ID'
# Url for the controlling web service
VIZIERCONTAINER_CONTROLLER_URL = 'VIZIERCONTAINER_CONTROLLER_URL'


class ContainerConfig(AppConfig):
    """Configuration object for vizier servers that run in a container to
    serve operations for a single project.

    Extends the application configuration object with the project id and the
    url of the controlling web service.
    """
    def __init__(self, default_values=None):
        """Initialize the configuration object from the respective environment
        variables and the optional default values.

        Raises ValueError if either the project identifier or the controller
        url are None.

        Parameters
        ----------
        default_values: dict, optional
            Dictionary of default values
        """
        super(ContainerConfig, self).__init__(default_values=default_values)
        # Get the project identifier. Raise exception if not set.
        self.project_id = get_config_value(
            env_variable=VIZIERCONTAINER_PROJECT_ID
        )
        if self.project_id is None:
            raise ValueError('missing project identifier')
        # Get controller url. Raise exception if not set.
        self.controller_url = get_config_value(
            env_variable=VIZIERCONTAINER_CONTROLLER_URL
        )
        if self.controller_url is None:
            raise ValueError('missing controller url')
