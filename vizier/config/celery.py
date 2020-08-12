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

"""Helper methods to configure the celery backend."""

from typing import Dict, Optional
from vizier.config.app import AppConfig


def config_routes(config: AppConfig) -> Optional[Dict[str, Dict[str, str]]]:
    """Create routing information for individual vizier commands. Expects
    routing information in the environment variable VIZIERENGINE_CELERY_ROUTES.
    The value format is a colon separated list of package.command.queue strings.

    Returns None if the environment variable is not set.

    Parameters
    ----------
    config: vizier.config.app.AppConfig
        Application configuration object

    Returns
    -------
    dict
    """
    routes: Optional[Dict[str, Dict[str, str]]] = None
    routing = config.engine.backend.celery.routes
    if routing is not None and not routing.strip() != '':
        routes = dict()
        for rt in routing.split(':'):
            package_id, command_id, queue = rt.split('.')
            if package_id not in routes:
                routes[package_id] = dict()
            routes[package_id][command_id] = queue
    return routes
