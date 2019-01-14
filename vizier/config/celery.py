# Copyright (C) 2019 New York University,
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

"""Helper methods to configure the celery backend."""

import os

"""Environment variable containing routing information."""
# Colon separated list of package.command=queue strings that define routing
# information for individual commands
VIZIERENGINE_CELERY_ROUTES = 'VIZIERENGINE_CELERY_ROUTES'


def config_routes():
    """Create routing information for individual vizier commands. Expects
    routing information in the environment variable VIZIERENGINE_CELERY_ROUTES.
    The value format is a colon separated list of package.command.queue strings.

    Returns None if the environment variable is not set.

    Returns
    -------
    dict
    """
    routes = None
    routing = os.getenv(VIZIERENGINE_CELERY_ROUTES)
    if not routing is None and not routing.strip() == '':
        routes = dict()
        for rt in routing.split(':'):
            package_id, command_id, queue = rt.split('.')
            if not package_id in routes:
                routes[package_id] = dict()
            routes[package_id][command_id] = queue
    return routes
