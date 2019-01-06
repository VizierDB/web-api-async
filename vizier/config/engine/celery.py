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

"""Helper classes and methods to configure the celery backend."""

from __future__ import absolute_import, unicode_literals
from celery import Celery

celery_app = Celery('vizier')
celery_app.config_from_object('vizier.config.engine.celeryconfig')


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def config_routes(elements):
    """Create routing information for individual vizier commands. The expected
    format of the properties dictionary is as follows:

    routes: List representing mapping from commands to queues
        - packageId
          commandId
          queue: Name of the queue (matching a name in the queues list)

    Returns a dictionary of dictionaries mapping package commands to routing
    queues.

    Parameters
    ----------
    elements: list(dict)
        List of dictionaries that contain routing information for individual
        vizier commands

    Returns
    -------
    dict
    """
    routes = dict()
    for rt in elements:
        for key in ['packageId','commandId', 'queue']:
            if not key in rt:
                raise ValueError('missing element \'' + key + '\' in route information')
        package_id = rt['packageId']
        command_id = rt['commandId']
        queue = rt['queue']
        if not package_id in routes:
            routes[package_id] = dict()
        routes[package_id][command_id] = queue
    return routes


if __name__ == '__main__':
    celery_app.start()
