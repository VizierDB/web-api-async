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

"""Module containing the Celery instance. The broker url is expected to be set
using environment variable VIZIERCELERYBROKER_URL."""

from __future__ import absolute_import, unicode_literals
from celery import Celery

import os


"""Environment variables."""
BROKER_URL = 'CELERY_BROKER_URL'


# Create and initialize the celery app. The broker url is expected to be set in
# environment variable VIZIERCELERYBROKER_URL. If the variable is not set the
# default broker will be used. The configiration file allows to override Celery
# configuration parameters.
broker_url =  os.getenv(BROKER_URL)
if broker_url is None:
    broker_url = 'amqp://guest@localhost//'
celeryapp = Celery('vizier', broker=broker_url)
