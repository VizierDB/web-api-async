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

"""Collection of helper methods for the command line interface."""

from vizier.core.timestamp import utc_to_local


"""Default timestamp format."""
TIME_FORMAT = '%d-%m-%Y %H:%M:%S'


def ts(timestamp):
    """Convert datatime timestamp to string."""
    return  utc_to_local(timestamp).strftime(TIME_FORMAT)
