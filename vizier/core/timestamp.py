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

"""Helper methods for timestamps - Contains functions to create and Convert
timestamps.
"""

import datetime
import time

def get_current_time():
    """Return timestamp for current system time in UTC time zone.

    Returns
    -------
    datetime
        Current system time
    """
    return datetime.datetime.utcnow()


def to_datetime(timestamp):
    """Converts a timestamp string in ISO format into a datatime object.

    Parameters
    ----------
    timstamp : string
        Timestamp in ISO format

    Returns
    -------
    datatime.datetime
        Datetime object
    """
    try:
        return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')


def utc_to_local(utc_datetime):
    """Convert datatime in UTC time zone to local time.

    Parameters
    ----------
    utc_datetime: datatime.datetime
        Timestamp in UCT time zone

    Returns
    -------
    datatime.datetime
    """
    # source: https://stackoverflow.com/questions/4770297/convert-utc-datetime-string-to-local-datetime-with-python
    now_timestamp = time.time()
    offset = datetime.datetime.fromtimestamp(now_timestamp) - datetime.datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset
