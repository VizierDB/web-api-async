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

"""The module timestamp contains for each module the time when the module
was created. For modules that are running or are finished the timestamp
contains the time when module execution started. For completed modules the
timestamp also contains the time when execution finished.
"""

from vizier.core.timestamp import get_current_time


class ModuleTimestamp(object):
    """Each module contains three timestamps:created_at, started_at and
    finished_at. The timestamp does not distinguish between the canceled, error,
    and success state. In either case the finished_at timestamp is set when the
    state change occurs.

    The timestamps started_at and finished_at may be None if the module is in
    PENDING state.

    Attributes
    ----------
    created_at: datatime.datetime
        Time when module was first created
    started_at: datatime.datetime
        Time when module execution started
    finished_at: datatime.datetime
        Time when module execution finished (either due to cancel, error or
        success state change)
    """
    def __init__(self, created_at=None, started_at=None, finished_at=None):
        """Initialize the timestamp components. If created_at is None the
        other two timestamps are expected to be None as well. Will raise
        ValueError if created_at is None but one of the other two timestamps
        is not None.

        Parameters
        ----------
        created_at: datatime.datetime
            Time when module was first created
        started_at: datatime.datetime
            Time when module execution started
        finished_at: datatime.datetime
            Time when module execution finished
        """
        # Raise ValueError if created_at is None but one of the other two
        # timestamps is not None
        if created_at is None and not (started_at is None and finished_at is None):
            raise ValueError('invalid timestamp information')
        self.created_at = created_at if not created_at is None else get_current_time()
        self.started_at = started_at
        self.finished_at = finished_at
