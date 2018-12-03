# Copyright (C) 2018 New York University,
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


class TaskHandle(object):
    """A task is uniquely identified by the combination of viztrail, branch,
    and module identifier. With each handle we maintain the external form of
    the command.
    """
    def __init__(self, viztrail_id, branch_id, module_id, external_form):
        """Initialize the components of the task handle.

        Parameters
        ----------
        viztrail_id: string
            Unique viztrail identifier
        branch_id: string
            Unique branch identifier
        module_id: string
            Unique module identifier
        external_form: string
            String containing a representation of the associated command in
            human-readable form
        """
        self.viztrail_id = viztrail_id
        self.branch_id = branch_id
        self.module_id = module_id
        self.external_form = external_form
