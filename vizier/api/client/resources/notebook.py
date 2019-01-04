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

import json
import requests

from vizier.api.client.resources.module import ModuleResource
from vizier.api.client.resources.workflow import WorkflowResource


class Notebook(object):
    def __init__(self, workflow):
        self.workflow = workflow

    def append_cell(self, command):
        """Append a new module to the notebook that executes te given command.

        Parameters
        ----------
        command: vizier.viztrail.command.ModuleCommand

        Returns
        -------
        ???
        """
        # Get append url and create request body
        url = self.workflow.links['workflow:append']
        data = {
            'packageId': command.package_id,
            'commandId': command.command_id,
            'arguments': command.arguments.to_list()
        }
        # Send request. Raise exception if status code indicates that the
        # request was not successful
        r = requests.post(url, json=data)
        r.raise_for_status()
        # The result is the new branch descriptor
        return json.loads(r.text)

    def cancel_exec(self):
        """Cancel exection of tasks for the notebook.

        Returns
        -------
        vizier.api.client.resources.workflow.WorkflowResource
        """
        url = self.workflow.links['workflow:cancel']
        r = requests.post(url)
        r.raise_for_status()
        # The result is the workflow handle
        return WorkflowResource.from_dict(json.loads(r.text))

    def delete_module(self, module_id):
        """Delete the notebook module with the given identifier.

        Returns
        -------
        list(vizier.api.client.resources.module.ModuleResource)
        """
        modules = list()
        for m in self.workflow.modules:
            if m.identifier == module_id:
                url = m.links['module:delete']
                r = requests.delete(url)
                r.raise_for_status()
                # The result is the workflow handle
                for obj in json.loads(r.text)['modules']:
                    modules.append(ModuleResource.from_dict(obj))
                return modules
            else:
                modules.append(m)
