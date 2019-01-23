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

"""Workflow controller for external (remote) workers. Sends task state changes
to the web service API via HTTP requests.
"""

import json
import requests

from vizier.engine.controller import WorkflowController

import vizier.api.serialize.base as serialize
import vizier.api.serialize.labels as labels
import vizier.api.serialize.dataset as serialds
import vizier.api.serialize.module as serialmd
import vizier.viztrail.module.base as states


class RemoteWorkflowController(WorkflowController):
    """Controller for tasks that are executed by a remote worker."""
    def __init__(self, urls):
        """Initialize the task url factory.

        Parameters
        ----------
        urls: vizier.api.routes.task.TaskUrlFactory
            Factory for url to update task state
        """
        self.urls = urls

    def get_url(self, task_id):
        """Get url to notify the web service about state changes for the given
        task.

        Parameters
        ----------
        task_id: string
            Unique task identifier

        Returns
        -------
        string
        """
        return self.urls.set_task_state(task_id=task_id)

    def set_error(self, task_id, finished_at=None, outputs=None):
        """Set status of a given task to error.

        If the web service signals that the task status was updated the result
        will be True, otherwise False. If the project or task were unknown the
        result is None.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        task_id : string
            Unique task identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module

        Returns
        -------
        bool
        """
        # Get the request url and create the request body
        url = self.get_url(task_id)
        data = {labels.STATE: states.MODULE_ERROR}
        if not finished_at is None:
            data[labels.FINISHED_AT] = finished_at.isoformat()
        if not outputs is None:
            data[labels.OUTPUTS] = serialize.OUTPUTS(outputs)
        # Send request
        r = requests.put(url, json=data)
        # A status code 200 will signal success. All other codes indicate an
        # error or unknown project or task identifier.
        if r.status_code != 200:
            return None
        result = json.loads(r.text)[labels.RESULT]
        return (result > 0)

    def set_running(self, task_id, started_at=None):
        """Set status of a given task to running.

        If the web service signals that the task status was updated the result
        will be True, otherwise False. If the project or task were unknown the
        result is None.

        Parameters
        ----------
        project_id : string
            Unique project identifier
        task_id : string
            Unique task identifier
        started_at: datetime.datetime, optional
            Timestamp when module started running

        Returns
        -------
        bool
        """
        # Get the request url and create the request body
        url = self.get_url(task_id)
        data = {labels.STATE: states.MODULE_RUNNING}
        if not started_at is None:
            data[labels.STARTED_AT] = started_at.isoformat()
        # Send request
        r = requests.put(url, json=data)
        # A status code 200 will signal success. All other codes indicate an
        # error or unknown project or task identifier.
        if r.status_code != 200:
            return None
        result = json.loads(r.text)[labels.RESULT]
        return (result > 0)

    def set_success(self, task_id, finished_at=None, datasets=None, outputs=None, provenance=None):
        """Set status of the module that is associated with the given task
        identifier to success.

        If the web service signals that the task status was updated the result
        will be True, otherwise False. If the project or task were unknown the
        result is None.

        Parameters
        ----------
        task_id : string
            Unique task identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        datasets : dict, optional
            Dictionary of resulting datasets. The user-specified name is the key
            for the dataset identifier.
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.

        Returns
        -------
        bool
        """
        # Get the request url and create the request body
        url = self.get_url(task_id)
        data = {labels.STATE: states.MODULE_SUCCESS}
        if not finished_at is None:
            data[labels.FINISHED_AT] = finished_at.isoformat()
        if not outputs is None:
            data[labels.OUTPUTS] = serialize.OUTPUTS(outputs)
        if not provenance is None:
            data[labels.PROVENANCE] = serialmd.PROVENANCE(provenance)
        # Send request
        r = requests.put(url, json=data)
        # A status code 200 will signal success. All other codes indicate an
        # error or unknown project or task identifier.
        if r.status_code != 200:
            return None
        result = json.loads(r.text)[labels.RESULT]
        return (result > 0)
