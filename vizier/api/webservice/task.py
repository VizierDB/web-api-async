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

"""Vizier Task API - Implements all methods of the API to interact with running
tasks in vizier projects.
"""

from vizier.core.timestamp import to_datetime

import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.labels as labels
import vizier.viztrail.module.base as states


class VizierTaskApi(object):
    """The Vizier task API implements the methods that interact with active
    task for vizier projects.
    """
    def __init__(self, engine):
        """Initialize the API components.

        Parameters
        ----------
        engine: vizier.engine.base.VizierEngine
            Instance of the API engine
        """
        self.engine = engine

    def update_task_state(self, task_id, state, body):
        """Update that state pf a given task. The contents of the request body
        depend on the value of the new task state.

        Raises a ValueError if the request body is invalid. The result is None
        if the task is unknown. Otherwise, the result is a dictionary with a
        single result value. The result is 0 if the task state did not change.
        A positive value signals a successful task state change.

        Parameters
        ----------
        task_id: string
            Unique task identifier
        state: int
            The new state of the task
        body: dict
            State-dependent additional information

        Returns
        -------
        dict
        """
        # Depending on the requested state change call the respective method
        # after extracting additional parameters from the request body.
        result = None
        if state == states.MODULE_RUNNING:
            if labels.STARTED_AT in body:
                result = self.engine.set_running(
                    task_id=task_id,
                    started_at=to_datetime(body[labels.STARTED_AT])
                )
            else:
                result = self.engine.set_running(task_id=task_id)
        elif state == states.MODULE_ERROR:
            finished_at = None
            if labels.FINISHED_AT in body:
                finished_at = to_datetime(body[labels.FINISHED_AT])
            outputs = None
            if labels.OUTPUTS in body:
                outputs = deserialize.OUTPUTS(body[labels.OUTPUTS])
            result = self.engine.set_error(
                task_id=task_id,
                finished_at=finished_at,
                outputs=outputs
            )
        elif state == states.MODULE_SUCCESS:
            finished_at = None
            if labels.FINISHED_AT in body:
                finished_at = to_datetime(body[labels.FINISHED_AT])
            outputs = None
            if labels.OUTPUTS in body:
                outputs = deserialize.OUTPUTS(body[labels.OUTPUTS])
            provenance = None
            if labels.PROVENANCE in body:
                provenance = deserialize.PROVENANCE(body[labels.PROVENANCE])
            result = self.engine.set_success(
                task_id=task_id,
                finished_at=finished_at,
                outputs=outputs,
                provenance=provenance
            )
        else:
            raise ValueError('invalid state change')
        # Create state change result
        if not result is None:
            return {labels.RESULT: result}
        return None
