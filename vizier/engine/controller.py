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

"""Interface for workflow controller that is passed to the execution backend to
signal state changes in workflow execution. The workflow module whoses state
changes is identified by the unique task identifier. The workflow engine, i.e.,
the project handle, is responsible for keeping track of the task identifier and
their mapping to workflow modules.
"""

from abc import abstractmethod


class WorkflowController(object):
    """The workflow controller is an interface that allows the execution
    backend to single state changes of a workflow module to the project handle
    that orchestrates workflow execution.

    Note that the controller does not define a method to cancel the execution
    of a workflow module. Cancelation of module excution is only triggered by
    the user and not the backend.
    """
    @abstractmethod
    def set_error(self, task_id, finished_at=None, outputs=None):
        """Set status of the module that is associated with the given task
        identifier to error. The finished_at property of the timestamp is set
        to the given value or the current time (if None). The module outputs
        are adjusted to the given value. The output streams are empty if no
        value is given for the outputs parameter.

        Cancels all pending modules in the workflow.

        Returns True if the state of the workflow was changed and False
        otherwise. The result is None if the project or task did not exist.

        Parameters
        ----------
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
        raise NotImplementedError

    @abstractmethod
    def set_running(self, task_id, started_at=None):
        """Set status of the module that is associated with the given task
        identifier to running. The started_at property of the timestamp is
        set to the given value or the current time (if None).

        Returns True if the state of the workflow was changed and False
        otherwise. The result is None if the project or task did not exist.

        Parameters
        ----------
        task_id : string
            Unique task identifier
        started_at: datetime.datetime, optional
            Timestamp when module started running

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def set_success(self, task_id, finished_at=None, datasets=None, outputs=None, provenance=None):
        """Set status of the module that is associated with the given task
        identifier to success. The finished_at property of the timestamp
        is set to the given value or the current time (if None).

        If case of a successful module execution the database state and module
        provenance information are also adjusted together with the module
        output streams. If the workflow has pending modules the first pending
        module will be executed next.

        Returns True if the state of the workflow was changed and False
        otherwise. The result is None if the project or task did not exist.

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
        raise NotImplementedError
