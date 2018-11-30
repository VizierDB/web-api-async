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

"""Vizier API - Workflow engine - Implements the methods of the API that
orchestrate the execution of data curation workflows.
"""

class WorkflowEngineApi(object):
    """API wrapper around the viztrail repository and the backend execution
    engine that implements the logic to orchestrate workflow execution.

    Provides methods to add, delete, and replace modules in workflows and to
    update the status of active modules.
    """
    def __init__(self, viztrails_repository):
        """Initialize the viztrails repository and the execution backend that
        are used to orchestrate workflow execution.

        Parameters
        ----------
        viztrails_repository: vizier.viztrail.repository.ViztrailRepository
            The viztrail repository manager for the Vizier instance
        """
        self.repository = viztrails_repository

    def append_workflow_module(self, viztrail_id, branch_id, before_module_id, command):
        """Append module to the workflow at the head of the given viztrail
        branch. The modified workflow will be executed. The result is the new
        head of the branch.

        Returns the handle for the modified workflow. The result is None if
        the specified viztrail or branch do not exist.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string
            Unique branch identifier
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed by the appended
            workflow module

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def insert_workflow_module(self, viztrail_id, branch_id, before_module_id, command):
        """Insert a new module to the workflow at the head of the given viztrail
        branch. The modified workflow will be executed. The result is the new
        head of the branch.

        The module is inserted into the sequence of workflow modules before the
        module with the identifier that is given as the before_module_id
        argument.

        Returns the handle for the modified workflow. The result is None if
        the specified viztrail, branch, or module do not exist.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string
            Unique branch identifier
        before_module_id : string
            Insert new module before module with given identifier.
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed by the inserted
            workflow module

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def delete_workflow_module(self, viztrail_id, branch_id, module_id):
        """Delete the module with the given identifier from the workflow at the
        head of the specified viztrail branch. The resulting workflow is execute
        as the new head of the branch.

        Returns the handle for the modified workflow. The result is None if the
        viztrail, branch or module do not exist.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id: string
            Unique branch identifier
        module_id : string
            Unique module identifier

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def replace_workflow_module(self, viztrail_id, branch_id, module_id, command):
        """Replace an existing module in the workflow at the head of the
        specified viztrail branch. The modified workflow is executed and the
        result is the new head of the branch.

        Returns a handle for the new workflow. Returns None if the specified
        viztrail, branch, or module do not exist.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier
        module_id : string
            Identifier of the module that is being replaced
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be evaluated

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def set_canceled(self, finished_at=None, outputs=None):
        """Set status of the module to canceled. The finished_at property of the
        timestamp is set to the given value or the current time (if None). The
        module outputs are set to the given value. If no outputs are given the
        module output streams will be empty.

        Parameters
        ----------
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def set_error(self, finished_at=None, outputs=None):
        """Set status of the module to error. The finished_at property of the
        timestamp is set to the given value or the current time (if None). The
        module outputs are adjusted to the given value. the output streams are
        empty if no value is given for the outputs parameter.

        Parameters
        ----------
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def set_running(self, started_at=None, external_form=None):
        """Set status of the module to running. The started_at property of the
        timestamp is set to the given value or the current time (if None).

        Parameters
        ----------
        started_at: datetime.datetime, optional
            Timestamp when module started running
        external_form: string, optional
            Adjusted external representation for the module command.

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def set_success(self, finished_at=None, datasets=None, outputs=None, provenance=None):
        """Set status of the module to success. The finished_at property of the
        timestamp is set to the given value or the current time (if None).

        If case of a successful module execution the database state and module
        provenance information are also adjusted together with the module
        output streams.

        Parameters
        ----------
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        datasets : dict(string:string), optional
            Dictionary of resulting datasets. The user-specified name is the key
            and the unique dataset identifier the value.
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module
        provenance: vizier.viztrail.module.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError
