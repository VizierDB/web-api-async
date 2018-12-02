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

from vizier.core.timestamp import get_current_time
from vizier.viztrail.module import ModuleHandle, ModuleTimestamp
from vizier.viztrail.module import MODULE_PENDING
from vizier.viztrail.workflow import ACTION_DELETE, ACTION_INSERT, ACTION_REPLACE


class WorkflowEngineApi(object):
    """API wrapper around the viztrail repository and the backend execution
    engine that implements the logic to orchestrate workflow execution.

    Provides methods to add, delete, and replace modules in workflows and to
    update the status of active modules.
    """
    def __init__(self, datastore, filestore, viztrails_repository, packages):
        """Initialize the viztrails repository and the execution backend that
        are used to orchestrate workflow execution.

        Parameters
        ----------
        datastore: vizier.datastore.base.Datastore
            Backend store for datasets
        filestore: vizier.filestore.base.Filestore
            Backend store for uploaded files
        viztrails_repository: vizier.viztrail.repository.ViztrailRepository
            The viztrail repository manager for the Vizier instance
        packages: dict(vizier.engine.package.base.PackageIndex)
            Dictionary of packages
        """
        self.datastore = datastore
        self.filestore = filestore
        self.repository = viztrails_repository
        self.packages = packages

    def append_workflow_module(self, viztrail_id, branch_id, command):
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
        # Get the handle for the specified branch
        branch = self.get_branch(viztrail_id=viztrail_id, branch_id=branch_id)
        if branch is None:
            return None
        # Get the current database state from the last module in the current
        # branch head
        head = branch.get_head()
        if not head is None and len(head.modules) > 0:
            datasets = head.modules[-1].datasets
        else:
            datasets = dict()
        #Get the external representation for the command
        external_form = to_external_form(
            command=command,
            datasets=datasets,
            datastore=self.datastore,
            filestore=self.filestore,
            packages=self.packages
        )
        # Check if the backend can execute the given command immediately
        if self.backend.can_execute(command):
            ts_start = get_current_time()
            result = self.backend.execute(command=command, context=datasets)
            ts_finish = get_current_time()
            workflow = branch.append_module(
                command=command,
                external_form=external_form,
                state=result.state,
                datasets=result.datasets,
                outputs=result.outputs,
                provenance=result.provenance,
                timestamp=ModuleTimestamp(
                    created_at=ts_start,
                    started_at=ts_start,
                    finished_at=ts_finish
                )
            )
        else:
            workflow = branch.append_module(
                command=command,
                external_form=external_form,
                state=MODULE_PENDING,
                timestamp=ModuleTimestamp(
                    created_at=get_current_time()
                )
            )
            self.backend.execute(command=command, context=datasets)
        return workflow

    def delete_workflow_module(self, viztrail_id, branch_id, module_id):
        """Delete the module with the given identifier from the workflow at the
        head of the specified viztrail branch. The resulting workflow is
        executed and will be the new head of the branch.

        Returns the handle for the modified workflow. The result is None if the
        viztrail, branch or module do not exist. Raises ValueError if the
        current head of the branch is active.

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
        # Get the handle for the specified branch and the branch head
        branch = self.get_branch(viztrail_id=viztrail_id, branch_id=branch_id)
        if branch is None:
            return None
        head = branch.get_head()
        if head is None or len(head.modules) == 0:
            return None
        # Raise ValueError if the head workflow is active
        if head.is_active:
            raise ValueError('cannot delete from active workflow')
        # Get the index of the module that is being deleted
        module_index = None
        for i in range(len(head.modules)):
            if head.modules[i].identifier == module_id:
                module_index = i
                break
        if module_index is None:
            return None
        deleted_module = head.modules[module_index]
        # Create module list for new workflow
        modules = head.modules[:module_index] + head.modules[module_index+1:]
        # Re-execute modules unless the last module was deleted or the module
        # list is empty
        module_count = len(modules)
        if module_count > 0 and module_index < module_count:
            # Get the context for the first module that requires re-execution.
            if module_index > 0:
                datasets = modules[module_index - 1].datasets
            else:
                datasets = dict()
            while not modules[module_index].provenance.requires_exec(datasets):
                module_index += 1
                if module_index == module_count:
                    break
                else:
                    datasets = modules[module_index - 1].datasets
            if module_index < module_count:
                # The module that module_index points to has to be executed.
                # Create a workflow that contains pending copies of all modules
                # that require execution and run the first of these modules.
                command = modules[module_index].command
                external_form = to_external_form(
                    command=command,
                    datasets=datasets,
                    datastore=self.datastore,
                    filestore=self.filestore,
                    packages=self.packages
                )
                # Replace original modules with pending modules for those that
                # need to be executes
                pending_modules = [
                    ModuleHandle(
                        command=command,
                        external_for=external_form
                    )
                ]
                for i in range(module_index + 1, module_count):
                    m = modules[i]
                    pending_modules.append(
                        ModuleHandle(
                            command=m.command,
                            external_for=m.external_form,
                            datasets=m.datasets,
                            outputs=m.outputs,
                            provenance=m.provenance
                        )
                    )
                workflow = branch.append_pending_workflow(
                    modules=modules[:module_index],
                    pending_modules=pending_modules,
                    action=ACTION_DELETE,
                    command=deleted_module.command
                )
                self.backend.execute(command=command, context=datasets)
                return workflow
            else:
                # None of the module required execution and the workflow is
                # complete
                return branch.append_completed_workflow(
                    modules=modules,
                    action=ACTION_DELETE,
                    command=deleted_module.command
                )
        else:
            return branch.append_completed_workflow(
                modules=modules,
                action=ACTION_DELETE,
                command=deleted_module.command
            )

    def get_branch(self, viztrail_id, branch_id):
        """Shortcut to get a specified branch. Returns None if either the
        viztrail or the branch are unknown.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        vt = self.repository.get_viztrail(viztrail_id)
        if vt is None:
            return None
        return vt.get_branch(branch_id)

    def insert_workflow_module(self, viztrail_id, branch_id, before_module_id, command):
        """Insert a new module to the workflow at the head of the given viztrail
        branch. The modified workflow will be executed. The result is the new
        head of the branch.

        The module is inserted into the sequence of workflow modules before the
        module with the identifier that is given as the before_module_id
        argument.

        Returns the handle for the modified workflow. The result is None if
        the specified viztrail, branch, or module do not exist. Raises
        ValueError if the current head of the branch is active.

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
        # Get the handle for the specified branch and the branch head
        branch = self.get_branch(viztrail_id=viztrail_id, branch_id=branch_id)
        if branch is None:
            return None
        head = branch.get_head()
        if head is None or len(head.modules) == 0:
            return None
        # Raise ValueError if the head workflow is active
        if head.is_active:
            raise ValueError('cannot insert into active workflow')
        # Get the index of the module at which the new module is inserted
        module_index = None
        modules = head.modules
        for i in range(len(modules)):
            if modules[i].identifier == before_module_id:
                module_index = i
                break
        if module_index is None:
            return None
        # Get handle for the inserted module
        if module_index > 0:
            datasets = modules[module_index - 1].datasets
        else:
            datasets = dict()
        inserted_module = ModuleHandle(
            command=command,
            external_form=to_external_form(
                command=command,
                datasets=datasets,
                datastore=self.datastore,
                filestore=self.filestore,
                packages=self.packages
            )
        )
        # Create list of pending modules for the new workflow
        pending_modules = [inserted_module]
        for m in modules[module_index:]:
            pending_modules.append(
                ModuleHandle(
                    command=m.command,
                    external_for=m.external_form,
                    datasets=m.datasets,
                    outputs=m.outputs,
                    provenance=m.provenance
                )
            )
        workflow = branch.append_pending_workflow(
            modules=modules[:module_index],
            pending_modules=pending_modules,
            action=ACTION_INSERT,
            command=inserted_module.command
        )
        self.backend.execute(command=command, context=datasets)
        return workflow

    def replace_workflow_module(self, viztrail_id, branch_id, module_id, command):
        """Replace an existing module in the workflow at the head of the
        specified viztrail branch. The modified workflow is executed and the
        result is the new head of the branch.

        Returns a handle for the new workflow. Returns None if the specified
        viztrail, branch, or module do not exist. Raises ValueError if the
        current head of the branch is active.

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
        # Get the handle for the specified branch and the branch head
        branch = self.get_branch(viztrail_id=viztrail_id, branch_id=branch_id)
        if branch is None:
            return None
        head = branch.get_head()
        if head is None or len(head.modules) == 0:
            return None
        # Raise ValueError if the head workflow is active
        if head.is_active:
            raise ValueError('cannot replace in active workflow')
        # Get the index of the module that is being replaced
        module_index = None
        modules = head.modules
        for i in range(len(modules)):
            if modules[i].identifier == module_id:
                module_index = i
                break
        if module_index is None:
            return None
        # Get handle for the replaced module
        if module_index > 0:
            datasets = modules[module_index - 1].datasets
        else:
            datasets = dict()
        replaced_module = ModuleHandle(
            command=command,
            external_form=to_external_form(
                command=command,
                datasets=datasets,
                datastore=self.datastore,
                filestore=self.filestore,
                packages=self.packages
            )
        )
        # Create list of pending modules for the new workflow
        pending_modules = [replaced_module]
        for m in modules[module_index+1:]:
            pending_modules.append(
                ModuleHandle(
                    command=m.command,
                    external_for=m.external_form,
                    datasets=m.datasets,
                    outputs=m.outputs,
                    provenance=m.provenance
                )
            )
        workflow = branch.append_pending_workflow(
            modules=modules[:module_index],
            pending_modules=pending_modules,
            action=ACTION_REPLACE,
            command=replaced_module.command
        )
        self.backend.execute(command=command, context=datasets)
        return workflow

    def set_canceled(self, viztrail_id, branch_id, finished_at=None, outputs=None):
        """Set status of the first active module at the head of the specified
        viztrail branch to canceled. The finished_at property of the timestamp
        is set to the given value or the current time (if None). The module
        outputs are set to the given value. If no outputs are given the module
        output streams will be empty.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string
            Unique branch identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def set_error(self, viztrail_id, branch_id, finished_at=None, outputs=None):
        """Set status of the first active module at the head of the specified
        viztrail branch to error. The finished_at property of the timestamp is
        set to the given value or the current time (if None). The module outputs
        are adjusted to the given value. the output streams are empty if no
        value is given for the outputs parameter.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string
            Unique branch identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def set_running(self, viztrail_id, branch_id, started_at=None, external_form=None):
        """Set status of the first active module at the head of the specified
        viztrail branch to running. The started_at property of the timestamp is
        set to the given value or the current time (if None).

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string
            Unique branch identifier
        started_at: datetime.datetime, optional
            Timestamp when module started running
        external_form: string, optional
            Adjusted external representation for the module command.

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    def set_success(self, viztrail_id, branch_id, finished_at=None, datasets=None, outputs=None, provenance=None):
        """Set status of the first active module at the head of the specified
        viztrail branch to success. The finished_at property of the timestamp
        is set to the given value or the current time (if None).

        If case of a successful module execution the database state and module
        provenance information are also adjusted together with the module
        output streams.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string
            Unique branch identifier
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


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def to_external_form(command, datasets, datastore, filestore, packages):
    """Get the external form for a given module command.

    Parameters
    ----------
    command: vizier.viztrail.module.ModuleCommand
        Specification of the command
    datasets: dict()
        Available dataset identifiers keyed by the user-defined dataset name
    datastore: vizier.datastore.base.Datastore
        Backend store for datasets
    filestore: vizier.filestore.base.Filestore
        Backend store for uploaded files
    packages: dict(vizier.engine.package.base.PackageIndex)
        Dictionary of packages

    Returns
    -------
    string
    """
    # To get the external representation for the command we first need to
    # have a dictionary of dataset handles keyed by the user-defined name.
    database_state = dict()
    for ds_name in datasets:
        ds_id = datasets[ds_name]
        database_state[ds_name] = datastore.get_dataset(ds_id)
    return command.to_external_form(
        command=packages[command.package_id].get(command.command_id),
        datasets=database_state,
        filestore=filestore
    )
