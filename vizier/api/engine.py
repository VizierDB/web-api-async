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
from vizier.engine.base import TaskHandle
from vizier.viztrail.module import ModuleHandle, ModuleTimestamp
from vizier.viztrail.module import MODULE_PENDING
from vizier.viztrail.workflow import ACTION_DELETE, ACTION_INSERT, ACTION_REPLACE


class WorkflowEngineApi(object):
    """API wrapper around the viztrail repository and the backend execution
    engine that implements the logic to orchestrate workflow execution.

    Provides methods to add, delete, and replace modules in workflows and to
    update the status of active modules.
    """
    def __init__(self, datastore, filestore, viztrails_repository, backend, packages):
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
        backend: vizier.engine.backend.base.VizierBackend
            Backend to execute workflow modules
        packages: dict(vizier.engine.package.base.PackageIndex)
            Dictionary of packages
        """
        self.datastore = datastore
        self.filestore = filestore
        self.repository = viztrails_repository
        self.backend = backend
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
        # branch head. At the same time we retrieve the list of modules for the
        # current head of the branch.
        head = branch.get_head()
        if not head is None and len(head.modules) > 0:
            datasets = head.modules[-1].datasets
            modules = head.modules
        else:
            datasets = dict()
            modules = list()
        #Get the external representation for the command
        external_form = to_external_form(
            command=command,
            datasets=datasets,
            datastore=self.datastore,
            filestore=self.filestore,
            packages=self.packages
        )
        # Create new workflow by appending one module to the current head of the
        # branch
        workflow = branch.append_pending_workflow(
            modules=modules,
            pending_modules=[
                ModuleHandle(
                    command=command,
                    external_for=external_form
                )
            ],
            action=ACTION_INSERT,
            command=command
        )
        self.backend.execute_task(
            api=self,
            task=TaskHandle(
                viztrail_id=viztrail_id,
                branch_id=branch_id,
                module_id=workflow.modules[-1].identifier,
                external_form=external_form
            ),
            command=command,
            context=datasets
        )
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
                if module_index == module_count - 1:
                    break
                else:
                    datasets = modules[module_index].provenance.adjust_state(datasets)
                    module_index += 1
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
                # need to be executed
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
                self.backend.execute_task(
                    api=self,
                    task=TaskHandle(
                        viztrail_id=viztrail_id,
                        branch_id=branch_id,
                        module_id=workflow.modules[module_index].identifier,
                        external_form=external_form
                    ),
                    command=command,
                    context=datasets
                )
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

    def get_task(self, task):
        """Get the workflow and module index for the given task. Returns None
        and -1 if the workflow or module is undefined.

        Parameters
        ----------
        task: vizier.engine.packages.processor.TaskHandle
            Unique task identifier

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle, int
        """
        # Get the handle for the head workflow of the specified branch
        branch = self.get_branch(
            viztrail_id=task.viztrail_id,
            branch_id=task.branch_id
        )
        if branch is None:
            return None, -1
        head = branch.get_head()
        if head is None or len(head.modules) == 0:
            return None, -1
        # Find module (searching from end of list)
        i = 0
        for m in reversed(head.modules):
            i += 1
            if m.identifier == task.module_id:
                return head, len(head.modules) - i
        return None, -1

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
        self.backend.execute_task(
            task=TaskHandle(
                api=self,
                viztrail_id=viztrail_id,
                branch_id=branch_id,
                module_id=workflow.modules[module_index].identifier,
                external_form=inserted_module.external_form
            ),
            command=command,
            context=datasets
        )
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
        self.backend.execute_task(
            task=TaskHandle(
                api=self,
                viztrail_id=viztrail_id,
                branch_id=branch_id,
                module_id=workflow.modules[module_index].identifier
            ),
            command=command,
            context=datasets
        )
        return workflow

    def set_canceled(self, task, finished_at=None, outputs=None):
        """Set status of the identified module at the head of the specified
        viztrail branch to canceled. The finished_at property of the timestamp
        is set to the given value or the current time (if None). The module
        outputs are set to the given value. If no outputs are given the module
        output streams will be empty.

        Cancels all pending modules in the workflow.

        Returns the handle for the modified workflow. If the specified module
        is not in an active state the result is None.

        Parameters
        ----------
        task : vizier.engine.packages.processor.TaskHandle
            Unique task identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        # Get the handle for the head workflow of the specified branch and the
        # index for the module matching the identifier in the task.
        workflow, module_index = self.get_task(task)
        if workflow is None or module_index == -1:
            return None
        module = workflow.modules[module_index]
        if module.is_active:
            self.backend.cancel_task(task)
            module.set_canceled(finished_at=finished_at, outputs=outputs)
            for m in workflow.modules[module_index+1:]:
                m.set_canceled()
            return workflow
        else:
            return None

    def set_error(self, task, finished_at=None, outputs=None):
        """Set status of the identified module at the head of the specified
        viztrail branch to error. The finished_at property of the timestamp is
        set to the given value or the current time (if None). The module outputs
        are adjusted to the given value. the output streams are empty if no
        value is given for the outputs parameter.

        Cancels all pending modules in the workflow.

        Returns the handle for the modified workflow. If the specified module
        is not in an active state the result is None.

        Parameters
        ----------
        task : vizier.engine.packages.processor.TaskHandle
            Unique task identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        # Get the handle for the head workflow of the specified branch and the
        # index for the module matching the identifier in the task.
        workflow, module_index = self.get_task(task)
        if workflow is None or module_index == -1:
            return None
        module = workflow.modules[module_index]
        if module.is_active:
            module.set_error(finished_at=finished_at, outputs=outputs)
            for m in workflow.modules[module_index+1:]:
                m.set_canceled()
            return workflow
        else:
            return None

    def set_running(self, task, external_form, started_at=None):
        """Set status of the identified module at the head of the specified
        viztrail branch to running. The started_at property of the timestamp is
        set to the given value or the current time (if None).

        Returns the handle for the modified workflow. If the specified module
        is not in pending state the result is None.

        Parameters
        ----------
        task : vizier.engine.packages.processor.TaskHandle
            Unique task identifier
        external_form: string
            Adjusted external representation for the module command.
        started_at: datetime.datetime, optional
            Timestamp when module started running

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        # Get the handle for the head workflow of the specified branch and the
        # index for the module matching the identifier in the task.
        workflow, module_index = self.get_task(task)
        if workflow is None or module_index == -1:
            return None
        module = workflow.modules[module_index]
        if module.is_pending:
            module.set_running(
                external_form=external_form,
                started_at=started_at
            )
            return workflow
        else:
            return None

    def set_success(self, task, finished_at=None, datasets=None, outputs=None, provenance=None):
        """Set status of the identified module at the head of the specified
        viztrail branch to success. The finished_at property of the timestamp
        is set to the given value or the current time (if None).

        If case of a successful module execution the database state and module
        provenance information are also adjusted together with the module
        output streams. If the workflow has pending modules the first pending
        module will be executed next.

        Returns the handle for the modified workflow. If the specified module
        is not in pending state the result is None.

        Parameters
        ----------
        task : vizier.engine.packages.processor.TaskHandle
            Unique task identifier
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
        # Get the handle for the head workflow of the specified branch and the
        # index for the module matching the identifier in the task.
        workflow, module_index = self.get_task(task)
        if workflow is None or module_index == -1:
            return None
        module = workflow.modules[module_index]
        if module.is_running:
            module.set_success(
                finished_at=finished_at,
                datasets=datasets,
                outputs=outputs,
                provenance=provenance
            )
            context = datasets if not datasets is None else dict()
            for next_module in workflow.modules[module_index+1:]:
                if not next_module.is_pending:
                    return None
                elif not next_module.requires_exec(context):
                    context = next_module.provenance.adjust_state(context)
                    next_module.set_success(
                        finished_at=finished_at,
                        datasets=context,
                        outputs=next_module.outputs,
                        provenance=next_module.provenance
                    )
                else:
                    command = next_module.command
                    external_form = to_external_form(
                        command=command,
                        datasets=context,
                        datastore=self.datastore,
                        filestore=self.filestore,
                        packages=self.packages
                    )
                    self.backend.execute_task(
                        api=self,
                        task=TaskHandle(
                            viztrail_id=task.viztrail_id,
                            branch_id=task.branch_id,
                            module_id=next_module.identifier,
                            external_form=external_form
                        ),
                        command=command,
                        context=context
                    )
            return workflow
        else:
            return None


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
