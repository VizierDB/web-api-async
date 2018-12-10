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

"""A vizier data curation project is a wrapper for the workflows, datastore,
filestore and execution backend that are responsible for various functionality
required by the projects.

The main functionality provided by the project handle is to orchestrate the
execution of individual steps in the associated data curation workflow.
"""

from vizier.core.timestamp import get_current_time
from vizier.core.util import get_unique_identifier
from vizier.engine.controller import WorkflowController
from vizier.engine.task.base import TaskHandle
from vizier.viztrail.module.base import ModuleHandle
from vizier.viztrail.module.base import MODULE_PENDING, MODULE_ERROR, MODULE_RUNNING, MODULE_SUCCESS
from vizier.viztrail.module.timestamp import ModuleTimestamp
from vizier.viztrail.workflow import ACTION_DELETE, ACTION_INSERT, ACTION_REPLACE


class ExtendedTaskHandle(TaskHandle):
    """Extend task handle with additional information that is used internally
    when the state of the task changes.

    Adds branch and module identifier to the task handle as well as the
    external representation of the executed command.
    """
    def __init__(self, viztrail_id, branch_id, module_id, controller, external_form=None):
        """Initialize the components of the extended task handle. Generates a
        unique identifier for the task.

        Parameters
        ----------
        viztrail_id: string
            Unique identifier of the associated viztrail
        branch_id: string
            Unique branch identifier
        module_id: string
            Unique module identifier
        controller: vizier.engine.controller.WorkflowController
            Controller for associates workflow engine
        external_form: string, optional
            External representation of the executed command
        """
        super(ExtendedTaskHandle, self).__init__(
            task_id=get_unique_identifier(),
            viztrail_id=viztrail_id,
            controller=controller
        )
        self.branch_id = branch_id
        self.module_id = module_id
        self.external_form = external_form


class ProjectHandle(WorkflowController):
    """The project handle is a wrapper around the main components of a vizier
    data curation project. These components provide all the functionality that
    is required by the project. Each project can therefore run in isolation from
    other projects.

    The individual components of a project are: the viztrail that maintains the
    project branches and workflows, the datatore to maintain all generated
    datasets, the filestore for uploaded files, and the backend to execute
    workflow modules.

    The project handle orchestrates the execution of curation workflows. This
    class provides methods to add, delete, and replace modules in workflows and
    to update the status of active workflow modules.
    """
    def __init__(self, viztrail, datastore, filestore, backend, packages):
        """Initialize the project components.

        Parameters
        ----------
        viztrail: vizier.viztrail.base.ViztrailHandle
            The viztrail repository manager for the Vizier instance
        datastore: vizier.datastore.base.Datastore
            Associated datastore
        filestore: vizier.filestore.base.Filestore
            Associated filestore
        backend: vizier.engine.backend.base.VizierBackend
            Backend to execute workflow modules
        packages: dict(vizier.engine.package.base.PackageIndex)
            Dictionary of loaded packages
        """
        self.viztrail = viztrail
        self.datastore = datastore
        self.filestore = filestore
        self.backend = backend
        self.packages = packages
        # Maintain an internal dictionary of running tasks
        self.tasks = dict()

    def append_workflow_module(self, branch_id, command):
        """Append module to the workflow at the head of the given viztrail
        branch. The modified workflow will be executed. The result is the new
        head of the branch.

        Returns the handle for the modified workflow. The result is None if
        the specified branch does not exist.

        Parameters
        ----------
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
        branch = self.viztrail.get_branch(branch_id)
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
        # Get the external representation for the command
        external_form = command.to_external_form(
            command=self.packages[command.package_id].get(command.command_id)
            datasets=datasets,
            filestore=self.filestore
        )
        # If the workflow is not active and the command can be executed
        # synchronously we run the command immediately and return the completed
        # workflow. Otherwise, a pending workflow is created.
        if not head.is_active and self.backend.can_execute(command):
            ts_start = get_current_time()
            result = self.backend.execute(
                command=command,
                context=self.task_context(datasets)
            )
            ts_finish = get_current_time()
            # Depending on the execution outcome create a handle for the
            # executed module
            if result.is_success:
                module = ModuleHandle(
                    state=MODULE_SUCCESS,
                    command=command,
                    external_for=external_form,
                    timestamp=ModuleTimestamp(
                        created_at=ts_start,
                        started_at=ts_start,
                        finished_at=ts_finish
                    ),
                    datasets=self.get_database_state(
                        workflow=head,
                        module_index=len(head.modules) - 1,
                        datasets=result.datasets
                    ),
                    outputs=result.outputs,
                    provenance=result.provenance
                )
            else:
                module = ModuleHandle(
                    state=MODULE_ERROR,
                    command=command,
                    external_for=external_form,
                    outputs=result.outputs
                )
            return branch.append_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command,
                pending_modules=[module]
            )
        else:
            # Create new workflow by appending one module to the current head of
            # the branch. The module state is pending if the workflow is active
            # otherwise it depends on the associated backend.
            state = self.backend.next_task_state() if not head.is_active else MODULE_PENDING
            workflow = branch.append_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command,
                pending_modules=[
                    ModuleHandle(
                        state=state,
                        command=command,
                        external_for=external_form
                    )
                ]
            )
            self.execute_module(
                branch_id=branch_id,
                module=workflow.modules[-1],
                datasets=datasets
            )
            return workflow

    def delete_workflow_module(self, branch_id, module_id):
        """Delete the module with the given identifier from the workflow at the
        head of the viztrail branch. The resulting workflow is executed and will
        be the new head of the branch.

        Returns the handle for the modified workflow. The result is None if the
        branch or module do not exist. Raises ValueError if the current head of
        the branch is active.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier
        module_id : string
            Unique module identifier

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        # Get the handle for the specified branch and the branch head
        branch = self.viztrail.get_branch(branch_id)
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
                    datasets = modules[module_index].provenance.adjust_state(
                        datasets,
                        datastore=self.datastore
                    )
                    module_index += 1
            if module_index < module_count:
                # The module that module_index points to has to be executed.
                # Create a workflow that contains pending copies of all modules
                # that require execution and run the first of these modules.
                command = modules[module_index].command
                external_form = command.to_external_form(
                    command=self.packages[command.package_id].get(command.command_id),
                    datasets=datasets,
                    filestore=self.filestore
                )
                # Replace original modules with pending modules for those that
                # need to be executed. The state of the first module depends on
                # the state of the backend. All following modules will be in
                # pending state.
                pending_modules = [
                    ModuleHandle(
                        command=command,
                        state=self.backend.next_task_state(),
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
                workflow = branch.append_workflow(
                    modules=modules[:module_index],
                    action=ACTION_DELETE,
                    command=deleted_module.command,
                    pending_modules=pending_modules
                )
                self.execute_module(
                    branch_id=branch_id,
                    module=workflow.modules[module_index],
                    datasets=datasets
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

    def execute_module(self, branch_id, module, datasets, external_form=None):
        """Create a new task for the given module and execute the module in
        asynchronous mode.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier
        module: vizier.viztrail.module.ModuleHandle
            handle for executed module
        datasets: dict(vizier.datastore.dataset.DatasetDescriptor)
            Index of datasets in the a database state
        external_form: string, optional
            External representation of the executed command
        """
        task = ExtendedTaskHandle(
            viztrail_id=self.viztrail.identifier,
            branch_id=branch_id,
            module_id=module.identifier,
            controller=self,
            external_form=external_form
        )
        self.tasks[task.task_id] = task
        self.backend.execute_async(
            task=task,
            command=module.command,
            context=self.task_context(datasets),
            resources=module.provenance.resources
        )

    def get_database_state(self, workflow, module_index, datasets):
        """Adjust the database state after module execution. The dictionary of
        datasets contains names and identifier of datasets in the new database
        state. To avoid reading descriptors for unchanged datasets from storage
        the database state of the previous module is used. Returns a dictionary
        that contains dataset descriptors instead of dataset identifier.

        Parameters
        ----------
        workflow: vizier.viztrail.workflow.WorkflowHandle
            Workflow that is being modified
        module_index: int
            Index position of the workflow modules that has been modified
        datasets: dict()
            Dictionary of dataset names and identifier in the new database
            state after module execution

        Returns
        -------
        dict(vizier.datastore.dataset.DatasetDescriptor)
        """
        if datasets is None:
            return dict()
        context = dict()
        if module_index > 0:
            prev_state = workflow.modules[module_index - 1].datasets
            for ds_name in prev_state:
                if ds_name in datasets:
                    context[ds_name] = prev_state[ds_name]
        for ds_name in datasets:
            ds_id = datasets[ds_name]
            if ds_name in context:
                if context[ds_name].identifier != ds_id:
                    context[ds_name] = self.datastore.get_descriptor(ds_id)
            else:
                context[ds_name] = self.datastore.get_descriptor(ds_id)
        return context

    def get_task(self, task_id):
        """Get the workflow and module index for the given task. Returns None
        and -1 if the workflow or module is undefined.

        Removes the tast from the internal task index.

        Parameters
        ----------
        task: vizier.engine.task.base.TaskHandle
            Unique task identifier

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle, int
        """
        # Check if a task with the given identifier exists
        if not task_id in self.tasks:
            return None, -1
        # Get the task handle and remove it from the internal task index
        task = self.tasks[task_id]
        del self.tasks[task_id]
        # Get the handle for the head workflow of the specified branch
        branch = self.viztrail.get_branch(task.branch_id)
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

    @property
    def identifier(self):
        """Unique project identifier. The project identifier is the same
        identifier as the unique identifier for the associated viztrail.

        Returns
        -------
        string
        """
        return self.viztrail.identifier

    def insert_workflow_module(self, branch_id, before_module_id, command):
        """Insert a new module to the workflow at the head of the given viztrail
        branch. The modified workflow will be executed. The result is the new
        head of the branch.

        The module is inserted into the sequence of workflow modules before the
        module with the identifier that is given as the before_module_id
        argument.

        Returns the handle for the modified workflow. The result is None if
        the specified branch or module do not exist. Raises ValueError if the
        current head of the branch is active.

        Parameters
        ----------
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
        branch = self.viztrail.get_branch(branch_id)
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
        # Create handle for the inserted module. The state on the module depends
        # on the state of the backend.
        inserted_module = ModuleHandle(
            command=command,
            state=self.backend.next_task_state(),
            external_form=command.to_external_form(
                command=self.packages[command.package_id].get(command.command_id),
                datasets=datasets,
                filestore=self.filestore
            )
        )
        # Create list of pending modules for the new workflow.
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
        workflow = branch.append_workflow(
            modules=modules[:module_index],
            action=ACTION_INSERT,
            command=inserted_module.command,
            pending_modules=pending_modules
        )
        self.execute_module(
            branch_id=branch_id,
            module=workflow.modules[module_index],
            datasets=datasets
        )
        return workflow

    def replace_workflow_module(self, branch_id, module_id, command):
        """Replace an existing module in the workflow at the head of the
        specified viztrail branch. The modified workflow is executed and the
        result is the new head of the branch.

        Returns a handle for the new workflow. Returns None if the specified
        branch or module do not exist. Raises ValueError if the current head of
        the branch is active.

        Parameters
        ----------
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
        branch = self.viztrail.get_branch(branch_id)
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
        # Get handle for the replaced module. Keep any resource information from
        # the provenance object of the previous module execution. The state of
        # the module depends on the state of the backend.
        if module_index > 0:
            datasets = modules[module_index - 1].datasets
        else:
            datasets = dict()
        replaced_module = ModuleHandle(
            command=command,
            state=self.backend.next_task_state(),
            external_form=command.to_external_form(
                command=self.packages[command.package_id].get(command.command_id),
                datasets=datasets,
                filestore=self.filestore
            ),
            provenance=ModuleProvenance(
                resources=modules[module_index].provenance.resources
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
        workflow = branch.append_workflow(
            modules=modules[:module_index],
            action=ACTION_REPLACE,
            command=replaced_module.command,
            pending_modules=pending_modules
        )
        self.execute_module(
            branch_id=branch_id,
            module=workflow.modules[module_index],
            datasets=datasets
        )
        return workflow

    def set_canceled(self, task_id, finished_at=None, outputs=None):
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
        task_id : string
            Unique task identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        # Get the handle for the head workflow of the specified branch and the
        # index for the module matching the identifier in the task.
        workflow, module_index = self.get_task(task_id)
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

    def set_error(self, task_id, finished_at=None, outputs=None):
        """Set status of the module that is associated with the given task
        identifier to error. The finished_at property of the timestamp is set
        to the given value or the current time (if None). The module outputs
        are adjusted to the given value. The output streams are empty if no
        value is given for the outputs parameter.

        Cancels all pending modules in the workflow.

        Returns the handle for the modified workflow. If the specified module
        is not in an active state the result is None.

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
        vizier.viztrail.workflow.WorkflowHandle
        """
        # Get the handle for the head workflow of the specified branch and the
        # index for the module matching the identifier in the task.
        workflow, module_index = self.get_task(task_id)
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

    def set_running(self, task_id, started_at=None):
        """Set status of the module that is associated with the given task
        identifier to running. The started_at property of the timestamp is
        set to the given value or the current time (if None).

        Returns the handle for the modified workflow. If the specified module
        is not in pending state the result is None.

        Parameters
        ----------
        task_id : string
            Unique task identifier
        started_at: datetime.datetime, optional
            Timestamp when module started running

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        # Get the handle for the head workflow of the specified branch and the
        # index for the module matching the identifier in the task.
        workflow, module_index = self.get_task(task_id)
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

    def set_success(self, task_id, finished_at=None, datasets=None, outputs=None, provenance=None):
        """Set status of the module that is associated with the given task
        identifier to success. The finished_at property of the timestamp
        is set to the given value or the current time (if None).

        If case of a successful module execution the database state and module
        provenance information are also adjusted together with the module
        output streams. If the workflow has pending modules the first pending
        module will be executed next.

        Returns the handle for the modified workflow. If the specified module
        is not in pending state the result is None.

        Parameters
        ----------
        task_id : string
            Unique task identifier
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        datasets : dict(), optional
            Dictionary of resulting datasets. The user-specified name is the key
            for the dataset identifier.
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        # Get the handle for the head workflow of the specified branch and the
        # index for the module matching the identifier in the task.
        workflow, module_index = self.get_task(task_id)
        if workflow is None or module_index == -1:
            return None
        module = workflow.modules[module_index]
        if module.is_running:
            # Adjust the module context
            context = self.get_database_state(
                workflow=workflow,
                module_idex=module_index,
                datasets=datasets
            )
            module.set_success(
                finished_at=finished_at,
                datasets=context,
                outputs=outputs,
                provenance=provenance
            )
            for next_module in workflow.modules[module_index+1:]:
                if not next_module.is_pending:
                    return None
                elif not next_module.requires_exec(context):
                    context = next_module.provenance.adjust_state(
                        context,
                        datastore=self.datastore
                    )
                    next_module.set_success(
                        finished_at=finished_at,
                        datasets=context,
                        outputs=next_module.outputs,
                        provenance=next_module.provenance
                    )
                else:
                    command = next_module.command
                    external_form = command.to_external_form(
                        command=self.packages[command.package_id].get(command.command_id),
                        datasets=context,
                        filestore=self.filestore
                    )
                    # If the backend is going to run the task immediately we
                    # need to adjust the module state
                    state = self.backend.next_task_state()
                    if state == MODULE_RUNNING:
                        next_module.set_running(
                            external_form=get_current_time(),
                            started_at=started_at
                        )
                    self.execute_module(
                        branch_id=task.branch_id,
                        module=next_module,
                        datasets=context,
                        external_form=external_form
                    )
            return workflow
        else:
            return None


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def task_context(datasets):
    """Convert a database state into a task context. The database state is a
    dictionary where dataset descriptors are indexed by the user defined name.
    The task context is a dictionary where dataset identifier are indexed by
    the user-defined name.

    Parameters
    ----------
    datasets: dict(vizier.datastore.dataset.DatasetDescriptor)
        Index of datasets in the a database state

    Returns
    -------
    dict()
    """
    return {name: datasets[name].identifier for name in datasets}
