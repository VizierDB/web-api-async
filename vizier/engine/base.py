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

"""The vizier engine defines the interface that is used by the API for creating,
deleting, and manipulating projects as well as for the orchestration of workflow
modules. Different instantiations of the engine will use different
implementations for datasores, filestores, vitrails repositories and backends.

The idea is to have a set of pre-defined 'configurations' for vizier engines
that can be used in different environments (i.e., (a) when running vizier on a
local machine, (b) with a set of celery workers, (c) running each project in
its own container, etc). The engine that is used by a vizier instance is
specified in the configuration file and loaded when the instance is started.
"""

from vizier.core.timestamp import get_current_time
from vizier.core.util import get_unique_identifier
from vizier.engine.controller import WorkflowController
from vizier.engine.task.base import TaskHandle
from vizier.viztrail.module.base import ModuleHandle
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp


import vizier.viztrail.workflow as wf
import vizier.viztrail.module.base as mstate


class ExtendedTaskHandle(TaskHandle):
    """Extend task handle with additional information that is used internally
    when the state of the task changes.

    Adds branch and module identifier to the task handle as well as the
    external representation of the executed command.
    """
    def __init__(self, project_id, branch_id, module_id, controller):
        """Initialize the components of the extended task handle. Generates a
        unique identifier for the task.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        module_id: string
            Unique module identifier
        controller: vizier.engine.base.VizierEngine
            Reference to the vizier engine
        """
        super(ExtendedTaskHandle, self).__init__(
            task_id=get_unique_identifier(),
            project_id=project_id,
            controller=controller
        )
        self.branch_id = branch_id
        self.module_id = module_id


class VizierEngine(WorkflowController):
    """Engine that is used by the API to create and manipulate projects. The
    engine maintains viztrails that represent user projects. At its base the
    engine is a wrapper around the viztrails repository and the execution
    backend. Depending on the implementation the engine may further include
    the datastore, and filestore. Different configurations for a vizier instance
    will use different classes for the wrapped objects. Each configuration
    should have a descriptive name and version information (for display purposes
    in the front-end).
    """
    def __init__(self, name, projects, backend, packages):
        """Initialize the engine components.

        Parameters
        ----------
        name: string
            Descriptive name for the configuration
        projects: vizier.engine.project.cache.base.ProjectCache
            Cache for project handles
        backend: vizier.engine.backend.base.VizierBackend
            Backend to execute workflow modules
        packages: dict(vizier.engine.package.base.PackageIndex)
            Dictionary of loaded packages
        """
        self.name = name
        self.projects = projects
        self.backend = backend
        self.packages = packages
        # Maintain an internal dictionary of running tasks
        self.tasks = dict()

    def append_workflow_module(self, project_id, branch_id, command):
        """Append module to the workflow at the head of the given viztrail
        branch. The modified workflow will be executed. The result is the new
        head of the branch.

        Returns the handle for the new module in the modified workflow. The
        result is None if the specified project or branch do not exist.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id : string
            Unique branch identifier
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed by the appended
            workflow module

        Returns
        -------
        vizier.viztrail.module.base.ModuleHandle
        """
        with self.backend.lock:
            # Get the handle for the specified branch
            branch = self.projects.get_branch(project_id=project_id, branch_id=branch_id)
            if branch is None:
                return None
            # Get the current database state from the last module in the current
            # branch head. At the same time we retrieve the list of modules for
            # the current head of the branch.
            head = branch.get_head()
            if not head is None and len(head.modules) > 0:
                datasets = head.modules[-1].datasets
                modules = head.modules
                is_active = head.is_active
                is_error = head.modules[-1].is_error or head.modules[-1].is_canceled
            else:
                datasets = dict()
                modules = list()
                is_active = False
                is_error = False
            # Get the external representation for the command
            external_form = command.to_external_form(
                command=self.packages[command.package_id].get(command.command_id),
                datasets=datasets
            )
            # If the workflow is not active and the command can be executed
            # synchronously we run the command immediately and return the
            # completed workflow. Otherwise, a pending workflow is created.
            if not is_active and self.backend.can_execute(command):
                ts_start = get_current_time()
                result = self.backend.execute(
                    task=TaskHandle(
                        task_id=get_unique_identifier(),
                        project_id=project_id,
                        controller=self
                    ),
                    command=command,
                    context=task_context(datasets)
                )
                ts = ModuleTimestamp(
                    created_at=ts_start,
                    started_at=ts_start,
                    finished_at=get_current_time()
                )
                # Depending on the execution outcome create a handle for the
                # executed module
                if result.is_success:
                    module = ModuleHandle(
                        state=mstate.MODULE_SUCCESS,
                        command=command,
                        external_form=external_form,
                        timestamp=ts,
                        datasets=result.provenance.get_database_state(
                            modules[-1].datasets if len(modules) > 0 else dict()
                        ),
                        outputs=result.outputs,
                        provenance=result.provenance
                    )
                else:
                    module = ModuleHandle(
                        state=mstate.MODULE_ERROR,
                        command=command,
                        external_form=external_form,
                        timestamp=ts,
                        outputs=result.outputs
                    )
                workflow = branch.append_workflow(
                    modules=modules,
                    action=wf.ACTION_APPEND,
                    command=command,
                    pending_modules=[module]
                )
            else:
                # Create new workflow by appending one module to the current
                # head of the branch. The module state is pending if the
                # workflow is active otherwise it depends on the associated
                # backend.
                if is_active:
                    state = mstate.MODULE_PENDING
                elif is_error:
                    state = mstate.MODULE_CANCELED
                else:
                    state = self.backend.next_task_state()
                workflow = branch.append_workflow(
                    modules=modules,
                    action=wf.ACTION_APPEND,
                    command=command,
                    pending_modules=[
                        ModuleHandle(
                            state=state,
                            command=command,
                            external_form=external_form
                        )
                    ]
                )
                if not is_active and not state == mstate.MODULE_CANCELED:
                    self.execute_module(
                        project_id=project_id,
                        branch_id=branch_id,
                        module=workflow.modules[-1],
                        datasets=datasets
                    )
        return workflow.modules[-1]

    def cancel_exec(self, project_id, branch_id):
        """Cancel the execution of all active modules for the head workflow of
        the given branch. Sets the status of all active modules to canceled and
        sends terminate signal to running tasks. The finished_at property is
        set to the current time. The module outputs will be empty.

        Returns the handle for the modified workflow. The result is None if the
        specified project or branch do not exist. If the specified branch
        head is None ValueError is raised.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id : string
            Unique branch identifier

        Returns
        -------
        list(vizier.viztrail.module.base.ModuleHandle)
        """
        with self.backend.lock:
            # Get the handle for the head workflow of the specified branch.
            branch = self.projects.get_branch(project_id=project_id, branch_id=branch_id)
            if branch is None:
                return None
            workflow = branch.head
            if workflow is None:
                raise ValueError('empty workflow at branch head')
            # Set the state of all active modules to canceled
            first_active_module_index = None
            for i in range(len(workflow.modules)):
                module = workflow.modules[i]
                if module.is_active:
                    module.set_canceled()
                    if first_active_module_index is None:
                        first_active_module_index = i
            # Cancel all running tasks for the project branch
            for task_id in self.tasks.keys():
                task = self.tasks[task_id]
                if task.project_id == project_id and task.branch_id == branch_id:
                    self.backend.cancel_task(task_id)
                    del self.tasks[task_id]
            if not first_active_module_index is None:
                return workflow.modules[first_active_module_index:]
            else:
                return list()

    def delete_workflow_module(self, project_id, branch_id, module_id):
        """Delete the module with the given identifier from the workflow at the
        head of the viztrail branch. The resulting workflow is executed and will
        be the new head of the branch.

        Returns the list of remaining modules in the modified workflow that are
        affected by the deletion. The result is None if the specified project
        or branch do not exist. Raises ValueError if the current head of the
        branch is active.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        module_id : string
            Unique module identifier

        Returns
        -------
        list(vizier.viztrail.module.base.ModuleHandle)
        """
        with self.backend.lock:
            # Get the handle for the specified branch and the branch head
            branch = self.projects.get_branch(project_id=project_id, branch_id=branch_id)
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
            # Re-execute modules unless the last module was deleted or the
            # module list is empty
            module_count = len(modules)
            if module_count > 0 and module_index < module_count:
                # Get the context for the first module that requires
                #  re-execution
                if module_index > 0:
                    datasets = modules[module_index - 1].datasets
                else:
                    datasets = dict()
                # Keep track of the first remaining module that was affected
                # by the delete
                first_remaining_module = module_index
                while not modules[module_index].provenance.requires_exec(datasets):
                    if module_index == module_count - 1:
                        # Update the counter before we exit the loop. Otherwise
                        # the last module would be executed.
                        print 'No need to execute anything'
                        module_index = module_count
                        break
                    else:
                        m = modules[module_index]
                        datasets = m.provenance.get_database_state(datasets)
                        m.datasets = datasets
                        module_index += 1
                if module_index < module_count:
                    # The module that module_index points to has to be executed.
                    # Create a workflow that contains pending copies of all
                    # modules that require execution and run the first of these
                    # modules.
                    command = modules[module_index].command
                    external_form = command.to_external_form(
                        command=self.packages[command.package_id].get(command.command_id),
                        datasets=datasets
                    )
                    # Replace original modules with pending modules for those
                    # that need to be executed. The state of the first module
                    # depends on the state of the backend. All following modules
                    # will be in pending state.
                    pending_modules = [
                        ModuleHandle(
                            command=command,
                            state=self.backend.next_task_state(),
                            external_form=external_form,
                            provenance=modules[module_index].provenance
                        )
                    ]
                    for i in range(module_index + 1, module_count):
                        m = modules[i]
                        pending_modules.append(
                            ModuleHandle(
                                command=m.command,
                                external_form=m.external_form,
                                datasets=m.datasets,
                                outputs=m.outputs,
                                provenance=m.provenance
                            )
                        )
                    workflow = branch.append_workflow(
                        modules=modules[:module_index],
                        action=wf.ACTION_DELETE,
                        command=deleted_module.command,
                        pending_modules=pending_modules
                    )
                    self.execute_module(
                        project_id=project_id,
                        branch_id=branch_id,
                        module=workflow.modules[module_index],
                        datasets=datasets
                    )
                    return workflow.modules[first_remaining_module:]
                else:
                    # None of the module required execution and the workflow is
                    # complete
                    branch.append_workflow(
                        modules=modules,
                        action=wf.ACTION_DELETE,
                        command=deleted_module.command
                    )
            else:
                branch.append_workflow(
                    modules=modules,
                    action=wf.ACTION_DELETE,
                    command=deleted_module.command
                )
            return list()

    def execute_module(self, project_id, branch_id, module, datasets):
        """Create a new task for the given module and execute the module in
        asynchronous mode.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id: string
            Unique branch identifier
        module: vizier.viztrail.module.ModuleHandle
            handle for executed module
        datasets: dict(vizier.datastore.dataset.DatasetDescriptor)
            Index of datasets in the a database state
        """
        task = ExtendedTaskHandle(
            project_id=project_id,
            branch_id=branch_id,
            module_id=module.identifier,
            controller=self
        )
        self.tasks[task.task_id] = task
        self.backend.execute_async(
            task=task,
            command=module.command,
            context=task_context(datasets),
            resources=module.provenance.resources
        )

    def get_task_module(self, task):
        """Get the workflow and module index for the given task. Returns None
        and -1 if the workflow or module is undefined.

        Removes the tast from the internal task index.

        Parameters
        ----------
        task: vizier.engine.base.ExtendedtaskHandle
            Extended handle for task

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle, int
        """
        # Get the handle for the head workflow of the specified branch
        branch = self.projects.get_branch(
            project_id=task.project_id,
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

    def insert_workflow_module(self, project_id, branch_id, before_module_id, command):
        """Insert a new module to the workflow at the head of the given viztrail
        branch. The modified workflow will be executed. The result is the new
        head of the branch.

        The module is inserted into the sequence of workflow modules before the
        module with the identifier that is given as the before_module_id
        argument.

        Returns the list of affected modules in the modified workflow. The
        result is None if the specified project, branch or module do not exist.
        Raises ValueError if the current head of the branch is active.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id : string
            Unique branch identifier
        before_module_id : string
            Insert new module before module with given identifier.
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed by the inserted
            workflow module

        Returns
        -------
        list(vizier.viztrail.module.base.ModuleHandle)
        """
        with self.backend.lock:
            # Get the handle for the specified branch and the branch head
            branch = self.projects.get_branch(project_id=project_id, branch_id=branch_id)
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
            # Create handle for the inserted module. The state of the module
            # depends on the state of the backend.
            inserted_module = ModuleHandle(
                command=command,
                state=self.backend.next_task_state(),
                external_form=command.to_external_form(
                    command=self.packages[command.package_id].get(command.command_id),
                    datasets=datasets
                )
            )
            # Create list of pending modules for the new workflow.
            pending_modules = [inserted_module]
            for m in modules[module_index:]:
                pending_modules.append(
                    ModuleHandle(
                        command=m.command,
                        external_form=m.external_form,
                        datasets=m.datasets,
                        outputs=m.outputs,
                        provenance=m.provenance
                    )
                )
            workflow = branch.append_workflow(
                modules=modules[:module_index],
                action=wf.ACTION_INSERT,
                command=inserted_module.command,
                pending_modules=pending_modules
            )
            self.execute_module(
                project_id=project_id,
                branch_id=branch_id,
                module=workflow.modules[module_index],
                datasets=datasets
            )
            return workflow.modules[module_index:]

    def replace_workflow_module(self, project_id, branch_id, module_id, command):
        """Replace an existing module in the workflow at the head of the
        specified viztrail branch. The modified workflow is executed and the
        result is the new head of the branch.

        Returns the list of affected modules in the modified workflow. The
        result is None if the specified project or branch do not exist. Raises
        ValueError if the current head of the branch is active.

        Parameters
        ----------
        project_id: string
            Unique project identifier
        branch_id : string, optional
            Unique branch identifier
        module_id : string
            Identifier of the module that is being replaced
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be evaluated

        Returns
        -------
        list(vizier.viztrail.module.base.ModuleHandle)
        """
        with self.backend.lock:
            # Get the handle for the specified branch and the branch head
            branch = self.projects.get_branch(project_id=project_id, branch_id=branch_id)
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
            # Get handle for the replaced module. Keep any resource information
            # from the provenance object of the previous module execution. The
            # state of the module depends on the state of the backend.
            if module_index > 0:
                datasets = modules[module_index - 1].datasets
            else:
                datasets = dict()
            replaced_module = ModuleHandle(
                command=command,
                state=self.backend.next_task_state(),
                external_form=command.to_external_form(
                    command=self.packages[command.package_id].get(command.command_id),
                    datasets=datasets
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
                        external_form=m.external_form,
                        datasets=m.datasets,
                        outputs=m.outputs,
                        provenance=m.provenance
                    )
                )
            workflow = branch.append_workflow(
                modules=modules[:module_index],
                action=wf.ACTION_REPLACE,
                command=replaced_module.command,
                pending_modules=pending_modules
            )
            self.execute_module(
                project_id=project_id,
                branch_id=branch_id,
                module=workflow.modules[module_index],
                datasets=datasets
            )
            return workflow.modules[module_index:]

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
        with self.backend.lock:
            # Get task handle and remove it from the internal index. The result
            # is None if the task does not exist.
            task = pop_task(tasks=self.tasks, task_id=task_id)
            if task is None:
                return None
            # Get the handle for the head workflow of the specified branch and
            # the index for the module matching the identifier in the task.
            workflow, module_index = self.get_task_module(task)
            if workflow is None or module_index == -1:
                return None
            # Notify the backend that the task is finished
            self.backend.task_finished(task_id)
            module = workflow.modules[module_index]
            if module.is_active:
                module.set_error(finished_at=finished_at, outputs=outputs)
                for m in workflow.modules[module_index+1:]:
                    m.set_canceled()
                return True
            else:
                return False

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
        with self.backend.lock:
            # Get task handle and remove it from the internal index. The result
            # is None if the task does not exist.
            # Check if a task with the given identifier exists
            if not task_id in self.tasks:
                return None
            task = self.tasks[task_id]
            # Get the handle for the head workflow of the specified branch and
            # the index for the module matching the identifier in the task.
            workflow, module_index = self.get_task_module(task)
            if workflow is None or module_index == -1:
                return None
            module = workflow.modules[module_index]
            if module.is_pending:
                module.set_running(
                    started_at=started_at
                )
                return True
            else:
                return False

    def set_success(self, task_id, finished_at=None, outputs=None, provenance=None):
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
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.

        Returns
        -------
        bool
        """
        with self.backend.lock:
            # Get task handle and remove it from the internal index. The result
            # is None if the task does not exist.
            task = pop_task(tasks=self.tasks, task_id=task_id)
            if task is None:
                return None
            # Get the handle for the head workflow of the specified branch and
            # the index for the module matching the identifier in the task.
            workflow, module_index = self.get_task_module(task)
            if workflow is None or module_index == -1:
                return None
            # Notify the backend that the task is finished
            self.backend.task_finished(task_id)
            module = workflow.modules[module_index]
            if not module.is_running:
                # The result is false if the state of the module did not change
                return False
            # Adjust the module context
            datasets = workflow.modules[module_index - 1].datasets if module_index > 0 else dict()
            context = provenance.get_database_state(datasets)
            module.set_success(
                finished_at=finished_at,
                datasets=context,
                outputs=outputs,
                provenance=provenance
            )
            for next_module in workflow.modules[module_index+1:]:
                if not next_module.is_pending:
                    # This case can only happen if we allow parallel execution
                    # of modules in the future. At this point it should not
                    # occur.
                    raise RuntimeError('invalid workflow state')
                elif not next_module.provenance.requires_exec(context):
                    context = next_module.provenance.get_database_state(context)
                    next_module.set_success(
                        finished_at=finished_at,
                        datasets=context,
                        outputs=next_module.outputs,
                        provenance=next_module.provenance
                    )
                else:
                    command = next_module.command
                    package_id = command.package_id
                    command_id = command.command_id
                    external_form = command.to_external_form(
                        command=self.packages[package_id].get(command_id),
                        datasets=context
                    )
                    # If the backend is going to run the task immediately we
                    # need to adjust the module state
                    state = self.backend.next_task_state()
                    if state == mstate.MODULE_RUNNING:
                        next_module.set_running(
                            external_form=external_form,
                            started_at=get_current_time()
                        )
                    else:
                        next_module.update_property(
                            external_form=external_form
                        )
                    self.execute_module(
                        project_id=task.project_id,
                        branch_id=workflow.branch_id,
                        module=next_module,
                        datasets=context
                    )
                    break
            return True


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def pop_task(tasks, task_id):
    """Remove task with given identifier and return the task handle. The result
    is None if no task with the given identifier exists.

    Parameters
    ----------
    tasks: dict(vizier.engine.base.ExtendedTaskHandle)
        Dictionary of task handles
    task_id: string
        Unique task identifier

    Returns
    -------
    vizier.engine.base.ExtendedTaskHandle
    """
    # Check if a task with the given identifier exists
    if not task_id in tasks:
        return None
    # Get the task handle and remove it from the task index
    task = tasks[task_id]
    del tasks[task_id]
    return task


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
