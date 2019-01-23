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

from vizier.engine.backend.remote.celery.app import celeryapp
from vizier.engine.backend.base import VizierBackend
from vizier.viztrail.module.base import MODULE_PENDING
from vizier.engine.backend.remote.celery.worker import execute


class CeleryBackend(VizierBackend):
    """The backend interface defines two main methods: execute and cancel. The
    first method is used by the API to request execution of a command that
    defines a module in a data curation workflow. The second method is used
    by the API to cancel execution (on user request).

    Each backend should provide an implementation-specific lock. The lock is
    used by the workflow controller to serialize execution for those parts of
    the code that are used by the controller and the backend.

    If tasks are executed remotely the lock is a dummy lock. Only for
    multi-process execution the lock shoulc be the default multi-process-lock.
    """
    def __init__(self, routes=None, synchronous=None):
        """

        Parameters
        ----------
        routes: dict, optional
            Mapping of package commands to queue names
        synchronous: vizier.engine.backend.base.TaskExecEngine, optional
            Engine for synchronous task execution
        """
        # Initialize the synchronous command execution engine
        super(CeleryBackend, self).__init__(synchronous=synchronous)
        self.routes = routes
        # Keep dictionary of celery tasks in order to be able to cancel a task
        self.tasks = dict()

    def cancel_task(self, task_id):
        """Request to cancel execution of the given task.

        Parameters
        ----------
        task_id: string
            Unique task identifier
        """
        if task_id in self.tasks:
            async_task = self.tasks[task_id]
            del self.tasks[task_id]
            celeryapp.control.revoke(task_id=async_task.id, terminate=True)

    def execute_async(self, task, command, context, resources=None):
        """Request execution of a given task. The task handle is used to
        identify the task when interacting with the API. The executed task
        itself is defined by the given command specification. The given context
        contains the names and identifier of resources in the database state
        against which the task is executed.

        Parameters
        ----------
        task: vizier.engine.task.base.TaskHandle
            Handle for task for which execution is requested by the controlling
            workflow engine
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed
        context: dict
            Dictionary of available resource in the database state. The key is
            the resource name. Values are resource identifiers.
        resources: dict, optional
            Optional information about resources that were generated during a
            previous execution of the command
        """
        queue = None
        if not self.routes is None and command.package_id in self.routes:
            pckg_routes = self.routes[command.package_id]
            if command.command_id in pckg_routes:
                queue = pckg_routes[command.command_id]
        if not queue is None:
            async_task = execute.apply_async(
                kwargs=dict(
                    task_id=task.task_id,
                    project_id=task.project_id,
                    command_doc=command.to_dict(),
                    context=context,
                    resources=resources
                ),
                queue=queue
            )
        else:
            async_task = execute.apply_async(
                kwargs=dict(
                    task_id=task.task_id,
                    project_id=task.project_id,
                    command_doc=command.to_dict(),
                    context=context,
                    resources=resources
                )
            )
        self.tasks[task.task_id] = async_task

    def next_task_state(self):
        """Get the module state of the next task that will be submitted for
        execution.

        For the celery backend a process will be in pending mode until a remote
        worker starts to execute it.

        Returns
        -------
        int
        """
        return MODULE_PENDING

    def task_finished(self, task_id):
        """The celery backend keeps track of the identifier for asynchronous
        tasks. Delete the respective entry if a task is done.

        Parameters
        ----------
        task_id: string
            Unique task id
        """
        if task_id in self.tasks:
            del self.tasks[task_id]
