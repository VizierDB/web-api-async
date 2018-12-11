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

"""Default multi-process backend to execute vizier workflow tasks. The default
backend starts a separate process for each task that is being executed. This
backend is primarily intended for local installations of vizier with a single
user or for installations where each project is running in a separate container
or virtual environment.
"""

from functools import partial
from multiprocessing import Lock, Pool

from vizier.core.timestamp import get_current_time
from vizier.engine.backend.synchron import SynchronousBackend
from vizier.engine.task.base import TaskContext
from vizier.engine.task.processor import ExecResult
from vizier.viztrail.module.base import MODULE_RUNNING
from vizier.viztrail.module.output import ModuleOutputs, TextOutput


class MultiProcessBackend(SynchronousBackend):
    """The multi-process backend executes every task in a separate process.
    Each task is executed by a single-process pool.
    """
    def __init__(self, datastore, filestore, processors, synchron_commands=None):
        """
        """
        super(MultiProcessBackend, self).__init__(
            datastore=datastore,
            filestore=filestore,
            commands=synchron_commands if not synchron_commands is None else dict(),
            lock=Lock()
        )
        self.processors = processors
        # Maintain tasks that are currently being executed and the associated
        # multiplressing pool. Values are tuples of (task handle, pool) and
        # the index is the unique task identifier. This dictionary is used to
        # cancel tasks and to update the controller when task execution
        # is complete.
        self.tasks = dict()

    def cancel_task(self, task_id):
        """Request to cancel execution of the given task.

        Parameters
        ----------
        task_id: string
            Unique task identifier
        """
        # Catch errors in case the task has already been removed
        try:
            task, pool = self.tasks[task_id]
            # Close the pool and terminate any running processes
            pool.close()
            pool.terminate()
            del self.tasks[task_id]
        except KeyError:
            pass

    def execute_async(self, task, command, context, resources=None):
        """Request execution of a given task. The task handle is used to
        identify the task when interacting with the API. The executed task
        itself is defined by the given command specification. The given context
        contains the names and identifier of resources in the database state
        against which the task is executed.

        The multi-process backend first ensures that if has a processor for the
        package of the given command. If True, the package-specific processor
        will be used to run the command in a separate process.

        Parameters
        ----------
        task: vizier.engine.task.base.TaskHandle
            Handle for task for which execution is requested by the controlling
            workflow engine
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed
        context: dict()
            Dictionary of available resource in the database state. The key is
            the resource name. Values are resource identifiers.
        resources: dict(), optional
            Optional information about resources that were generated during a
            previous execution of the command
        """
        # Ensure there is a processor for the package that contains the command
        if not command.package_id in self.processors:
            raise ValueError('unknown package \'' + str(command.package_id) + '\'')
        processor = self.processors[command.package_id]
        # Create a pool with a single process to execute the task. Maintain
        # pair of task handle and pool in the internal task index.
        pool = Pool(processes=1)
        self.tasks[task.task_id] = (task, pool)
        # Create a callback function that is called when the pool finishes
        # execution. Use partial to create a function that receives the
        # internal task index as parameter so we can remove the finished task
        # from the dictionary
        task_callback_function = partial(callback_function, tasks=self.tasks)
        # Execute task using wiorger function
        pool.apply_async(
            worker,
            args=(
                task.task_id,
                command,
                TaskContext(
                    datastore=self.datastore,
                    filestore=self.filestore,
                    datasets=context,
                    resources=resources
                ),
                processor,
            ),
            callback=task_callback_function
        )

    def next_task_state(self):
        """Get the module state of the next task that will be submitted for
        execution.

        For the multi-process backend a process will start running immediately
        in a separate process.

        Returns
        -------
        int
        """
        return MODULE_RUNNING


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def callback_function(result, tasks):
    """Callback function for executed tasks. Notifies the workflow controller
    and removes the task from the task index.

    Parameters
    ----------
    result: (string, vizier.engine.task.processor.ExecResult)
        Tupe of task identifier and execution result
    tasks: dict()
        Task index of the backend
    """
    task_id, exec_result = result
    try:
        task, pool = tasks[task_id]
        # Close the pool and remove the entry from the task index
        pool.close()
        del tasks[task_id]
        # Notify the workflow controller that the task is finished
        if exec_result.is_success:
            task.controller.set_success(
                task_id=task_id,
                datasets=exec_result.datasets,
                outputs=exec_result.outputs,
                provenance=exec_result.provenance
            )
        else:
            task.controller.set_error(
                task_id=task_id,
                outputs=exec_result.outputs
            )
    except KeyError:
        pass


def worker(task_id, command, context, processor):
    """The worker function executes a given task using a package task processor.
    Returns a pair of task identifier and execution result.

    Parameters
    ----------
    task_id: string
        Unique task identifier
    command : vizier.viztrail.command.ModuleCommand
        Specification of the command that is to be executed
    context: vizier.engine.task.base.TaskContext
        Context for the executed task
    processor: vizier.engine.task.processor.TaskProcessor
        Task processor to execute the given command

    Returns
    -------
    (string, vizier.engine.task.processor.ExecResult)
    """
    try:
        result = processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=context
        )
    except Exception as ex:
        template = "{0}:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        result = ExecResult(
            is_success=False,
            outputs=ModuleOutputs(stderr=[TextOutput(message)])
        )
    return task_id, result
