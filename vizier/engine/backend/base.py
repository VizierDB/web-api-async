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

"""Interface for the vizier backend that executes workflow modules. Different
implementations for the backend are possible.
"""

from abc import abstractmethod

from vizier.engine.task.processor import ExecResult
from vizier.viztrail.module.output import ModuleOutputs, TextOutput


class NonLock(object):
    """Dummy implementation of __enter__ and __exit__ methods for backends that
    do not require a lock.
    """
    def __enter__(self):
        pass

    def __exit__(self, type, value, tb):
        pass


class VizierBackend(object):
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
    def __init__(self, lock=None):
        """Initialize the implementation-specific lock. Use a dummy lock if
        the subclass does not provide a lock.

        Parameters
        ----------
        lock: class
            Class that implements __enter__ and __exit__ methods
        """
        self.lock = lock if not lock is None else NonLock()

    @abstractmethod
    def can_execute(self, command):
        """Test whether a given command can be executed in synchronous mode. If
        the result is True the command can be executed in the same process as
        the calling method.

        Parameters
        ----------
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_task(self, task_id):
        """Request to cancel execution of the given task.

        Parameters
        ----------
        task_id: string
            Unique task identifier
        """
        raise NotImplementedError

    @abstractmethod
    def execute(self, command, context, resources=None):
        """Execute a given command. The command will be executed immediately if
        the backend supports synchronous excution, i.e., if the .can_excute()
        method returns True. The result is the execution result returned by the
        respective package task processor.

        Raises ValueError if the given command cannot be excuted in synchronous
        mode.

        Parameters
        ----------
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed
        context: dict
            Dictionary of available resource in the database state. The key is
            the resource name. Values are resource identifiers.
        resources: dict, optional
            Optional information about resources that were generated during a
            previous execution of the command

        Returns
        ------
        vizier.engine.task.processor.ExecResult
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def next_task_state(self):
        """Get the module state of the next task that will be submitted for
        execution. This method allows the workflow controller to set the initial
        state of a module before submitting it for execution.

        Returns
        -------
        int
        """
        raise NotImplementedError


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------


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
