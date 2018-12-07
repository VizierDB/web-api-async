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

from vizier.engine.backen.based import VizierBackend
from vizier.engine.task.base import TaskContext
from vizier.viztrail.module import MODULE_RUNNING


class MultiProcessBackend(VizierBackend):
    """The multi-process backend maintains references to the datastore and the
    filestore that are associated with a project. It contains an index of task
    processors.
    """
    def __init__(self, datastore, filestore, processors):
        """
        """
        self.datastore = datastore
        self.filestore = filestore
        self.processors = processors

    @abstractmethod
    def cancel_task(self, task):
        """Request to cancel execution of the given task.

        Parameters
        ----------
        task: vizier.engine.task.base.TaskHandle
            Handle for task for which execution is requested to be canceled
            controlling workflow engine
        """
        raise NotImplementedError

    def execute_task(self, task, command, context, resources=None):
        """Request execution of a given task. The task handle is used to
        identify the task when interacting with the API. The executed task
        itself is defined by the given command specification. The given context
        contains the names and identifier of resources in the database state
        against which the task is executed.

        The multi-process backend first ensures that if has a processor for the
        package of the given command. If True, the package-specific processor
        will be used to run the command in a separate process.

        Raises ValueError if no processor for the command package exists.

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
        processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets=context,
                resources=resources
            )
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
