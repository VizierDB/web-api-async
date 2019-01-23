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

"""Backend for synchronized command execution. This class is promarily intended
for test purposes. It can only be used to append a new module to a workflow
but not to insert delete or replace modules in an existing workflow.

The synchronized backend can also be used as a base class for the asynchronous
backend.
"""

from abc import abstractmethod

from vizier.engine.backend.base import TaskExecEngine
from vizier.engine.task.base import TaskContext
from vizier.engine.task.processor import ExecResult
from vizier.viztrail.module.output import ModuleOutputs, TextOutput


class SynchronousTaskEngine(TaskExecEngine):
    """Backend that only supports synchronous execution of tasks."""
    def __init__(self, commands, projects):
        """Initialize the commands dictionary that contains the task engines
        for all commands that can be executed. This is a dictionary of
        dictionaries that are keyed by the package identifier. Each internal
        dictionary contains the task engines keyed by the command identifier.

        Parameters
        ----------
        commands: dict(dict(vizier.engine.packages.task.processor.TaskProcessor))
            Dictionary of task processors for executable tasks that are keyed
            by the pakage identifier and the command identifier
        projects: vizier.engine.project.cache.base.ProjectCache
            Cache for project handles
        """
        self.commands = commands
        self.projects = projects

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
        pckg = command.package_id
        cmd = command.command_id
        return pckg in self.commands and cmd in self.commands[pckg]

    def execute(self, task, command, context, resources=None):
        """Execute a given command. The command will be executed immediately if
        the backend supports synchronous excution, i.e., if the .can_excute()
        method returns True. The result is the execution result returned by the
        respective package task processor.

        Raises ValueError if the given command cannot be excuted in synchronous
        mode.

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

        Returns
        ------
        vizier.engine.task.processor.ExecResult
        """
        if command.package_id in self.commands:
            package = self.commands[command.package_id]
            if command.command_id in package:
                processor = package[command.command_id]
                # Get the project handle from the cache
                project = self.projects.get_project(task.project_id)
                try:
                    return processor.compute(
                        command_id=command.command_id,
                        arguments=command.arguments,
                        context=TaskContext(
                            datastore=project.datastore,
                            filestore=project.filestore,
                            datasets=context,
                            resources=resources
                        )
                    )
                except Exception as ex:
                    template = "{0}:{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    return ExecResult(
                        is_success=False,
                        outputs=ModuleOutputs(stderr=[TextOutput(message)])
                    )
        raise ValueError('cannot execute given command')
