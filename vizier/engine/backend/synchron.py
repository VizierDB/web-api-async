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

"""Backend for synchronized command execution. This class is promarily intended
for test purposes. It can only be used to append a new module to a workflow
but not to insert delete or replace modules in an existing workflow.

The synchronized backend can also be used as a base class for the asynchronous
backend.
"""

from vizier.engine.backend.base import VizierBackend
from vizier.engine.task.base import TaskContext


class SynchronousBackend(VizierBackend):
    """Backend that only supports synchronous execution of tasks. All methods
    that are related to asynchronous task execution remain uninplemented.
    """
    def __init__(self, datastore, filestore, commands):
        """Initialize the datastore and filestore. The commands dictionary
        contains the task engines for all commands that can be executed. This
        is a dictionary of dictionaries that are keyed by the package
        identifier. Each internal idictionary contains the task engines keyed
        by the command identifier.

        Parameters
        ----------
        datastore: vizier.datastore.base.Datastore
            Datastore for the project that execute the task
        filestore: vizier.filestore.Filestore
            Filestore for the project that executes the task
        commands: dict(dict(vizier.engine.packages.task.processor.TaskProcessor))
            Dictionary of task processors for executable tasks that are keyed
            by the pakage identifier and the command identifier
        """
        self.datastore = datastore
        self.filestore = filestore
        self.commands = commands

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
        context: dict()
            Dictionary of available resource in the database state. The key is
            the resource name. Values are resource identifiers.
        resources: dict(), optional
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
                return processor.compute(
                    command_id=command.command_id,
                    arguments=command.arguments,
                    context=TaskContext(
                        datastore=self.datastore,
                        filestore=self.filestore,
                        datasets=context,
                        resources=resources
                    )
                )
        raise ValueError('cannot execute given command')
