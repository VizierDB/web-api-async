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

"""The processor module defines the base classes for executing commands and the
results of task execution. Every package declaration is accompanied with a
package-specific task processor that is used to execute the commands that are
declared in the package in a vizier workflow.
"""

from abc import abstractmethod


class ExecResult(object):
    """Wrapper containing results for executed task. Contains the output
    streams, the new database state, and provenance information.

    Attributes
    ----------
    datasets : dict
        Mapping of dataset names to unique identifier for datasets in the
        resulting database state
    is_error: bool
        Flag indicating if the execution resulted in an error
    is_success: bool
        True if not in error, i.e., execution was succcessful
    outputs: vizier.viztrail.module.output.ModuleOutputs
        Output streams for STDOUT and STDERR
    provenance: vizier.viztrail.module.provenance.ModuleProvenance
        Provenance information about datasets that were read and writen during
        task execution.
    """
    def __init__(
        self, is_success=True, datasets=None, outputs=None, provenance=None
    ):
        """Initialize the result components.

        Parameters
        ----------
        is_success: bool
            Flag indicating if execution was successful
        datasets : dict, optional
            Mapping of dataset names to unique identifier for datasets in the
            resulting database state
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Outputs to STDOUT and STDERR generated during task execution
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen
            during task execution.
        """
        self.is_success = is_success
        self.datasets = datasets
        self.outputs = outputs
        self.provenance = provenance

    @property
    def is_error(self):
        """Flag indicating that task execution generated errors and did not
        complete successfully.

        Returns
        -------
        bool
        """
        return not self.is_success


class TaskProcessor(object):
    """The task processor is used to execute commands in a package. For each
    package there has to exists an instance of the task processor class that is
    capable to excute each of the declared package commands.

    The task processor has a single compute method that takes the command
    identifier, user specified command arguments and information about the
    current database state as input. This method is called by the execution
    backend during workflow execution.
    """
    @abstractmethod
    def compute(self, command_id, arguments, context):
        """Compute results for a given package command using the set of user-
        provided arguments and the current database state.

        Parameters
        ----------
        command_id: string
            Unique identifier for a command in a package declaration
        arguments: vizier.viztrail.command.ModuleArguments
            User-provided command arguments
        context: vizier.engine.task.base.TaskContext
            Context in which a task is being executed

        Returns
        -------
        vizier.engine.task.processor.ExecResult
        """
        raise NotImplementedError
