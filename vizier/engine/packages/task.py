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

"""The package task module defines the base classes for executing commands,
i.e., tasks, that are supported by a given package. Every package declaration
is accompanied with a package-specific task engine that will be used to execute
commands that are declared in the package as part of a vizier workflow.
"""

from abc import abstractmethod


class ExecResult(object):
    """Wrapper containing results for executed task. Contains the output
    streams, the new database state, and provenance information.

    Attributes
    ----------
    datasets : dict(vizier.datastore.dataset.DatasetDescriptor)
        Dictionary for the new database state. The user-specified name is the
        key for the dataset descriptors. If the state of the result is an error
        the database state might be empty.
    is_error: bool
        Flag indicating if the execution resulted in an error
    is_success: bool
        True if not in error, i.e., execution was succcessful
    outputs: vizier.viztrail.module.ModuleOutputs
        Output streams for STDOUT and STDERR
    provenance: vizier.viztrail.module.ModuleProvenance
        Provenance information about datasets that were read and writen during
        task execution.
    task: vizier.engine.packages.task.TaskHandle
        Handle for the executed task
    """
    def __init__(self, task, is_success=True, datasets=None, outputs=None, provenance=None):
        """Initialize the result components.

        Parameters
        ----------
        task: vizier.engine.packages.task.TaskHandle
            Handle for the executed task
        is_success: bool
            Flag indicating if execution was successful
        datasets : dict(vizier.datastore.dataset.DatasetDescriptor), optional
            Dictionary of resulting database state.
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Outputs to STDOUT and STDERR generated during task execution
        provenance: vizier.viztrail.module.ModuleProvenance, optional
            Provenance information about datasets that were read and writen
            during task execution.
        """
        self.task = task
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


class TaskHandle(object):
    """A task is uniquely identified by the combination of viztrail, branch,
    and module identifier. With each handle we maintain the external form of
    the command.
    """
    def __init__(self, viztrail_id, branch_id, module_id, external_form):
        """Initialize the components of the task handle.

        Parameters
        ----------
        viztrail_id: string
            Unique viztrail identifier
        branch_id: string
            Unique branch identifier
        module_id: string
            Unique module identifier
        external_form: string
            String containing a representation of the associated command in
            human-readable form
        """
        self.viztrail_id = viztrail_id
        self.branch_id = branch_id
        self.module_id = module_id
        self.external_form = external_form


class TaskEngine(object):
    """The task engine is used to execute commands in packages. For each package
    there has to exists an instance of the task engine class that is able to
    excute each of the commands that are declared in the package.

    The task engine has a single compute method that takes the command
    identifier, user specified command arguments and information about the
    current database state as input. This method is called by the execution
    backend during workflow execution.
    """
    @abstractmethod
    def compute(self, command_id, arguments, context):
        """Compute results for a given package command using the set of user-
        provided arguments and the current database state. Return an execution
        result is case of success or error.

        Parameters
        ----------
        command_id: string
            Unique identifier for a command in a package declaration
        arguments: vizier.viztrail.command.ModuleArguments
            User-provided command arguments
        context: dict(vizier.datastore.dataset.DatasetDescriptors)
            Descriptors for datasets in the database state agains which the
            command is executed (keyed by user-provided name)

        Returns
        -------
        vizier.engine.packages.task.ExecResult
        """
        raise NotImplementedError
