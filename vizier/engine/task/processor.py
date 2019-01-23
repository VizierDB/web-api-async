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

"""The processor module defines the base classes for executing commands and the
results of task execution. Every package declaration is accompanied with a
package-specific task processor that is used to execute the commands that are
declared in the package in a vizier workflow.
"""

import os

from abc import abstractmethod

from vizier.core.io.base import read_object_from_file
from vizier.core.loader import ClassLoader
from vizier.viztrail.module.output import ModuleOutputs
from vizier.viztrail.module.provenance import ModuleProvenance


class ExecResult(object):
    """Wrapper containing results for executed task. Contains the output
    streams, and provenance information.

    Attributes
    ----------
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
    def __init__(self, is_success=True, outputs=None, provenance=None):
        """Initialize the result components.

        Parameters
        ----------
        is_success: bool
            Flag indicating if execution was successful
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Outputs to STDOUT and STDERR generated during task execution
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen
            during task execution.
        """
        self.is_success = is_success
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.provenance = provenance if not provenance is None else ModuleProvenance()

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


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def load_processors(path):
    """Load task processors for packages from directories in the given
    path. The path may contain multiple directories separated by ':'. The
    directories in the path are processed in reverse order to ensure that
    loaded processors are not overwritten by files that occur in directories
    later in the path.

    The format of individual files is expected to be as follows:
    {
      - packages: [string]
      - engine: {class loader definition}
    }

    Returns
    -------
    dict(vizier.engine.packages.task.processor.TaskProcessor)
    """
    processors = dict()
    for dir_name in path.split(':')[::-1]:
        for filename in os.listdir(dir_name):
            filename = os.path.join(dir_name, filename)
            if os.path.isfile(filename):
                obj = read_object_from_file(filename)
                # Ignore files that do not contain the mandatory elements
                for key in ['engine', 'package']:
                    if not key in obj:
                        continue
                engine = ClassLoader(values=obj['engine']).get_instance()
                for key in obj['packages']:
                    processors[key] = engine
    return processors
