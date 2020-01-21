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

"""
Implementation of the task processor that executes commands that
are defined in the `sampling` package.
"""

from vizier.engine.task.processor import ExecResult, TaskProcessor
import vizier.engine.packages.sample.base as cmd


class SamplingProcessor(TaskProcessor):
    """
    Implmentation of the task processor for the sampling package. The 
    processor uses an instance of the vizual API to allow running on 
    different types of datastores (e.g., the default datastore or the 
    Mimir datastore).
    """
    def __init__(self):
        """
        Initialize the vizual API instance
        """

    def compute(self, command_id, arguments, context):
        """Compute results for commands in the sampling package using 
        the set of user-provided arguments and the current database 
        state.

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


        if command_id == cmd.BASIC_SAMPLE:
            return self.basic_sample(arguments, context)

    def basic_sample(self, arguments, context):
        """Generate a dataset sample in the specified context.

        Parameters
        ----------
        args: vizier.viztrail.command.ModuleArguments
            User-provided command arguments
        context: vizier.engine.task.base.TaskContext
            Context in which a task is being executed

        Returns
        -------
        vizier.engine.task.processor.ExecResult
        """

        outputs = ModuleOutputs()

        input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()
        input_dataset = context.get_dataset(input_ds_name)

        output_ds_name = arguments.get_value(cmd.PARA_OUTPUT_DATASET, raise_error=False)
        if output_ds_name == None:
            output_ds_name = input_ds_name("SAMPLE")

        sampling_rate = float(arguments.get_value(cmd.PARA_SAMPLING_RATE))

        





        # Return task result
        return ExecResult(
            outputs=outputs,
            provenance=ModuleProvenance(
                read={ds_name: dataset.identifier},
                write={ds_name: dsd},
                resources=result_resources
            )
        )