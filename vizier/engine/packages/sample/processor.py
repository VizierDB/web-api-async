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
from vizier.api.webservice import server
from vizier.viztrail.module.output import ModuleOutputs, DatasetOutput
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.datastore.dataset import DatasetDescriptor
import vizier.engine.packages.sample.base as cmd
import vizier.mimir as mimir


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


        input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()
        input_dataset = context.get_dataset(input_ds_name)
        if input_dataset is None:
            raise ValueError('unknown dataset \'' + input_ds_name + '\'')

        output_ds_name = arguments.get_value(cmd.PARA_OUTPUT_DATASET, raise_error=False)
        if output_ds_name == None or output_ds_name == "":
            output_ds_name = input_ds_name + "_SAMPLE"
        output_ds_name = output_ds_name.lower()

        sampling_rate = float(arguments.get_value(cmd.PARA_SAMPLING_RATE))
        if sampling_rate > 1.0 or sampling_rate < 0.0:
            raise Exception("Sampling rate must be between 0.0 and 1.0")

        print("input_ds_name: {}".format(input_ds_name))
        print("input_dataset.table_name: {}".format(input_dataset.table_name))
        print("output_ds_name: {}".format(output_ds_name))
        print("sampling_rate: {}".format(sampling_rate))
        
        sample_view_id = mimir.createSample(
            input_dataset.table_name,
            {
                "mode" : "uniform_probability",
                "probability" : sampling_rate
            }
        )
        row_count = mimir.countRows(sample_view_id)
        
        ## Register the view with Vizier
        ds = context.datastore.register_dataset(
            table_name=sample_view_id,
            columns=input_dataset.columns,
            row_counter=row_count
        )

        ## And start rendering some output
        outputs = ModuleOutputs()
        ds_output = server.api.datasets.get_dataset(
            project_id=context.project_id,
            dataset_id=ds.identifier,
            offset=0,
            limit=10
        )
        ds_output['name'] = output_ds_name
        outputs.stdout.append(DatasetOutput(ds_output))

        ## Record Reads and writes
        provenance = ModuleProvenance(
            read={
                input_ds_name: input_dataset.identifier
            },
            write={
                output_ds_name: DatasetDescriptor(
                    identifier=ds.identifier,
                    columns=ds.columns,
                    row_count=ds.row_count
                )
            }
        )

        # Return task result
        return ExecResult(
            outputs=outputs,
            provenance=provenance
        )