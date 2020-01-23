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
import math


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

        input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()
        input_dataset = context.get_dataset(input_ds_name)
        if input_dataset is None:
            raise ValueError('unknown dataset \'' + input_ds_name + '\'')

        output_ds_name = arguments.get_value(cmd.PARA_OUTPUT_DATASET, raise_error=False)
        if output_ds_name == None or output_ds_name == "":
            output_ds_name = input_ds_name + "_SAMPLE"
        output_ds_name = output_ds_name.lower()

        ## Load the sampling configuration
        sample_mode = None

        if command_id == cmd.BASIC_SAMPLE:
            sampling_rate = float(arguments.get_value(cmd.PARA_SAMPLING_RATE))
            if sampling_rate > 1.0 or sampling_rate < 0.0:
                raise Exception("Sampling rate must be between 0.0 and 1.0")
            sample_mode = {
                "mode" : cmd.SAMPLING_MODE_UNIFORM_PROBABILITY,
                "probability" : sampling_rate
            }
        elif command_id == cmd.MANUAL_STRATIFIED_SAMPLE or command_id == cmd.AUTOMATIC_STRATIFIED_SAMPLE:
            column = arguments.get_value(cmd.PARA_STRATIFICATION_COLUMN)
            column_defn = input_dataset.columns[column]
            if command_id == cmd.MANUAL_STRATIFIED_SAMPLE:
                strata = [
                    {
                        "value" : stratum.get_value(cmd.PARA_STRATUM_VALUE),
                        "probability" : stratum.get_value(cmd.PARA_SAMPLING_RATE)
                    } 
                    for stratum in arguments.get_value(cmd.PARA_STRATA)
                ]
            else:
                probability = arguments.get_value(cmd.PARA_SAMPLING_RATE)
                strata = self.get_automatic_strata(input_dataset, column_defn, probability)
            sample_mode = {
                "mode" : cmd.SAMPLING_MODE_STRATIFIED_ON,
                "column" : column_defn.name,
                "type" : column_defn.data_type,
                "strata" : strata
            }
        else:
            raise Exception("Unknown sampling command: {}".format(command_id))

        sample_view_id = mimir.createSample(
            input_dataset.table_name,
            sample_mode
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

    def get_automatic_strata(self, dataset, column, probability):
        counts = mimir.vistrailsQueryMimirJson(
            query = "SELECT `{}`, COUNT(*) FROM `{}` GROUP BY `{}`".format(
                    column.name, 
                    dataset.table_name,
                    column.name
                ),
            include_uncertainty = False,
            include_reasons = False,
        )
        data = counts["data"]
        total = sum([ stratum[1] for stratum in data ])
        sample_bin_goal = total * probability / len(data)
        imbalanced_strata = [
            stratum
            for stratum in data
            if stratum[1] < sample_bin_goal
        ]
        if len(imbalanced_strata) > 0:
            minimum_safe_rate = min(float(stratum[1]) / total for stratum in data)
            raise Exception("Sampling rate too high (maximum safe rate = {}).  Too few records for the following values: {}".format(
                minimum_safe_rate,
                ", ".join(stratum[0] for stratum in imbalanced_strata)    
            ))

        stratum_bins = [
            {
                "value" : stratum[0],
                "probability" : float(sample_bin_goal) / float(stratum[1])
            }
            for stratum in data
        ]
        return stratum_bins
