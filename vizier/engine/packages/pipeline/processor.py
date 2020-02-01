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
are defined in the `pipeline` package.
"""

from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.api.webservice import server
from vizier.viztrail.module.output import ModuleOutputs, DatasetOutput, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.datastore.dataset import DatasetDescriptor, DatasetColumn, DatasetRow
import vizier.engine.packages.pipeline.base as cmd
import vizier.mimir as mimir
import math


class PipelineProcessor(TaskProcessor):
    """
    Implmentation of the task processor for the pipeline package. The 
    processor uses an instance of the vizual API to allow running on 
    different types of datastores (e.g., the default datastore or the 
    Mimir datastore).
    """
    def __init__(self):
        """
        Initialize the vizual API instance
        """

    def compute(self, command_id, arguments, context):

        outputs = ModuleOutputs()
        provenance = ModuleProvenance()
        output_ds_name = ""
        notebook_context = context.get_dataset('context')

        if command_id == cmd.SELECT_TRAINING or command_id == cmd.SELECT_TESTING:
            input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()
            input_dataset = context.get_dataset(input_ds_name)
            if input_dataset is None:
                raise ValueError('unknown dataset \'' + input_ds_name + '\'')

            sample_mode = None
            if command_id == cmd.SELECT_TRAINING:
                output_ds_name = (input_ds_name + "_training").lower()
                sampling_rate = float(arguments.get_value(cmd.PARA_SAMPLING_RATE))
                if sampling_rate > 1.0 or sampling_rate < 0.0:
                    raise Exception("Sampling rate must be between 0.0 and 1.0")
                sample_mode = {
                    "mode" : cmd.SAMPLING_MODE_UNIFORM_PROBABILITY,
                    "probability" : sampling_rate
                }
            elif command_id == cmd.SELECT_TESTING:
                output_ds_name = (input_ds_name + "_testing").lower()
                sampling_rate = float(arguments.get_value(cmd.PARA_SAMPLING_RATE))
                if sampling_rate > 1.0 or sampling_rate < 0.0:
                    raise Exception("Sampling rate must be between 0.0 and 1.0")
                sample_mode = {
                    "mode" : cmd.SAMPLING_MODE_UNIFORM_PROBABILITY,
                    "probability" : sampling_rate
                }
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
        elif command_id == cmd.SELECT_MODEL:
            # NOTE: THIS DOES NOT ACTUALLY DO ANYTHING AS OF NOW
            classifier = arguments.get_value(cmd.PARA_MODEL)
            outputs.stdout.append(TextOutput("{} selected for classification.".format(classifier)))
            
            """ save the model in a context database
            columns = [
                DatasetColumn(
                    identifier = 1,
                    name = "k",
                    data_type = "varchar"
                ),
                DatasetColumn(
                    identifier = 2,
                    name = "v",
                    data_type = "varchar"
                )
            ]

            rows = [
                DatasetRow(identifier=1, values=["model", classifier])
            ]
            ds = context.datastore.create_dataset(
                columns = columns,
                rows = rows
            )
            outputs.stdout.append("{} selected for classification.".format(classifier))
            provenance = ModuleProvenance(
            write={
                output_ds_name: DatasetDescriptor(
                    identifier=ds.identifier,
                    columns=ds.columns,
                    row_count=ds.row_count
                )
            }
        )"""
        else:
            raise Exception("Unknown pipeline command: {}".format(command_id))

        return ExecResult(
            outputs=outputs,
            provenance=provenance
        )
