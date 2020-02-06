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

    def save_context(self, context, notebook_context, key, value):

        rows = notebook_context.fetch_rows()
        is_key_present = False
        for i, row in enumerate(rows):
            if row.values[0] == key:
                is_key_present = True
                row[i] = DatasetRow(
                    identifier = notebook_context.max_row_id()+1,
                    values = [key, value]
                )
                break
        if not is_key_present:
            rows.append(
                DatasetRow(
                    identifier = notebook_context.max_row_id()+1,
                    values = [key, value]
                )
            )
        print([row.values for row in rows], flush = True)
        ds = context.datastore.create_dataset(
            columns = notebook_context.columns,
            rows = rows,
            annotations = notebook_context.annotations
        )
        return ds


    def compute(self, command_id, arguments, context):

        outputs = ModuleOutputs()
        provenance = ModuleProvenance()
        output_ds_name = ""
        notebook_context = context.get_dataset(cmd.CONTEXT_DATABASE_NAME)

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

            classifier = arguments.get_value(cmd.PARA_MODEL)

            outputs.stdout.append(TextOutput("{} selected for classification.".format(classifier)))

            ds = self.save_context(context, notebook_context, "model", classifier)

            provenance = ModuleProvenance(
                write = {
                    cmd.CONTEXT_DATABASE_NAME: DatasetDescriptor(
                        identifier=ds.identifier,
                        columns=ds.columns,
                        row_count=ds.row_count
                        )
                    }
            )
        elif command_id == cmd.SELECT_PREDICTION_COLUMNS:

            input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()
            input_dataset = context.get_dataset(input_ds_name)

            columns = arguments.get_value(cmd.PARA_COLUMNS)
            outputs.stdout.append(TextOutput("Input columns selected for prediction. "))

            #ds = self.save_context(context, notebook_context, "columns", [column.arguments["columns_column"] for column in columns])
            ds = self.save_context(context, notebook_context, "columns", 1)
            #ds = self.save_context(context, notebook_context, "columns", [column for column in columns])

            provenance = ModuleProvenance(
                read = {
                    input_ds_name: input_dataset.identifier
                },
                write={
                    cmd.CONTEXT_DATABASE_NAME: DatasetDescriptor(
                        identifier=ds.identifier,
                        columns=ds.columns,
                        row_count=ds.row_count
                        )
                    }
            )
        
        elif command_id == cmd.SELECT_LABEL_COLUMN:

            input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()
            input_dataset = context.get_dataset(input_ds_name)
            label_column = arguments.get_value(cmd.PARA_LABEL_COLUMN)

            ds = self.save_context(context, notebook_context, "label", label_column)

            outputs.stdout.append(TextOutput("Column {} selected as the label column. ".format(label_column)))

            provenance = ModuleProvenance(
                read = {
                    input_ds_name: input_dataset.identifier
                },
                write={
                    cmd.CONTEXT_DATABASE_NAME: DatasetDescriptor(
                        identifier=ds.identifier,
                        columns=ds.columns,
                        row_count=ds.row_count
                        )
                    }
            )

        elif command_id == cmd.SELECT_ACCURACY_METRIC:
            saved_context = [row.values for row in notebook_context.fetch_rows()]
            print(saved_context)
            
        else:
            raise Exception("Unknown pipeline command: {}".format(command_id))

        return ExecResult(
            outputs=outputs,
            provenance=provenance
        )
