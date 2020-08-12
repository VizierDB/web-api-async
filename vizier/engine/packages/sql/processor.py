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

"""Implementation of the task processor for the SQL package."""

import json
import sys

from vizier.datastore.dataset import DatasetDescriptor
from vizier.datastore.mimir.base import ROW_ID
from vizier.datastore.mimir.dataset import MimirDatasetColumn, MimirDatasetHandle
from vizier.engine.packages.mimir.processor import print_dataset_schema
from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.viztrail.module.output import ModuleOutputs, DatasetOutput, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance

import vizier.config.base as config
import vizier.engine.packages.sql.base as cmd
import vizier.mimir as mimir


class SQLTaskProcessor(TaskProcessor):
    """Implementation of the task processor for the SQL package."""
    def compute(self, command_id, arguments, context):
        """Execute the SQL query that is contained in the given arguments.

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
        if command_id == cmd.SQL_QUERY:
            return self.execute_query(
                args=arguments,
                context=context
            )
        else:
            raise ValueError('unknown sql command \'' + str(command_id) + '\'')

    def execute_query(self, args, context):
        """Execute a SQL query in the given context.

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
        # Get SQL source code that is in this cell and the global
        # variables
        source = args.get_value(cmd.PARA_SQL_SOURCE)
        if not source.endswith(';'):
            source = source 
        ds_name = args.get_value(cmd.PARA_OUTPUT_DATASET, raise_error=False)
        # Get mapping of datasets in the context to their respective table
        # name in the Mimir backend
        mimir_table_names = dict()
        for ds_name_o in context.datasets:
            dataset_id = context.datasets[ds_name_o].identifier
            dataset = context.datastore.get_dataset(dataset_id)
            if dataset is None:
                raise ValueError('unknown dataset \'' + ds_name_o + '\'')
            mimir_table_names[ds_name_o] = dataset.identifier
        # Module outputs
        outputs = ModuleOutputs()
        is_success = True
        try:
            # Create the view from the SQL source
            view_name, dependencies, mimirSchema, properties = mimir.createView(
                mimir_table_names,
                source
            )
            ds = MimirDatasetHandle.from_mimir_result(view_name, mimirSchema, properties)

            print(mimirSchema)
            
            if ds_name is None or ds_name == '':
                ds_name = "TEMPORARY_RESULT"

            from vizier.api.webservice import server

            ds_output = server.api.datasets.get_dataset(
                project_id=context.project_id,
                dataset_id=ds.identifier,
                offset=0,
                limit=10
            )
            if ds_output is None:
                outputs.stderr.append(TextOutput("Error displaying dataset {}".format(ds_name)))
            else:
                ds_output['name'] = ds_name
                outputs.stdout.append(DatasetOutput(ds_output))

            dependencies = dict(
                (dep_name.lower(), context.datasets.get(dep_name.lower(), None))
                for dep_name in dependencies
            )
            # print("---- SQL DATASETS ----\n{}\n{}".format(context.datasets, dependencies))

            provenance = ModuleProvenance(
                write={
                    ds_name: DatasetDescriptor(
                        identifier=ds.identifier,
                        name=ds_name,
                        columns=ds.columns
                    )
                },
                read=dependencies
            )
        except Exception as ex:
            provenance = ModuleProvenance()
            outputs.error(ex)
            is_success = False
        # Return execution result
        return ExecResult(
            is_success=is_success,
            outputs=outputs,
            provenance=provenance
        )
