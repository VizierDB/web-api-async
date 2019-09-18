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
from vizier.datastore.mimir.dataset import MimirDatasetColumn
from vizier.engine.packages.mimir.processor import print_dataset_schema
from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.viztrail.module.output import ModuleOutputs, TextOutput
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
            source = source + ';'
        ds_name = args.get_value(cmd.PARA_OUTPUT_DATASET, raise_error=False)
        # Get mapping of datasets in the context to their respective table
        # name in the Mimir backend
        mimir_table_names = dict()
        for ds_name_o in context.datasets:
            dataset_id = context.datasets[ds_name_o]
            dataset = context.datastore.get_dataset(dataset_id)
            if dataset is None:
                raise ValueError('unknown dataset \'' + ds_name_o + '\'')
            mimir_table_names[ds_name_o] = dataset.table_name
        # Module outputs
        outputs = ModuleOutputs()
        try:
            # Create the view from the SQL source
            view_name = mimir.createView(
                mimir_table_names,
                source
            )
            sql = 'SELECT * FROM ' + view_name
            mimirSchema = mimir.getSchema(sql)

            columns = list()
            colSql = ''

            for col in mimirSchema:
                col_id = len(columns)
                name_in_dataset = col['name']
                col = MimirDatasetColumn(
                    identifier=col_id,
                    name_in_dataset=name_in_dataset
                )
                if colSql == '':
                    colSql = name_in_dataset + ' AS ' + name_in_dataset
                else:
                    colSql = colSql + ', ' + name_in_dataset + ' AS ' + name_in_dataset
                columns.append(col)

            sql = 'SELECT ' + colSql + ' FROM {{input}};'
            view_name = mimir.createView(view_name, sql)

            row_count = mimir.countRows(view_name)
            
            provenance = None
            if ds_name is None or ds_name == '':
                ds_name = "TEMPORARY_RESULT"

            
            ds = context.datastore.register_dataset(
                table_name=view_name,
                columns=columns,
                row_counter=row_count
            )
            print_dataset_schema(outputs, ds_name, ds.columns)
            outputs.stdout.append(TextOutput(str(row_count) + ' row(s)'))
            provenance = ModuleProvenance(
                write={
                    ds_name: DatasetDescriptor(
                        identifier=ds.identifier,
                        columns=ds.columns,
                        row_count=ds.row_count
                    )
                }
            )
        except Exception as ex:
            provenance = ModuleProvenance()
            outputs.error(ex)
        # Return execution result
        return ExecResult(
            is_success=(len(outputs.stderr) == 0),
            outputs=outputs,
            provenance=provenance
        )
