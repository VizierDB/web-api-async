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

"""Task processor for commands in the vizual package."""
from typing import Optional, Dict, Any

from vizier.core.loader import ClassLoader
from vizier.core.util import is_valid_name
from vizier.datastore.dataset import DatasetDescriptor
from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.viztrail.module.output import (
    ModuleOutputs, TextOutput, HtmlOutput, DatasetOutput
)
from vizier.viztrail.module.provenance import ModuleProvenance

import vizier.engine.packages.base as pckg
import vizier.engine.packages.vizual.base as cmd
import vizier.engine.packages.vizual.api.base as apibase
from vizier.viztrail.command import ARG_ID, ARG_VALUE, ModuleArguments
from vizier.config.app import AppConfig
from vizier.engine.task.base import TaskContext
from vizier.engine.packages.vizual.api.base import VizualApi, VizualApiResult

"""Property defining the API class if instantiated from dictionary."""
PROPERTY_API = 'api'

class VizualTaskProcessor(TaskProcessor):
    """Implmentation of the task processor for the vizual package. The processor
    uses an instance of the vizual API to allow running on different types of
    datastores (e.g., the default datastore or the Mimir datastore).
    """
    def __init__(self, 
            api: Optional[VizualApi] = None, 
            properties: Dict[str, Any] = None, 
            config: AppConfig = AppConfig()
        ):
        """Initialize the vizual API instance. Either expects an API instance or
        a dictionary from which an instance can be loaded. The second option is
        only attempted if the given api is None.

        Parameters
        ----------
        api: vizier.engine.packages.vizual.api.base.VizualApi, optional
            Instance of the vizual API
        """
        self.config = config
        self.api: VizualApi
        if api is not None:
            self.api = api
        elif properties is not None:
            # Expects a dictionary with a single element that contains the
            # class specification
            values = properties[PROPERTY_API]
            self.api = ClassLoader(values=values).get_instance()
        else:
            raise ValueError("VizualTaskProcessor expects either `properties` or `api`")

    def compute(self, command_id, arguments, context):
        """Compute results for the given vizual command using the set of user-
        provided arguments and the current database state. Return an execution
        result is case of success or error.

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
        if command_id == cmd.VIZUAL_DEL_COL:
            return self.compute_delete_column(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_DEL_ROW:
            return self.compute_delete_row(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_DROP_DS:
            return self.compute_drop_dataset(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_INS_COL:
            return self.compute_insert_column(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_INS_ROW:
            return self.compute_insert_row(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_LOAD:
            return self.compute_load_dataset(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_EMPTY_DS:
            return self.compute_empty_dataset(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_CLONE_DS:
            return self.compute_clone_dataset(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_UNLOAD:
            return self.compute_unload_dataset(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_MOV_COL:
            return self.compute_move_column(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_MOV_ROW:
            return self.compute_move_row(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_PROJECTION:
            return self.compute_filter_columns(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_REN_COL:
            return self.compute_rename_column(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_REN_DS:
            return self.compute_rename_dataset(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_SORT:
            return self.compute_sort_dataset(
                args=arguments,
                context=context
            )
        elif command_id == cmd.VIZUAL_UPD_CELL:
            return self.compute_update_cell(
                args=arguments,
                context=context
            )
        else:
            raise ValueError("unknown vizual command '{}'".format(command_id))

    def compute_delete_column(self, args, context):
        """Execute delete column command.

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
        # Get dataset name and column identifier.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        column_id = args.get_value(pckg.PARA_COLUMN)
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute delete column command
        result = self.api.delete_column(
            identifier=ds.identifier,
            column_id=column_id,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=['1 column deleted'],
            database_state=context.datasets
        )

    def compute_delete_row(self, args, context):
        """Execute delete row command.

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
        # Get dataset name and and row index.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        row = args.get_value(cmd.PARA_ROW)
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute delete row command
        result = self.api.delete_row(
            identifier=ds.identifier,
            row_index=row,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=['1 row deleted'],
            database_state=context.datasets
        )

    def compute_drop_dataset(self, args, context):
        """Execute drop dataset command.

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
        # Get dataset name and remove the associated entry from the
        # dictionary of datasets in the context. Will raise exception if the
        # specified dataset does not exist.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        # verify that the dataset exists
        context.get_dataset(ds_name)
        datasets = dict(context.datasets)
        del datasets[ds_name]
        return ExecResult(
            outputs=ModuleOutputs(
                stdout=[
                    TextOutput('Dataset \'' + ds_name + '\' deleted')
                ]
            ),
            provenance=ModuleProvenance(
                read=dict(),
                write=dict(),
                delete=[ds_name]
            )
        )

    def compute_filter_columns(self, args, context):
        """Execute projection command.

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
        # Get the name of the dataset and the list of columns to filter
        # as well as the optional new column name.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        columns = list()
        names = list()
        for col in args.get_value(cmd.PARA_COLUMNS):
            f_col = col.get_value(cmd.PARA_COLUMNS_COLUMN)
            columns.append(f_col)
            col_name = col.get_value(
                cmd.PARA_COLUMNS_RENAME,
                raise_error=False,
                default_value=None
            )
            if col_name == '':
                col_name = None
            names.append(col_name)
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute projection command.
        result = self.api.filter_columns(
            identifier=ds.identifier,
            columns=columns,
            names=names,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=[str(len(columns)) + ' column(s) filtered'],
            database_state=context.datasets
        )

    def compute_insert_column(self, args, context):
        """Execute insert column command.

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
        # Get dataset name, column index, and new column name.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        position = args.get_value(cmd.PARA_POSITION)
        column_name = args.get_value(pckg.PARA_NAME)
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute insert column command.
        result = self.api.insert_column(
            identifier=ds.identifier,
            position=position,
            name=column_name,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=['1 column inserted'],
            database_state=context.datasets
        )

    def compute_insert_row(self, args, context):
        """Execute insert row command.

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
        # Get dataset name, and row index.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        position = args.get_value(cmd.PARA_POSITION)
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute insert row command.
        result = self.api.insert_row(
            identifier=ds.identifier,
            position=position,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=['1 row inserted'],
            database_state=context.datasets
        )

    def compute_load_dataset(self, args: ModuleArguments, context: TaskContext) -> ExecResult:
        """Execute load dataset command.

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
        # Get the new dataset name. Raise exception if a dataset with the
        # specified name already exsists.
        ds_name = args.get_value(pckg.PARA_NAME).lower()
        if ds_name in context.datasets:
            raise ValueError('dataset \'' + ds_name + '\' exists')
        if not is_valid_name(ds_name):
            raise ValueError('invalid dataset name \'' + ds_name + '\'')
        # Get components of the load source. Raise exception if the source
        # descriptor is invalid.
        source_desc = args.get_value(cmd.PARA_FILE)
        file_id = None
        url = None
        if source_desc.get(pckg.FILE_ID) is not None:
            file_id = source_desc[pckg.FILE_ID]
        elif source_desc.get(pckg.FILE_URL) is not None:
            url = source_desc[pckg.FILE_URL]
        else:
            raise ValueError('invalid source descriptor')
        username = source_desc.get(pckg.FILE_USERNAME)
        password = source_desc.get(pckg.FILE_PASSWORD)
        reload = source_desc.get(pckg.FILE_RELOAD, False)
        load_format = args.get_value(cmd.PARA_LOAD_FORMAT)
        detect_headers = args.get_value(
            cmd.PARA_DETECT_HEADERS,
            raise_error=False,
            default_value=True
        )
        infer_types = args.get_value(cmd.PARA_INFER_TYPES)
        options = args.get_value(cmd.PARA_LOAD_OPTIONS, raise_error=False)
        m_opts = []
        if args.get_value(cmd.PARA_LOAD_DSE, raise_error=False, default_value=False):
            m_opts.append({'name': 'datasourceErrors', 'value': 'true'})
        if options is not None:
            for option in options:
                load_opt_key = option.get_value(cmd.PARA_LOAD_OPTION_KEY)
                load_opt_val = option.get_value(cmd.PARA_LOAD_OPTION_VALUE)
                m_opts.append({'name':load_opt_key, 'value':load_opt_val})

        proposed_schema = [
            (
                column.get_value(cmd.PARA_SCHEMA_COLUMN, raise_error=True),
                column.get_value(cmd.PARA_SCHEMA_TYPE, raise_error=True)
            )
            for column 
            in args.get_value(cmd.PARA_SCHEMA, raise_error=False, default_value=[])
        ]
        # Execute load command.
        result: VizualApiResult = self.api.load_dataset(
            datastore=context.datastore,
            filestore=context.filestore,
            file_id=file_id,
            url=url,
            detect_headers=detect_headers,
            infer_types=infer_types,
            load_format=load_format,
            options=m_opts,
            username=username,
            password=password,
            resources=context.resources,
            reload=reload,
            human_readable_name = ds_name.upper(),
            proposed_schema = proposed_schema
        )

        actual_schema = [
                ModuleArguments([
                    {
                        ARG_ID: cmd.PARA_SCHEMA_COLUMN,
                        ARG_VALUE: col.name
                    },
                    {
                        ARG_ID: cmd.PARA_SCHEMA_TYPE,
                        ARG_VALUE: col.data_type
                    }
                ], parent = cmd.PARA_SCHEMA)
                for col in result.dataset.columns
            ]
        updated_args = ModuleArguments(args.to_list())
        updated_args.arguments[cmd.PARA_SCHEMA] = actual_schema


        outputs = ModuleOutputs()
        ds_output = DatasetOutput.from_handle(result.dataset, context.project_id, ds_name)
        if ds_output is not None:
            outputs.stdout.append(ds_output)
        else:
            outputs.stderr.append(TextOutput("Error displaying dataset"))


        return ExecResult(
            outputs=outputs,
            provenance=ModuleProvenance(
                read=dict(), # need to explicitly declare a lack of dependencies
                write={ds_name: result.dataset},
                resources=result.resources
            ),
            updated_arguments = updated_args
        )

    def compute_empty_dataset(self, args, context):
        """Execute empty dataset command.

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
        ds_name = args.get_value(pckg.PARA_NAME).lower()
        if ds_name in context.datasets:
            raise ValueError('dataset \'' + ds_name + '\' exists')
        if not is_valid_name(ds_name):
            raise ValueError('invalid dataset name \'' + ds_name + '\'')
        try:
            result = self.api.empty_dataset(
                datastore = context.datastore,
                filestore = context.filestore
            )
            provenance = ModuleProvenance(
                write={
                    ds_name: DatasetDescriptor(
                        identifier=result.identifier,
                        name=ds_name,
                        columns=result.columns
                    )
                },
                read=dict()  # explicitly declare a lack of dependencies.
            )
            outputs.stdout.append(
                TextOutput("Empty dataset '{}' created".format(ds_name))
            )
        except Exception as ex:
            provenance = ModuleProvenance()
            outputs.error(ex)
        return ExecResult(
            is_success=(len(outputs.stderr) == 0),
            outputs=outputs,
            provenance=provenance
        )

    def compute_clone_dataset(self, args, context):
        """Execute empty dataset command.

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
        input_name = args.get_value(pckg.PARA_DATASET).lower()
        output_name = args.get_value(pckg.PARA_NAME).lower()
        if not is_valid_name(output_name):
            raise ValueError('invalid dataset name \'' + output_name + '\'')

        input_ds = context.get_dataset(input_name)
        if input_ds is None:
            raise ValueError('invalid dataset \'' + input_name + '\'')

        print("{}: {}".format(input_ds.identifier, input_ds))

        provenance = ModuleProvenance(
                write={output_name: input_ds},
                read={input_name: input_ds.identifier}
            )
        outputs = ModuleOutputs()
        outputs.stdout.append(
            TextOutput("Cloned `{}` as `{}`".format(input_name, output_name))
        )
        return ExecResult(
            is_success=True,
            outputs=outputs,
            provenance=provenance
        )

    def compute_unload_dataset(self, args, context):
        """Execute unload dataset command.

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
        # Get the new dataset name. Raise exception if a dataset with the
        # specified name already exsists.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()

        if not is_valid_name(ds_name):
            raise ValueError('invalid dataset name \'' + ds_name + '\'')
        # Get components of the load source. Raise exception if the source
        # descriptor is invalid.
        unload_format = args.get_value(cmd.PARA_UNLOAD_FORMAT)
        options = args.get_value(cmd.PARA_UNLOAD_OPTIONS, raise_error=False)
        m_opts = []

        if options is not None:
            for option in options:
                unload_opt_key = option.get_value(cmd.PARA_UNLOAD_OPTION_KEY)
                unload_opt_val = option.get_value(cmd.PARA_UNLOAD_OPTION_VALUE)
                m_opts.append({
                    'name': unload_opt_key,
                    'value': unload_opt_val
                })
        # Execute load command.
        dataset = context.get_dataset(ds_name)
        result = self.api.unload_dataset(
            dataset=dataset,
            datastore=context.datastore,
            filestore=context.filestore,
            unload_format=unload_format,
            options=m_opts,
            resources=context.resources
        )
        # Create result object
        outputhtml = HtmlOutput(''.join(["<div><a href=\""+ self.config.webservice.app_path+"/projects/"+str(context.project_id)+"/files/"+ out_file.identifier +"\" download=\""+out_file.name+"\">Download "+out_file.name+"</a></div>" for out_file in result.resources[apibase.RESOURCE_FILEID]]))
        return ExecResult(
            outputs=ModuleOutputs(
                stdout=[outputhtml]
            ),
            provenance=ModuleProvenance(
                read={
                    ds_name: context.datasets.get(ds_name.lower(), None)
                },
                write=dict()
            )
        )

    def compute_move_column(self, args, context):
        """Execute move column command.

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
        # Get dataset name, column name, and target position.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        column_id = args.get_value(pckg.PARA_COLUMN)
        position = args.get_value(cmd.PARA_POSITION)
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute move column command.
        result = self.api.move_column(
            identifier=ds.identifier,
            column_id=column_id,
            position=position,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=['1 column moved'],
            database_state=context.datasets
        )

    def compute_move_row(self, args, context):
        """Execute move row command.

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
        # Get dataset name, row index, and target index.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        row_index = args.get_value(cmd.PARA_ROW)
        position = args.get_value(cmd.PARA_POSITION)
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute move row command.
        result = self.api.move_row(
            identifier=ds.identifier,
            row_index=row_index,
            position=position,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=['1 row moved'],
            database_state=context.datasets
        )

    def compute_rename_column(self, args, context):
        """Execute rename column command.

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
        # Get dataset name, column specification, and new column name.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        column_id = args.get_value(pckg.PARA_COLUMN)
        column_name = args.get_value(pckg.PARA_NAME).upper()
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute rename column command.
        result = self.api.rename_column(
            identifier=ds.identifier,
            column_id=column_id,
            name=column_name,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=['1 column renamed'],
            database_state=context.datasets
        )

    def compute_rename_dataset(self, args, context):
        """Execute rename dataset command.

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
        # Get name of existing dataset and the new dataset name. Raise
        # exception if a dataset with the new name already exists or if the new
        # dataset name is not a valid name.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        new_name = args.get_value(pckg.PARA_NAME).lower()
        if new_name in context.datasets:
            raise ValueError('dataset \'' + new_name + '\' exists')
        if not is_valid_name(new_name):
            raise ValueError('invalid dataset name \'' + new_name + '\'')
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Adjust database state
        datasets = dict(context.datasets)
        del datasets[ds_name]
        datasets[new_name] = ds
        return ExecResult(
            outputs=ModuleOutputs(stdout=[TextOutput('1 dataset renamed')]),
            provenance=ModuleProvenance(
                read=dict(),
                write={
                    new_name: DatasetDescriptor(
                        identifier=ds.identifier,
                        name=new_name,
                        columns=ds.columns
                    )
                },
                delete=[ds_name]
            )
        )

    def compute_sort_dataset(self, args, context):
        """Execute sort dataset command.

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
        # Get the name of the dataset and the list of columns to sort on
        # as well as the optional sort order.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        columns = list()
        reversed = list()
        for col in args.get_value(cmd.PARA_COLUMNS):
            s_col = col.get_value(cmd.PARA_COLUMNS_COLUMN)
            columns.append(s_col)
            sort_order = col.get_value(
                cmd.PARA_COLUMNS_ORDER,
                default_value=cmd.SORT_DESC
            )
            reversed.append((sort_order == cmd.SORT_DESC))
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute sort dataset command.
        result = self.api.sort_dataset(
            identifier=ds.identifier,
            columns=columns,
            reversed=reversed,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=['dataset sorted'],
            database_state=context.datasets
        )

    def compute_update_cell(self, args, context):
        """Execute update cell command.

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
        # Get dataset name, cell coordinates, and update value.
        ds_name = args.get_value(pckg.PARA_DATASET).lower()
        column_id = args.get_value(pckg.PARA_COLUMN)
        row_id = args.get_value(cmd.PARA_ROW)
        value = args.get_value(cmd.PARA_VALUE, as_int=True)
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute update cell command. Replacte existing dataset
        # identifier with updated dataset id and set number of affected
        # rows in output
        # Execute update cell command.
        result = self.api.update_cell(
            identifier=ds.identifier,
            column_id=column_id,
            row_id=row_id,
            value=value,
            datastore=context.datastore
        )
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            input_dataset=ds,
            output_dataset=result.dataset,
            stdout=['row(s) updated'],
            database_state=context.datasets
        )

    def create_exec_result(
        self, dataset_name, input_dataset=None, output_dataset=None,
        database_state=None, stdout=None, resources=None
    ):
        """Create execution result object for a successfully completed task.
        Assumes that a single datasets has been modified.

        Note that this method is not suitable to generate the result object for
        the drop dataset and rename dataset commands.

        Parameters
        ----------
        dataset_name: string
            Name of the manipulated dataset
        input_dataset: vizier.datastore.dataset.DatasetDescriptor
            Descriptor for the input dataset
        output_dataset: vizier.datastore.dataset.DatasetDescriptor, optional
            Descriptor for the resulting dataset
        database_state: dict, optional
            Identifier for datasets in the database state agains which a task
            was executed (keyed by user-provided name)
        stdout= list(string), optional
            Lines in the command output
        resources: dict, optional
            Optional resources that were generated by the command

        Returns
        -------
        vizier.engine.task.processor.ExecResult
        """
        if not output_dataset is None:
            ds = DatasetDescriptor(
                identifier=output_dataset.identifier,
                name=dataset_name,
                columns=output_dataset.columns
            )
        else:
            ds = None
        return ExecResult(
            outputs=ModuleOutputs(stdout=[TextOutput(line) for line in stdout]),
            provenance=ModuleProvenance(
                read={dataset_name: input_dataset.identifier} if not input_dataset is None else None,
                write={dataset_name: ds},
                resources=resources
            )
        )
