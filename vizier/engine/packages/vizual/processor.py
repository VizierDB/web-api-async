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

from vizier.core.loader import ClassLoader
from vizier.core.util import is_valid_name
from vizier.datastore.dataset import DatasetDescriptor
from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.viztrail.module.output import ModuleOutputs, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance

import vizier.engine.packages.base as pckg
import vizier.engine.packages.vizual.base as cmd

"""Property defining the API class if instantiated from dictionary."""
PROPERTY_API = 'api'


class VizualTaskProcessor(TaskProcessor):
    """Implmentation of the task processor for the vizual package. The processor
    uses an instance of the vizual API to allow running on different types of
    datastores (e.g., the default datastore or the Mimir datastore).
    """
    def __init__(self, api=None, properties=None):
        """Initialize the vizual API instance. Either expects an API instance or
        a dictionary from which an instance can be loaded. The second option is
        only attempted if the given api is None.

        Parameters
        ----------
        api: vizier.engine.packages.vizual.api.base.VizualApi, optional
            Instance of the vizual API
        """
        if not api is None:
            self.api = api
        else:
            # Expects a dictionary with a single element that contains the
            # class specification
            values = properties[PROPERTY_API]
            self.api = ClassLoader(values=values).get_instance()

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
            raise ValueError('unknown vizual command \'' + str(command_id) + '\'')

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
        row_index = args.get_value(cmd.PARA_ROW)
        #  Get dataset. Raises exception if the dataset does not exist.
        ds = context.get_dataset(ds_name)
        # Execute delete row command
        result = self.api.delete_row(
            identifier=ds.identifier,
            row_index=row_index,
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
        ds = context.get_dataset(ds_name)
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

    def compute_load_dataset(self, args, context):
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
        if pckg.FILE_ID in source_desc and source_desc[pckg.FILE_ID] is not None:
            file_id = source_desc[pckg.FILE_ID]
        elif pckg.FILE_URL in source_desc and source_desc[pckg.FILE_URL] is not None:
            url = source_desc[pckg.FILE_URL]
        else:
            raise ValueError('invalid source descriptor')
        username = source_desc[pckg.FILE_USERNAME] if pckg.FILE_USERNAME in source_desc else None
        password = source_desc[pckg.FILE_PASSWORD] if pckg.FILE_PASSWORD in source_desc else None
        reload = source_desc[pckg.FILE_RELOAD] if pckg.FILE_RELOAD in source_desc else False
        load_format = args.get_value(cmd.PARA_LOAD_FORMAT)
        detect_headers = args.get_value(
            cmd.PARA_DETECT_HEADERS,
            raise_error=False,
            default_value=True
        )
        infer_types = args.get_value(
            cmd.PARA_INFER_TYPES,
            raise_error=False,
            default_value=True
        )
        options = args.get_value(cmd.PARA_LOAD_OPTIONS, raise_error=False)
        m_opts = []
        print(args.get_value(cmd.PARA_LOAD_DSE, raise_error=False, default_value=False))
        if args.get_value(cmd.PARA_LOAD_DSE, raise_error=False, default_value=False):
            m_opts.append({'datasourceErrors': 'true'})
        if not options is None:
            for option in options:
                load_opt_key = option.get_value(cmd.PARA_LOAD_OPTION_KEY)
                load_opt_val = option.get_value(cmd.PARA_LOAD_OPTION_VALUE)
                m_opts.append({load_opt_key: load_opt_val})
        # Execute load command.
        result = self.api.load_dataset(
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
            human_readable_name = ds_name.upper()
        )
        # Delete the uploaded file (of load was from file). A reference to the
        # created dataset is in the resources and will be used if the module is
        # re-executed.
        if not file_id is None:
            context.filestore.delete_file(file_id)
        # Create result object
        return self.create_exec_result(
            dataset_name=ds_name,
            output_dataset=result.dataset,
            stdout=result.dataset.print_schema(ds_name),
            database_state=context.datasets,
            resources=result.resources
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
        column_name = args.get_value(pckg.PARA_NAME)
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
                        columns=ds.columns,
                        row_count=ds.row_count
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
            stdout=[str(result.dataset.row_count) + ' row(s) sorted'],
            database_state=context.datasets
        )

        vizierdb.set_dataset_identifier(ds_name, ds_id)
        outputs.stdout(content=PLAIN_TEXT(str(count) + ' row(s) sorted'))

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
            stdout=['1 row updated'],
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
                columns=output_dataset.columns,
                row_count=output_dataset.row_count
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
