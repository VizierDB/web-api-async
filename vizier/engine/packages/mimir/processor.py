# Copyright (C) 2018 New York University,
#                    University at Buffalo,
#                    Illinois Institute of Technology.
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

"""Implementation of the task processor that executes commands that are defined
in the default Mimir package declaration.
"""

from vizier.engine.task.processor import TaskProcessor


class MimirProcessor(TaskProcessor):
    """Task processor  to execute commands in the Mimir package."""
    def compute(self, command_id, arguments, context):
        """Compute results for commands in the Mimir package using the set of
        user- provided arguments and the current database state.

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
        # Get input arguments
        lens = self.get_input('name')
        args = self.get_input('arguments')
        context = self.get_input('context')
        # Get module identifier and VizierDB client for current workflow state
        module_id = self.moduleInfo['moduleId']
        vizierdb = get_env(module_id, context)
        # Get command text. Catch potential exceptions (bugs) to avoid that the
        # workflow execution breaks here.
        #try:
        cmd_text = self.to_string(lens, args, vizierdb)
        #except Exception as ex:
        #    cmd_text = 'MIMIR ' + str(name)
        self.set_output('command', cmd_text)
        # Module outputs
        outputs = ModuleOutputs()
        store_as_dataset = None
        update_rows = False
        # Get dataset. Raise exception if dataset is unknown
        ds_name = get_argument(cmd.PARA_DATASET, args).lower()
        dataset_id = vizierdb.get_dataset_identifier(ds_name)
        dataset = vizierdb.datastore.get_dataset(dataset_id)
        if dataset is None:
            raise ValueError('unknown dataset \'' + ds_name + '\'')
        mimir_table_name = dataset.table_name
        if lens == cmd.MIMIR_DOMAIN:
            column = get_column_argument(dataset, args, cmd.PARA_COLUMN)
            params = [column.name_in_rdb]
        elif lens == cmd.MIMIR_GEOCODE:
            geocoder = get_argument(cmd.PARA_GEOCODER, args)
            params = ['GEOCODER(' + geocoder + ')']
            add_param(params, 'HOUSE_NUMBER', dataset, args, cmd.PARA_HOUSE_NUMBER)
            add_param(params, 'STREET', dataset, args, cmd.PARA_STREET)
            add_param(params, 'CITY', dataset, args, cmd.PARA_CITY)
            add_param(params, 'STATE', dataset, args, cmd.PARA_STATE)
            # Add columns for LATITUDE and LONGITUDE
            c_id = dataset.column_counter
            c_lat = COL_PREFIX + str(c_id)
            dataset.columns.append(MimirDatasetColumn(c_id, 'LATITUDE', c_lat))
            dataset.column_counter += 1
            c_id = dataset.column_counter
            c_lon = COL_PREFIX + str(c_id)
            dataset.columns.append(MimirDatasetColumn(c_id, 'LONGITUDE', c_lon))
            dataset.column_counter += 1
            params.append('RESULT_COLUMNS(' + c_lat + ',' + c_lon + ')')
        elif lens == cmd.MIMIR_KEY_REPAIR:
            column = get_column_argument(dataset, args, cmd.PARA_COLUMN)
            params = [column.name_in_rdb]
            update_rows = True
        elif lens == cmd.MIMIR_MISSING_KEY:
            column = get_column_argument(dataset, args, cmd.PARA_COLUMN)
            params = [column.name_in_rdb]
            # Set MISSING ONLY to FALSE to ensure that all rows are returned
            params += ['MISSING_ONLY(FALSE)']
            # Need to run this lens twice in order to generate row ids for
            # any potential new tuple
            mimir_table_name = mimir._mimir.createLens(
                dataset.table_name,
                mimir._jvmhelper.to_scala_seq(params),
                lens,
                get_argument(cmd.PARA_MAKE_CERTAIN, args),
                False
            )
            params = [ROW_ID, 'MISSING_ONLY(FALSE)']
            update_rows = True
        elif lens == cmd.MIMIR_MISSING_VALUE:
            column = get_column_argument(dataset, args, cmd.PARA_COLUMN)
            params = column.name_in_rdb
            if cmd.PARA_CONSTRAINT in args:
                params = params + ' ' + str(args[cmd.PARA_CONSTRAINT])
                params = '\'' + params + '\''
            params = [params]
        elif lens == cmd.MIMIR_PICKER:
            pick_from = list()
            column_names = list()
            for col in get_argument(cmd.PARA_SCHEMA, args):
                c_col = get_argument(cmd.PARA_PICKFROM, col, as_int=True)
                column = dataset.column_by_id(c_col)
                pick_from.append(column.name_in_rdb)
                column_names.append(column.name.upper().replace(' ', '_'))
            # Add result column to dataset schema
            pick_as = ''
            if cmd.PARA_PICKAS in args:
                pick_as = args[cmd.PARA_PICKAS].strip()
            if pick_as == '':
                pick_as = 'PICK_ONE_' + '_'.join(column_names)
            target_column = COL_PREFIX + str(dataset.column_counter)
            dataset.columns.append(
                MimirDatasetColumn(
                    dataset.column_counter,
                    pick_as,
                    target_column
                )
            )
            dataset.column_counter += 1
            params = ['PICK_FROM(' + ','.join(pick_from) + ')']
            params.append('PICK_AS(' + target_column + ')')
        elif lens == cmd.MIMIR_SCHEMA_MATCHING:
            store_as_dataset = get_argument(cmd.PARA_RESULT_DATASET, args)
            if vizierdb.has_dataset_identifier(store_as_dataset):
                raise ValueError('dataset \'' + store_as_dataset + '\' exists')
            if not is_valid_name(store_as_dataset):
                raise ValueError('invalid dataset name \'' + store_as_dataset + '\'')
            column_names = list()
            params = ['\'' + ROW_ID + ' int\'']
            for col in get_argument(cmd.PARA_SCHEMA, args):
                c_name = get_argument(cmd.PARA_COLUMN, col)
                c_type = get_argument(cmd.PARA_TYPE, col)
                params.append('\'' + COL_PREFIX + str(len(column_names)) + ' ' + c_type + '\'')
                column_names.append(c_name)
        elif lens == cmd.MIMIR_TYPE_INFERENCE:
            params = [str(get_argument(cmd.PARA_PERCENT_CONFORM, args))]
        else:
            raise ValueError('unknown Mimir lens \'' + str(lens) + '\'')
        # Create Mimir lens
        if lens in [cmd.MIMIR_SCHEMA_MATCHING, cmd.MIMIR_TYPE_INFERENCE]:
            lens_name = mimir._mimir.createAdaptiveSchema(
                mimir_table_name,
                mimir._jvmhelper.to_scala_seq(params),
                lens
            )
        else:
            lens_name = mimir._mimir.createLens(
                mimir_table_name,
                mimir._jvmhelper.to_scala_seq(params),
                lens,
                get_argument(cmd.PARA_MAKE_CERTAIN, args),
                False
            )
        # Create a view including missing row ids for the result of a
        # MISSING KEY lens
        if lens == cmd.MIMIR_MISSING_KEY:
            lens_name, row_counter = create_missing_key_view(
                dataset,
                lens_name,
                column
            )
            dataset.row_counter = row_counter
        # Create datastore entry for lens.
        if not store_as_dataset is None:
            columns = list()
            for c_name in column_names:
                col_id = len(columns)
                columns.append(MimirDatasetColumn(
                    col_id,
                    c_name,
                    COL_PREFIX + str(col_id)
                ))
            #ds = vizierdb.datastore.create_dataset(table_name, columns)
            ds = vizierdb.datastore.register_dataset(
                table_name=lens_name,
                columns=columns,
                row_ids=dataset.row_ids,
                annotations=dataset.annotations,
                update_rows=True
            )
            ds_name = store_as_dataset
        else:
            ds = vizierdb.datastore.register_dataset(
                table_name=lens_name,
                columns=dataset.columns,
                row_ids=dataset.row_ids,
                column_counter=dataset.column_counter,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations,
                update_rows=update_rows
            )
        print_dataset_schema(outputs, ds_name, ds.columns)
        vizierdb.set_dataset_identifier(ds_name, ds.identifier)
        # Propagate potential changes to the dataset mappings
        propagate_changes(module_id, vizierdb.datasets, context)
        # Set the module outputs
        self.set_output('context', context)
        self.set_output('output', outputs)
