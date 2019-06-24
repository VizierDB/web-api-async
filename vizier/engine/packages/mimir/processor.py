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

"""Implementation of the task processor that executes commands that are defined
in the Mimir lenses package.
"""

from vizier.core.util import is_valid_name
from vizier.datastore.dataset import DATATYPE_REAL
from vizier.datastore.mimir.dataset import MimirDatasetColumn
from vizier.datastore.mimir.base import ROW_ID
from vizier.datastore.mimir.store import create_missing_key_view
from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.viztrail.module.output import ModuleOutputs, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance

import vizier.engine.packages.base as pckg
import vizier.engine.packages.mimir.base as cmd
import vizier.mimir as mimir


class MimirProcessor(TaskProcessor):
    """Implmentation of the task processor for the mimir package. The processor
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
        
    
    """Task processor  to execute commands in the Mimir package."""
    def compute(self, command_id, arguments, context):
        """Compute results for commands in the Mimir package using the set of
        user-provided arguments and the current database state.

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
        outputs = ModuleOutputs()
        store_as_dataset = None
        update_rows = False
        lens_annotations = []
        # Get dataset. Raise exception if dataset is unknown.
        ds_name = arguments.get_value(pckg.PARA_DATASET).lower()
        dataset = context.get_dataset(ds_name)
        mimir_table_name = dataset.table_name
        # Keep track of the name of the input dataset for the provenance
        # information.
        input_ds_name = ds_name
        if command_id == cmd.MIMIR_DOMAIN:
            column = dataset.column_by_id(arguments.get_value(pckg.PARA_COLUMN))
            params = [column.name_in_rdb]
        elif command_id == cmd.MIMIR_GEOCODE:
            geocoder = arguments.get_value(cmd.PARA_GEOCODER)
            params = ['GEOCODER(' + geocoder + ')']
            add_column_parameter(params, 'HOUSE_NUMBER', dataset, arguments, cmd.PARA_HOUSE_NUMBER)
            add_column_parameter(params, 'STREET', dataset, arguments, cmd.PARA_STREET)
            add_column_parameter(params, 'CITY', dataset, arguments, cmd.PARA_CITY)
            add_column_parameter(params, 'STATE', dataset, arguments, cmd.PARA_STATE)
            # Add columns for LATITUDE and LONGITUDE
            column_counter = dataset.max_column_id() + 1
            cname_lat = dataset.get_unique_name('LATITUDE')
            cname_lon = dataset.get_unique_name('LONGITUDE')
            dataset.columns.append(
                MimirDatasetColumn(
                    identifier=column_counter,
                    name_in_dataset=cname_lat,
                    data_type=DATATYPE_REAL
                )
            )
            dataset.columns.append(
                MimirDatasetColumn(
                    identifier=column_counter + 1,
                    name_in_dataset=cname_lon,
                    data_type=DATATYPE_REAL
                )
            )
            params.append('RESULT_COLUMNS(' + cname_lat + ',' + cname_lon + ')')
        elif command_id == cmd.MIMIR_KEY_REPAIR:
            column = dataset.column_by_id(arguments.get_value(pckg.PARA_COLUMN))
            params = [column.name_in_rdb]
            update_rows = True
        elif command_id == cmd.MIMIR_MISSING_KEY:
            column = dataset.column_by_id(arguments.get_value(pckg.PARA_COLUMN))
            params = [column.name_in_rdb]
            # Set MISSING ONLY to FALSE to ensure that all rows are returned
            params += ['MISSING_ONLY(FALSE)']
            # Need to run this lens twice in order to generate row ids for
            # any potential new tuple
            mimir_lens_response = mimir.createLens(
                dataset.table_name,
                params,
                command_id,
                arguments.get_value(cmd.PARA_MAKE_CERTAIN, default_value=True),
                False
            )
            (mimir_table_name, lens_annotations) = (
                mimir_lens_response.lensName(),
                mimir_lens_response.annotations()
            )
            params = [ROW_ID, 'MISSING_ONLY(FALSE)']
            update_rows = True
        elif command_id == cmd.MIMIR_MISSING_VALUE:
            params = list()
            for col in arguments.get_value(cmd.PARA_COLUMNS, default_value=[]):
                f_col = dataset.column_by_id(col.get_value(pckg.PARA_COLUMN))
                param = f_col.name_in_rdb
                col_constraint = col.get_value(
                    cmd.PARA_COLUMNS_CONSTRAINT,
                    raise_error=False
                )
                if col_constraint == '':
                    col_constraint = None
                if not col_constraint is None:
                    param = param + ' ' + str(col_constraint).replace("'", "\'\'").replace("OR", ") OR (")
                param = '\'(' + param + ')\''
                params.append(param)
        elif command_id == cmd.MIMIR_PICKER:
            pick_from = list()
            column_names = list()
            for col in arguments.get_value(cmd.PARA_SCHEMA):
                c_col = col.get_value(cmd.PARA_PICKFROM)
                column = dataset.column_by_id(c_col)
                pick_from.append(column.name_in_rdb)
                column_names.append(column.name.upper().replace(' ', '_'))
            # Add result column to dataset schema
            pick_as = arguments.get_value(
                cmd.PARA_PICKAS,
                default_value='PICK_ONE_' + '_'.join(column_names)
            )
            pick_as = dataset.get_unique_name(pick_as.strip().upper())
            dataset.columns.append(
                MimirDatasetColumn(
                    identifier=dataset.max_column_id() + 1,
                    name_in_dataset=pick_as
                )
            )
            params = ['PICK_FROM(' + ','.join(pick_from) + ')']
            params.append('PICK_AS(' + pick_as + ')')
        elif command_id == cmd.MIMIR_SCHEMA_MATCHING:
            store_as_dataset = arguments.get_value(cmd.PARA_RESULT_DATASET)
            if store_as_dataset in context.datasets:
                raise ValueError('dataset \'' + store_as_dataset + '\' exists')
            if not is_valid_name(store_as_dataset):
                raise ValueError('invalid dataset name \'' + store_as_dataset + '\'')
            column_names = list()
            params = ['\'' + ROW_ID + ' int\'']
            for col in arguments.get_value(cmd.PARA_SCHEMA):
                c_name = col.get_value(pckg.PARA_COLUMN)
                c_type = col.get_value(cmd.PARA_TYPE)
                params.append('\'' + c_name + ' ' + c_type + '\'')
                column_names.append(c_name)
        elif command_id == cmd.MIMIR_TYPE_INFERENCE:
            params = [str(arguments.get_value(cmd.PARA_PERCENT_CONFORM))]
        elif command_id == cmd.MIMIR_SHAPE_DETECTOR:
            dseModel = arguments.get_value(cmd.PARA_MODEL_NAME)
            params = []
            if not dseModel is None:
                params = [str(dseModel)]
        else:
            raise ValueError('unknown Mimir lens \'' + str(lens) + '\'')
        # Create Mimir lens
        if command_id in [cmd.MIMIR_SCHEMA_MATCHING, cmd.MIMIR_TYPE_INFERENCE, cmd.MIMIR_SHAPE_DETECTOR]:
            lens_name = mimir.createAdaptiveSchema(
                mimir_table_name,
                params,
                command_id.upper()
            )
        else:
            mimir_lens_response = mimir.createLens(
                mimir_table_name,
                params,
                command_id.upper(),
                arguments.get_value(cmd.PARA_MAKE_CERTAIN, default_value=True),
                False,
                human_readable_name = ds_name.upper()
            )
            (lens_name, lens_annotations) = (
                mimir_lens_response['lensName'],
                mimir_lens_response['annotations']
            )
        # Create a view including missing row ids for the result of a
        # MISSING KEY lens
        if command_id == cmd.MIMIR_MISSING_KEY:
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
                columns.append(
                    MimirDatasetColumn(
                        identifier=col_id,
                        name_in_dataset=c_name
                    )
                )
            ds = context.datastore.register_dataset(
                table_name=lens_name,
                columns=columns,
                row_idxs=dataset.row_idxs,
                row_ids=dataset.row_ids,
                annotations=dataset.annotations,
                update_rows=True
            )
            ds_name = store_as_dataset
        else:
            ds = context.datastore.register_dataset(
                table_name=lens_name,
                columns=dataset.columns,
                row_idxs=dataset.row_idxs,
                row_ids=dataset.row_ids,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations,
                update_rows=update_rows
            )
        # Add dataset schema and returned annotations to output
        print_dataset_schema(outputs, ds_name, ds.columns)
        print_lens_annotations(outputs, lens_annotations)
        # Return task result
        return ExecResult(
            outputs=outputs,
            provenance=ModuleProvenance(
                read={input_ds_name: dataset.identifier},
                write={ds_name: ds}
            )
        )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def add_column_parameter(params, name, dataset, args, key):
    """Add a Mimir lens parameter to the given list. This is a shortcut to add
    a column parameter to a list of arguments for a lens.

    params: list
        List of Mimir lens parameters
    name: string
        Name of the parameter
    dataset: vizier.datastore.base.DatasetHandle
        Dataset handle
    args : dict(dict())
        Dictionary of command arguments
    key : string
        Argument name
    """
    column_id = args.get_value(key, raise_error=False)
    if column_id is None:
        return
    column = dataset.column_by_id(column_id)
    params.append(name + '(' + column.name_in_rdb + ')')


def print_dataset_schema(outputs, name, columns):
    """Add schema infromation for given dataset to cell output.

    Parameters
    ----------
    outputs: vizier.workflow.module.ModuleOutputs
        Cell outputt streams
    name: string
        Dataset name
    columns: list(vizier.datasetore.base.DatasetColumn)
        Columns in the dataset schema
    """
    outputs.stdout.append(TextOutput(name + ' ('))
    for i in range(len(columns)):
        text = '  ' + str(columns[i])
        if i != len(columns) - 1:
            text += ','
        outputs.stdout.append(TextOutput(text))
    outputs.stdout.append(TextOutput(')'))


def print_lens_annotations(outputs, annotations):
    """Add annotation infromation for given lens to cell output.

    Parameters
    ----------
    outputs: vizier.workflow.module.ModuleOutputs
        Cell outputt streams
    annotations: dict
        Annotations from first 200 rows of queried lens
    """
    if not annotations is None:
        if annotations > 0:
            outputs.stdout.append(TextOutput('Repairs in first 200 rows:'))
            outputs.stdout.append(TextOutput(str(annotations)))
