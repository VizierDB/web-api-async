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

import re

from vizier.core.util import is_valid_name
from vizier.datastore.dataset import DATATYPE_REAL, DatasetDescriptor
from vizier.datastore.mimir.dataset import MimirDatasetColumn
from vizier.datastore.mimir.base import ROW_ID
from vizier.datastore.mimir.store import create_missing_key_view
from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.viztrail.module.output import ModuleOutputs, TextOutput, DatasetOutput
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.api.webservice import server

import vizier.engine.packages.vizual.api.base as base
import vizier.engine.packages.base as pckg
import vizier.engine.packages.mimir.base as cmd
import vizier.mimir as mimir

LENSES_THAT_SHOULD_NOT_DISPLAY_TABLES = set([
    cmd.MIMIR_SCHEMA_MATCHING, 
    cmd.MIMIR_TYPE_INFERENCE, 
    cmd.MIMIR_SHAPE_DETECTOR
])


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
            house = arguments.get_value(cmd.PARA_HOUSE_NUMBER, raise_error=False, default_value=None)
            street = arguments.get_value(cmd.PARA_STREET, raise_error=False, default_value=None)
            city = arguments.get_value(cmd.PARA_CITY, raise_error=False, default_value=None)
            state = arguments.get_value(cmd.PARA_STATE, raise_error=False, default_value=None)

            params = {
                'houseColumn': dataset.column_by_id(house).name_in_rdb   if house  is not None and house  != '' else None,
                'streetColumn': dataset.column_by_id(street).name_in_rdb if street is not None and street != '' else None,
                'cityColumn': dataset.column_by_id(city).name_in_rdb     if city   is not None and city   != '' else None,
                'stateColumn': dataset.column_by_id(state).name_in_rdb   if state  is not None and state  != '' else None,
                'geocoder': geocoder#,
                #'latitudeColumn': Option[String],
                #'longitudeColumn': Option[String],
                #'cacheCode': Option[String]
            }
        elif command_id == cmd.MIMIR_KEY_REPAIR:
            column = dataset.column_by_id(arguments.get_value(pckg.PARA_COLUMN))
            params = { "key" : column.name_in_rdb }
            update_rows = True
        elif command_id == cmd.MIMIR_MISSING_KEY:
            column = dataset.column_by_id(arguments.get_value(pckg.PARA_COLUMN))
            params = column.name_in_rdb
            # Set MISSING ONLY to FALSE to ensure that all rows are returned
            #params += ['MISSING_ONLY(FALSE)']
            # Need to run this lens twice in order to generate row ids for
            # any potential new tuple
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
                #if not col_constraint is None:
                #    param = param + ' ' + str(col_constraint).replace("'", "\'\'").replace("OR", ") OR (")
                #param = '\'(' + param + ')\''
                params.append(param)
        elif command_id == cmd.MIMIR_PICKER:
            ## Compute the input columns
            inputs = []
            for col in arguments.get_value(cmd.PARA_SCHEMA):
                c_col = col.get_value(cmd.PARA_PICKFROM)
                column = dataset.column_by_id(c_col)
                inputs.append(column.name_in_rdb)

            ## Compute the output column
            output = arguments.get_value(cmd.PARA_PICKAS, default_value = inputs[0])
            if output == "":
                output = inputs[0]
            else:
                output = dataset.get_unique_name(output.strip().upper())

            ## Compute the final parameter list
            params = {
                "inputs" : inputs,
                "output" : output
            }
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
        elif command_id == cmd.MIMIR_COMMENT:
            commentsParams = []
            for idx, comment in enumerate(arguments.get_value(cmd.PARA_COMMENTS)):
                commentParam = {}
                
                # If target is defined, it is the column that we're trying to annotate
                # If unset (or empty), it means we're annotating the row.
                column_id = comment.get_value(cmd.PARA_EXPRESSION, None)

                if column_id is not None:
                    column = dataset.column_by_id(column_id)
                    commentParam['target'] = column.name_in_rdb

                # The comment
                commentParam['comment'] = comment.get_value(cmd.PARA_COMMENT)

                # If rowid is defined, it is the row that we're trying to annotate.  
                # If unset (or empty), it means that we're annotating all rows
                rowid = comment.get_value(cmd.PARA_ROWID, None) 
                if (rowid is not None) and (rowid != ""):
                    # If rowid begins with '=', it's a formula
                    if rowid[0] == '=':
                        commentParam['condition'] = rowid[1:]
                    else:
                        commentParam['rows'] = [ int(rowid) ]
                
                #TODO: handle result columns
                commentsParams.append(commentParam)
            params = {'comments' : commentsParams}
        elif command_id == cmd.MIMIR_PIVOT:
            column = dataset.column_by_id(arguments.get_value(pckg.PARA_COLUMN))
            params = {
                "target" : column.name_in_rdb,
                "keys" : [],
                "values" : []
            }
            for col_arg in arguments.get_value(cmd.PARA_VALUES):
                col = dataset.column_by_id(col_arg.get_value(cmd.PARA_VALUE))
                params["values"].append(col.name_in_rdb)
            for col_arg in arguments.get_value(cmd.PARA_KEYS, default_value=[]):
                col = dataset.column_by_id(col_arg.get_value(cmd.PARA_KEY))
                params["keys"].append(col.name_in_rdb)
            if len(params["values"]) < 1:
                raise ValueError("Need at least one value column")
            update_rows = True
            store_as_dataset = arguments.get_value(cmd.PARA_RESULT_DATASET)
        elif command_id == cmd.MIMIR_SHRED:
            params = { 
                "keepOriginalColumns" : arguments.get_value(cmd.PARA_KEEP_ORIGINAL)
            }
            shreds = []
            global_input_col = dataset.column_by_id(arguments.get_value(cmd.PARA_COLUMN_NAME))
            for (idx, shred) in enumerate(arguments.get_value(cmd.PARA_COLUMNS)):
                output_col = shred.get_value(cmd.PARA_OUTPUT_COLUMN)
                if output_col is None:
                    output_col = "{}_{}".format(input_col,idx)
                config = {}
                shred_type = shred.get_value(cmd.PARA_TYPE)
                expression = shred.get_value(cmd.PARA_EXPRESSION)
                group = shred.get_value(cmd.PARA_INDEX)
                if shred_type == "pattern":
                    config["regexp"] = expression
                    config["group"] = int(group)
                elif shred_type == "field":
                    config["separator"] = expression
                    config["field"] = int(group)
                elif shred_type == "explode":
                    config["separator"] = expression
                elif shred_type == "pass":
                    pass
                elif shred_type == "substring":
                    range_parts = re.match("([0-9]+)(([+\\-])([0-9]+))?", expression)
                    # print(range_parts)

                    ## Mimir expects ranges to be given from start (inclusive) to end (exclusive)
                    ## in a zero-based numbering scheme.

                    ## Vizier expects input ranges to be given in a one-based numbering scheme.

                    # Convert to this format

                    if range_parts is None:
                        raise ValueError("Substring requires a range of the form '10', '10-11', or '10+1', but got '{}'".format(expression))
                    config["start"] = int(range_parts.group(1))-1 # Convert 1-based numbering to 0-based
                    if range_parts.group(2) is None:
                        config["end"] = config["start"] + 1 # if only one character, split one character
                    elif range_parts.group(3) == "+":
                        config["end"] = config["start"] + int(range_parts.group(4)) # start + length
                    elif range_parts.group(3) == "-":
                        config["end"] = int(range_parts.group(4)) # Explicit end, 1-based -> 0-based and exclusive cancel out
                    else:
                        raise ValueError("Invalid expression '{}' in substring shredder".format(expression))
                    # print("Shredding {} <- {} -- {}".format(output_col,config["start"],config["end"]))
                else:
                    raise ValueError("Invalid Shredding Type '{}'".format(shred_type))

                shreds.append({
                    **config,
                    "op" : shred_type,
                    "input" : global_input_col.name_in_rdb,
                    "output" : output_col,
                })
            params["shreds"] = shreds
            store_as_dataset = arguments.get_value(cmd.PARA_RESULT_DATASET)
        else:
            raise ValueError("Unknown Mimir lens '{}'".format(command_id))
        # Create Mimir lens
       
        mimir_lens_response = mimir.createLens(
            mimir_table_name,
            params,
            command_id.upper(),
            arguments.get_value(cmd.PARA_MATERIALIZE_INPUT, default_value=True),
            human_readable_name = ds_name.upper()
        )
        lens_name = mimir_lens_response['lensName']
        lens_schema = mimir_lens_response['schema']

        columns = [
            MimirDatasetColumn(
                identifier = col_id,
                name_in_dataset = col["name"],
                data_type = col["type"]
            )
            for (col, col_id) in zip(lens_schema, range(0, len(lens_schema)))
        ]
        ds = context.datastore.register_dataset(
            table_name=lens_name,
            columns=columns,
            annotations=dataset.annotations
        )
        if store_as_dataset is not None:
            ds_name = store_as_dataset

        if command_id in LENSES_THAT_SHOULD_NOT_DISPLAY_TABLES:
            print_dataset_schema(outputs, ds_name, columns)
        else:
            ds_output = server.api.datasets.get_dataset(
                project_id=context.project_id,
                dataset_id=ds.identifier,
                offset=0,
                limit=10
            )
            outputs.stdout.append(DatasetOutput(ds_output))
        
        dsd = DatasetDescriptor(
                identifier=ds.identifier,
                columns=ds.columns,
                row_count=ds.row_count
            )
        result_resources = dict()
        result_resources[base.RESOURCE_DATASET] = ds.identifier
                
        # Return task result
        return ExecResult(
            outputs=outputs,
            provenance=ModuleProvenance(
                read={input_ds_name: dataset.identifier},
                write={ds_name: dsd},
                resources=result_resources
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
        try:
            annotations = int(float(annotations))
        except:
            annotations = 0
        if annotations > 0:
            outputs.stdout.append(TextOutput('Repairs in first 200 rows:'))
            outputs.stdout.append(TextOutput(str(annotations)))
