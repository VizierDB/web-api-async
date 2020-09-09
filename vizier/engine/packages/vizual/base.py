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

"""Specification of parameters for VizUAL commands."""

import vizier.engine.packages.base as pckg


"""Global constants."""

# Package name
PACKAGE_DATA = 'data'
PACKAGE_VIZUAL = 'vizual'
PACKAGE_OPERATIONS = 'operations'

# Package-specific identifier for VizUAL commands
VIZUAL_DEL_COL = 'deleteColumn'
VIZUAL_DEL_ROW = 'deleteRow'
VIZUAL_DROP_DS = 'dropDataset'
VIZUAL_INS_COL = 'insertColumn'
VIZUAL_INS_ROW = 'insertRow'
VIZUAL_LOAD = 'load'
VIZUAL_EMPTY_DS = 'empty'
VIZUAL_CLONE_DS = 'clone'
VIZUAL_UNLOAD = 'unload'
VIZUAL_MOV_COL = 'moveColumn'
VIZUAL_MOV_ROW = 'moveRow'
VIZUAL_PROJECTION = 'projection'
VIZUAL_REN_COL = 'renameColumn'
VIZUAL_REN_DS = 'renameDataset'
VIZUAL_SORT = 'sortDataset'
VIZUAL_UPD_CELL = 'updateCell'
VIZUAL_DESCRIBE = 'describeDataset'
VIZUAL_STRING_CASE = 'changeCase'
VIZUAL_STRING_REPLACE = 'stringReplace'
VIZUAL_STRING_SPLIT = 'stringSplit'
VIZUAL_FILL_NULLS = 'fillNa'
VIZUAL_DROP_NULLS = 'dropNa'
VIZUAL_CONCAT = 'concat'
VIZUAL_ARITHMETIC = 'arithmetic'
VIZUAL_COLUMN_ARITHMETIC = 'columnArithmetic'
VIZUAL_FILTER_BY_VALUE = 'filterByValue'

# VizUAL command arguments
PARA_COLUMNS = 'columns'
PARA_DETECT_HEADERS = 'loadDetectHeaders'
PARA_FILE = 'file'
PARA_INFER_TYPES = 'loadInferTypes'
PARA_LOAD_FORMAT = 'loadFormat'
PARA_LOAD_OPTIONS = 'loadOptions'
PARA_LOAD_OPTION_KEY = 'loadOptionKey'
PARA_LOAD_OPTION_VALUE = 'loadOptionValue'
PARA_LOAD_DSE = 'loadDataSourceErrors'
PARA_UNLOAD_FORMAT = 'unloadFormat'
PARA_UNLOAD_OPTIONS = 'unloadOptions'
PARA_UNLOAD_OPTION_KEY = 'unloadOptionKey'
PARA_UNLOAD_OPTION_VALUE = 'unloadOptionValue'
PARA_ORDER = 'order'
PARA_POSITION = 'position'
PARA_ROW = 'row'
PARA_VALUE = 'value'
PARA_CASE = 'case'
PARA_NEW_VALUE = 'newValue'
PARA_LEFT_COLUMN = 'lColumn'
PARA_RIGHT_COLUMN = 'rColumn'
PARA_OPERATION = 'arithmeticOperation'

# Concatenation of parameter keys
PARA_COLUMNS_COLUMN = PARA_COLUMNS + '_' + pckg.PARA_COLUMN
PARA_COLUMNS_ORDER = PARA_COLUMNS + '_' + PARA_ORDER
PARA_COLUMNS_RENAME = PARA_COLUMNS + '_' + pckg.PARA_NAME

#Values for sort order.
SORT_ASC = 'ASC'
SORT_DESC = 'DESC'

#Values for changeCase
STR_CASE_UPPER = 'upper'
STR_CASE_LOWER = 'lower'
STR_CASE_TITLE = 'title'

#Values for dropNa
DROP_ALL = 'all'
DROP_ANY = 'any'

#Values for Arithmetics
OP_SUM = 'ADD'
OP_SUB = 'SUB'
OP_MUL = 'MUL'
OP_DIV = 'DIV'


"""VizUAL command specification schema."""
def para_position(index):
    """Return dictionary for position parameter used by some Vizual moduels.

    Returns
    -------
    dict
    """
    return pckg.parameter_declaration(
        PARA_POSITION,
        name='Position',
        data_type=pckg.DT_INT,
        index=index
    )


def para_row_id(index):
    """Return dictionary specifying the a row identifier parameter (currently
    used by the update cell command).

    Returns
    -------
    dict
    """
    return pckg.parameter_declaration(
        PARA_ROW,
        name='Row',
        data_type=pckg.DT_ROW_ID,
        index=index
    )


def para_row_index(index):
    """Return dictionary specifying the default row parameter used by most
    modules.

    Returns
    -------
    dict
    """
    return pckg.parameter_declaration(
        PARA_ROW,
        name='Row',
        data_type=pckg.DT_ROW_INDEX,
        index=index
    )


VIZUAL_COMMANDS = pckg.package_declaration(
    identifier=PACKAGE_VIZUAL,
    commands=[
        pckg.command_declaration(
            identifier=VIZUAL_DEL_COL,
            name='Delete Column',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1)
            ],
            format=[
                pckg.constant_format('DELETE'),
                pckg.constant_format('COLUMN'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('FROM'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_DEL_ROW,
            name='Delete Row',
            parameters=[
                pckg.para_dataset(0),
                para_row_index(1)
            ],
            format=[
                pckg.constant_format('DELETE'),
                pckg.constant_format('ROW'),
                pckg.variable_format(PARA_ROW),
                pckg.constant_format('FROM'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_DROP_DS,
            name='Drop Dataset',
            parameters=[
                pckg.para_dataset(0)
            ],
            format=[
                pckg.constant_format('DROP'),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_INS_COL,
            name='Insert Column',
            parameters=[
                pckg.para_dataset(0),
                para_position(2),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='Column Name',
                    data_type=pckg.DT_STRING,
                    index=1
                )
            ],
            format=[
                pckg.constant_format('INSERT'),
                pckg.constant_format('COLUMN'),
                pckg.variable_format(pckg.PARA_NAME),
                pckg.constant_format('INTO'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('AT'),
                pckg.constant_format('POSITION'),
                pckg.variable_format(PARA_POSITION)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_INS_ROW,
            name='Insert Row',
            parameters=[
                pckg.para_dataset(0),
                para_position(1)
            ],
            format=[
                pckg.constant_format('INSERT'),
                pckg.constant_format('ROW'),
                pckg.constant_format('INTO'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('AT'),
                pckg.constant_format('POSITION'),
                pckg.variable_format(PARA_POSITION)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_LOAD,
            name='Load Dataset',
            parameters=[
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=0
                ),
                pckg.parameter_declaration(
                    identifier=PARA_FILE,
                    name='Source File',
                    data_type=pckg.DT_FILE_ID,
                    index=1
                ),
                pckg.parameter_declaration(
                    PARA_LOAD_FORMAT,
                    name='Load Format',
                    data_type=pckg.DT_STRING,
                    values=[
                        pckg.enum_value(value='csv', text='CSV', is_default=True),
                        pckg.enum_value(value='json', text='JSON'),
                        pckg.enum_value(value='mimir.exec.spark.datasource.pdf', text='PDF'),
                        pckg.enum_value(value='mimir.exec.spark.datasource.google.spreadsheet', text='Google Sheet'),
                        pckg.enum_value(value='com.databricks.spark.xml', text='XML'),
                        pckg.enum_value(value='com.crealytics.spark.excel', text='Excel'),
                        pckg.enum_value(value='jdbc', text='JDBC Source'),
                        pckg.enum_value(value='text', text='Text'),
                        pckg.enum_value(value='parquet', text='Parquet'),
                        pckg.enum_value(value='orc', text='ORC')
                    ],
                    index=2,
                    required=True
                ),
                pckg.parameter_declaration(
                    PARA_INFER_TYPES,
                    name='Infer Types',
                    data_type=pckg.DT_STRING,
                    values=[
                        pckg.enum_value(value='none', text='Do not infer data types', is_default=True),
                        pckg.enum_value(value='datamartprofiler', text='Datamart Profiler'),
                        pckg.enum_value(value='mimirprofiler', text='Mimir Profiler')
                    ],
                    index=3,
                    required=True
                ),
                # pckg.parameter_declaration(
                #     PARA_INFER_TYPES,
                #     name='Infer Types',
                #     data_type=pckg.DT_BOOL,
                #     index=3,
                #     default_value=True,
                #     required=False
                # ),
                pckg.parameter_declaration(
                    PARA_DETECT_HEADERS,
                    name='Detect Headers',
                    data_type=pckg.DT_BOOL,
                    index=4,
                    default_value=True,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_LOAD_DSE,
                    name='Data Source Error Annotations',
                    data_type=pckg.DT_BOOL,
                    index=5,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_LOAD_OPTIONS,
                    name='Load Options',
                    data_type=pckg.DT_LIST,
                    index=6,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_LOAD_OPTION_KEY,
                    name='Option Key',
                    data_type=pckg.DT_STRING,
                    index=7,
                    parent=PARA_LOAD_OPTIONS,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_LOAD_OPTION_VALUE,
                    name='Option Value',
                    data_type=pckg.DT_STRING,
                    index=8,
                    parent=PARA_LOAD_OPTIONS,
                    required=False
                )
            ],
            format=[
                pckg.constant_format('LOAD'),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_NAME),
                pckg.constant_format('FROM'),
                pckg.variable_format(PARA_FILE)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_UNLOAD,
            name='Unload Dataset',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    PARA_UNLOAD_FORMAT,
                    name='Unload Format',
                    data_type=pckg.DT_STRING,
                    values=[
                        pckg.enum_value(value='csv', text='CSV', is_default=True),
                        pckg.enum_value(value='json', text='JSON'),
                        pckg.enum_value(value='mimir.exec.spark.datasource.google.spreadsheet', text='Google Sheet'),
                        pckg.enum_value(value='com.databricks.spark.xml', text='XML'),
                        pckg.enum_value(value='com.crealytics.spark.excel', text='Excel'),
                        pckg.enum_value(value='jdbc', text='JDBC Source'),
                        pckg.enum_value(value='text', text='Text'),
                        pckg.enum_value(value='parquet', text='Parquet'),
                        pckg.enum_value(value='orc', text='ORC')
                    ],
                    index=1,
                    required=True
                ),
                pckg.parameter_declaration(
                    PARA_UNLOAD_OPTIONS,
                    name='Unload Options',
                    data_type=pckg.DT_LIST,
                    index=2,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_UNLOAD_OPTION_KEY,
                    name='Option Key',
                    data_type=pckg.DT_STRING,
                    index=3,
                    parent=PARA_UNLOAD_OPTIONS,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_UNLOAD_OPTION_VALUE,
                    name='Option Value',
                    data_type=pckg.DT_STRING,
                    index=4,
                    parent=PARA_UNLOAD_OPTIONS,
                    required=False
                )
            ],
            format=[
                pckg.constant_format('UNLOAD'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('TO'),
                pckg.variable_format(PARA_UNLOAD_FORMAT)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_MOV_COL,
            name='Move Column',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                para_position(2)
            ],
            format=[
                pckg.constant_format('MOVE'),
                pckg.constant_format('COLUMN'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('TO'),
                pckg.constant_format('POSITION'),
                pckg.variable_format(PARA_POSITION),
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_MOV_ROW,
            name='Move Row',
            parameters=[
                pckg.para_dataset(0),
                para_row_index(1),
                para_position(2)
            ],
            format=[
                pckg.constant_format('MOVE'),
                pckg.constant_format('ROW'),
                pckg.variable_format(PARA_ROW),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('TO'),
                pckg.constant_format('POSITION'),
                pckg.variable_format(PARA_POSITION),
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_PROJECTION,
            name='Filter Columns',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMNS,
                    name='Columns',
                    data_type=pckg.DT_LIST,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMNS_COLUMN,
                    name='Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=2,
                    parent=PARA_COLUMNS
                ),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMNS_RENAME,
                    name='Rename as ...',
                    data_type=pckg.DT_STRING,
                    index=3,
                    parent=PARA_COLUMNS,
                    required=False
                )
            ],
            format=[
                pckg.constant_format('FILTER'),
                pckg.constant_format('COLUMNS'),
                pckg.group_format(
                    PARA_COLUMNS,
                    format=[pckg.variable_format(PARA_COLUMNS_COLUMN)]
                ),
                pckg.constant_format('FROM'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_REN_COL,
            name='Rename Column',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Column Name',
                    data_type=pckg.DT_STRING,
                    index=2
                )
            ],
            format=[
                pckg.constant_format('RENAME'),
                pckg.constant_format('COLUMN'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('TO'),
                pckg.variable_format(pckg.PARA_NAME)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_REN_DS,
            name='Rename Dataset',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=1
                )
            ],
            format=[
                pckg.constant_format('RENAME'),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('TO'),
                pckg.variable_format(pckg.PARA_NAME)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_SORT,
            name='Sort Dataset',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMNS,
                    name='Columns',
                    data_type=pckg.DT_LIST,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMNS_COLUMN,
                    name='Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=2,
                    parent=PARA_COLUMNS
                ),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMNS_ORDER,
                    name='Order',
                    data_type=pckg.DT_STRING,
                    index=3,
                    values=[
                        pckg.enum_value(value=SORT_ASC, text='A -> Z', is_default=True),
                        pckg.enum_value(value=SORT_DESC, text='Z -> A')
                    ],
                    parent=PARA_COLUMNS,
                    required=True
                )
            ],
            format=[
                pckg.constant_format('SORT'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('BY'),
                pckg.group_format(
                    PARA_COLUMNS,
                    format=[
                        pckg.variable_format(PARA_COLUMNS_COLUMN),
                        pckg.optional_format(
                            PARA_COLUMNS_ORDER,
                            prefix='(',
                            suffix=')'
                        )
                    ]
                )
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_UPD_CELL,
            name='Update Cell',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                para_row_id(2),
                pckg.parameter_declaration(
                    identifier=PARA_VALUE,
                    name='Value',
                    data_type=pckg.DT_SCALAR,
                    index=3,
                    required=False
                )
            ],
            format=[
                pckg.constant_format('UPDATE'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('SET'),
                pckg.constant_format('[', rspace=False),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format(',', lspace=False),
                pckg.variable_format(PARA_ROW),
                pckg.constant_format(']', lspace=False),
                pckg.constant_format('='),
                pckg.variable_format(PARA_VALUE)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_DESCRIBE,
            name='Describe',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=1
                )
            ],
            format=[
                pckg.constant_format('DESCRIBE'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_STRING_CASE,
            name='Change Case',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=2
                ),
                pckg.parameter_declaration(
                    identifier=PARA_CASE,
                    name='New Case',
                    data_type=pckg.DT_STRING,
                    index=3,
                    values=[
                        pckg.enum_value(value=STR_CASE_LOWER, text='Lower', is_default=True),
                        pckg.enum_value(value=STR_CASE_TITLE, text='Title'),
                        pckg.enum_value(value=STR_CASE_UPPER, text='Upper')
                    ],
                    required=True
                )
            ],
            format=[
                pckg.constant_format('UPDATE'),
                pckg.constant_format('COLUMN'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('CASE'),
                pckg.constant_format('TO'),
                pckg.variable_format(PARA_CASE)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_STRING_REPLACE,
            name='Replace',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=2
                ),
                pckg.parameter_declaration(
                    identifier=PARA_VALUE,
                    name='Current Value',
                    data_type=pckg.DT_STRING,
                    index=3
                ),
                pckg.parameter_declaration(
                    identifier=PARA_NEW_VALUE,
                    name='New Value',
                    data_type=pckg.DT_STRING,
                    index=4
                )
            ],
            format=[
                pckg.constant_format('REPLACE'),
                pckg.variable_format(PARA_VALUE),
                pckg.constant_format('IN'),
                pckg.constant_format('COLUMN'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('WITH'),
                pckg.variable_format(PARA_NEW_VALUE)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_STRING_SPLIT,
            name='Split',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=2
                ),
                pckg.parameter_declaration(
                    identifier=PARA_VALUE,
                    name='On',
                    data_type=pckg.DT_STRING,
                    index=3
                ),
            ],
            format=[
                pckg.constant_format('SPLIT'),
                pckg.constant_format('COLUMN'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('ON'),
                pckg.variable_format(PARA_VALUE)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_FILL_NULLS,
            name='Fill Nulls',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=2
                ),
                pckg.parameter_declaration(
                    identifier=PARA_VALUE,
                    name='Fill Value',
                    data_type=pckg.DT_STRING,
                    index=3
                ),
            ],
            format=[
                pckg.constant_format('FILL'),
                pckg.constant_format('NULLS'),
                pckg.constant_format('WITH'),
                pckg.variable_format(PARA_VALUE),
                pckg.constant_format('IN'),
                pckg.constant_format('COLUMN'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_DATASET),
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_DROP_NULLS,
            name='Drop Nulls',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=2
                ),
                pckg.parameter_declaration(
                    identifier=PARA_VALUE,
                    name='How',
                    data_type=pckg.DT_STRING,
                    index=3,
                    values=[
                        pckg.enum_value(value=DROP_ALL, text='All', is_default=True),
                        pckg.enum_value(value=DROP_ANY, text='Any'),
                    ],
                    required=True
                )
            ],
            format=[
                pckg.constant_format('DROP'),
                pckg.constant_format('NULLS'),
                pckg.constant_format('FROM'),
                pckg.constant_format('COLUMN'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_CONCAT,
            name='Concatenate',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_LEFT_COLUMN,
                    name='Left',
                    data_type=pckg.DT_COLUMN_ID,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=PARA_VALUE,
                    name='Delimiter',
                    data_type=pckg.DT_STRING,
                    index=2,
                    required=True
                ),
                pckg.parameter_declaration(
                    identifier=PARA_RIGHT_COLUMN,
                    name='Right',
                    data_type=pckg.DT_COLUMN_ID,
                    index=3
                ),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=4
                ),
            ],
            format=[
                pckg.constant_format('CONCATENATE'),
                pckg.variable_format(PARA_LEFT_COLUMN),
                pckg.constant_format('AND'),
                pckg.variable_format(PARA_RIGHT_COLUMN),
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_CONCAT,
            name='Concatenate',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_LEFT_COLUMN,
                    name='Left Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=PARA_VALUE,
                    name='Delimiter',
                    data_type=pckg.DT_STRING,
                    index=2,
                    required=True
                ),
                pckg.parameter_declaration(
                    identifier=PARA_RIGHT_COLUMN,
                    name='Right Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=3
                ),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=4
                ),
            ],
            format=[
                pckg.constant_format('CONCATENATE'),
                pckg.variable_format(PARA_LEFT_COLUMN),
                pckg.constant_format('AND'),
                pckg.variable_format(PARA_RIGHT_COLUMN),
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_ARITHMETIC,
            name='Arithmetic',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_OPERATION,
                    name='Operator',
                    data_type=pckg.DT_STRING,
                    values=[
                        pckg.enum_value(value=OP_SUM, text='Add', is_default=True),
                        pckg.enum_value(value=OP_SUB, text='Subtract'),
                        pckg.enum_value(value=OP_MUL, text='Multiply'),
                        pckg.enum_value(value=OP_DIV, text='Divide'),
                    ],
                    index=1,
                    required=True
                ),
                pckg.parameter_declaration(
                    identifier=PARA_LEFT_COLUMN,
                    name='Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=2
                ),
                pckg.parameter_declaration(
                    PARA_VALUE,
                    name='Value',
                    data_type=pckg.DT_SCALAR,
                    index=3,
                    required=True
                ),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=4
                ),
            ],
            format=[
                pckg.variable_format(PARA_OPERATION),
                pckg.variable_format(PARA_LEFT_COLUMN),
                pckg.constant_format('AND'),
                pckg.variable_format(PARA_VALUE)
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_COLUMN_ARITHMETIC,
            name='Column Arithmetic',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_OPERATION,
                    name='Operator',
                    data_type=pckg.DT_STRING,
                    values=[
                        pckg.enum_value(value=OP_SUM, text='Add', is_default=True),
                        pckg.enum_value(value=OP_SUB, text='Subtract'),
                        pckg.enum_value(value=OP_MUL, text='Multiply'),
                        pckg.enum_value(value=OP_DIV, text='Divide'),
                    ],
                    index=1,
                    required=True
                ),
                pckg.parameter_declaration(
                    identifier=PARA_LEFT_COLUMN,
                    name='Left Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=2
                ),
                pckg.parameter_declaration(
                    name='Right Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=3,
                    identifier=PARA_RIGHT_COLUMN,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=4
                ),
            ],
            format=[
                pckg.variable_format(PARA_OPERATION),
                pckg.variable_format(PARA_LEFT_COLUMN),
                pckg.constant_format('AND'),
                pckg.variable_format(PARA_RIGHT_COLUMN),
            ]
        ),
        pckg.command_declaration(
            identifier=VIZUAL_FILTER_BY_VALUE,
            name='Filter By Value',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_COLUMN,
                    name='Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=PARA_VALUE,
                    name='Values',
                    data_type=pckg.DT_LIST,
                    index=2,
                    required=True
                ),
                pckg.parameter_declaration(
                    identifier=PARA_NEW_VALUE,
                    name='Value',
                    data_type=pckg.DT_STRING,
                    index=3,
                    parent=PARA_VALUE,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='New Dataset Name',
                    data_type=pckg.DT_STRING,
                    index=4
                ),
            ],
            format=[
                pckg.constant_format('FILTER'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('ON'),
                pckg.group_format(
                    PARA_VALUE,
                    format=[pckg.variable_format(PARA_NEW_VALUE)]
                )
            ]
        )
    ]
)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------


def export_package(filename, format='YAML'):
    """Write package specification to the given file.

    Parameters
    ----------
    filename: string
        Name of the output file
    format: string, optional
        One of 'YAML' or 'JSON'
    """
    pckg.export_package(
        filename,
        PACKAGE_VIZUAL,
        VIZUAL_COMMANDS,
        format=format
    )
