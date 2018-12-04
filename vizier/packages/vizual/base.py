# Copyright (C) 2018 New York University
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

"""Specification of parameters for VizUAL commands."""

import vizier.packages.base as pckg


"""Global constants."""

# Package name
PACKAGE_VIZUAL = 'vizual'

# Package-specific identifier for VizUAL commands
VIZUAL_DEL_COL = 'DELETE_COLUMN'
VIZUAL_DEL_ROW = 'DELETE_ROW'
VIZUAL_DROP_DS = 'DROP_DATASET'
VIZUAL_INS_COL = 'INSERT_COLUMN'
VIZUAL_INS_ROW = 'INSERT_ROW'
VIZUAL_LOAD = 'LOAD'
VIZUAL_MOV_COL = 'MOVE_COLUMN'
VIZUAL_MOV_ROW = 'MOVE_ROW'
VIZUAL_PROJECTION = 'PROJECTION'
VIZUAL_REN_COL = 'RENAME_COLUMN'
VIZUAL_REN_DS = 'RENAME_DATASET'
VIZUAL_SORT = 'SORT_DATASET'
VIZUAL_UPD_CELL = 'UPDATE_CELL'

# VizUAL command arguments
PARA_COLUMNS = 'columns'
PARA_FILE = 'file'
PARA_FILEID = 'fileid'
PARA_ORDER = 'order'
PARA_POSITION = 'position'
PARA_ROW = 'row'
PARA_VALUE = 'value'
# Concatenation of parameter keys
PARA_COLUMNS_COLUMN = PARA_COLUMNS + '_' + pckg.PARA_COLUMN
PARA_COLUMNS_ORDER = PARA_COLUMNS + '_' + PARA_ORDER
PARA_COLUMNS_RENAME = PARA_COLUMNS + '_' + pckg.PARA_NAME

#Values for sort order.
SORT_ASC = 'A-Z'
SORT_DESC = 'Z-A'


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


def para_row(index):
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
    module_name='',
    class_name='',
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
                para_row(1)
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
                para_row(1),
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
                        pckg.enum_value(SORT_ASC, is_default=True),
                        pckg.enum_value(SORT_DESC)
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
                para_row(2),
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
                pckg.constant_format(','),
                pckg.variable_format(PARA_ROW),
                pckg.constant_format(']', lspace=False),
                pckg.constant_format('='),
                pckg.variable_format(PARA_VALUE)
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
