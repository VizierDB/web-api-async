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

"""Declaration of lenses in the Mimir package."""

import vizier.engine.packages.base as pckg


"""Global constants."""

# Package name
PACKAGE_MIMIR = 'mimir'

# Category name
CATEGORY_MIMIR = "data_m"

#Identifier for Mimir lenses.
MIMIR_DOMAIN          = 'domain'
MIMIR_GEOCODE         = 'geocode'
MIMIR_KEY_REPAIR      = 'repair_key'
MIMIR_MISSING_KEY     = 'missing_key'
MIMIR_MISSING_VALUE   = 'missing_value'
MIMIR_PICKER          = 'picker'
MIMIR_SCHEMA_MATCHING = 'schema_matching'
MIMIR_TYPE_INFERENCE  = 'type_inference'
MIMIR_SHAPE_DETECTOR  = 'shape_watcher'
MIMIR_COMMENT         = 'comment'
MIMIR_PIVOT           = 'pivot'
MIMIR_SHRED           = 'shred'

# Command arguments
PARA_CITY               = 'city'
PARA_COLUMN_NAME        = 'column'
PARA_OUTPUT_COLUMN      = 'output_col'
PARA_COLUMNS            = 'columns'
PARA_COLUMNS_CONSTRAINT = 'constraint'
PARA_GEOCODER           = 'geocoder'
PARA_HOUSE_NUMBER       = 'strnumber'
PARA_MATERIALIZE_INPUT  = 'materializeInput'
PARA_PERCENT_CONFORM    = 'percentConform'
PARA_PICKAS             = 'pickAs'
PARA_PICKFROM           = 'pickFrom'
PARA_RESULT_DATASET     = 'resultName'
PARA_SCHEMA             = 'schema'
PARA_STATE              = 'state'
PARA_STREET             = 'strname'
PARA_TYPE               = 'type'
PARA_MODEL_NAME         = 'modelName'
PARA_COMMENTS           = 'comments'
PARA_RESULT_COLUMNS     = 'resultColumns'
PARA_EXPRESSION         = 'expression'
PARA_COMMENT            = 'comment'
PARA_ROWID              = 'rowid'
PARA_KEY                = 'key'
PARA_KEYS               = 'keys'
PARA_VALUE              = 'value'
PARA_VALUES             = 'values'
PARA_INDEX              = 'index'
PARA_KEEP_ORIGINAL      = 'keepOriginal'

"""Mimir lens specification schema."""
def para_materialize_input(index):
    """Return dictionary for 'materializeInput' parameter of Mimir lenses.

    Returns
    -------
    dict
    """
    return pckg.parameter_declaration(
        identifier=PARA_MATERIALIZE_INPUT,
        name='Materialize Input',
        data_type=pckg.DT_BOOL,
        index=index,
        required=False,
        hidden=True
    )


MIMIR_LENSES = pckg.package_declaration(
    identifier=PACKAGE_MIMIR,
    category=CATEGORY_MIMIR,
    commands=[
        # pckg.command_declaration(
        #     identifier=MIMIR_DOMAIN,
        #     name='Domain Lens',
        #     parameters=[
        #         pckg.para_dataset(0),
        #         pckg.para_column(1),
        #         para_materialize_input(2)
        #     ],
        #     format=[
        #         pckg.constant_format('DOMAIN'),
        #         pckg.constant_format('FOR'),
        #         pckg.variable_format(pckg.PARA_COLUMN),
        #         pckg.constant_format('IN'),
        #         pckg.variable_format(pckg.PARA_DATASET)
        #     ]
        # ),
        pckg.command_declaration(
            identifier=MIMIR_GEOCODE,
            name='Geocode',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_HOUSE_NUMBER,
                    name='House Nr.',
                    data_type=pckg.DT_COLUMN_ID,
                    index=1,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_STREET,
                    name='Street',
                    data_type=pckg.DT_COLUMN_ID,
                    index=2,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_CITY,
                    name='City',
                    data_type=pckg.DT_COLUMN_ID,
                    index=3,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_STATE,
                    name='State',
                    data_type=pckg.DT_COLUMN_ID,
                    index=4,
                    required=False
                ),
                 pckg.parameter_declaration(
                    identifier=PARA_GEOCODER,
                    name='Geocoder',
                    data_type=pckg.DT_STRING,
                    index=5,
                    values=[
                        pckg.enum_value(value='GOOGLE', is_default=True),
                        pckg.enum_value(value='OSM')
                    ]
                ),
                para_materialize_input(6)
            ],
            format=[
                pckg.constant_format('GEOCODE'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('COLUMNS'),
                pckg.optional_format(PARA_HOUSE_NUMBER, prefix='HOUSE_NUMBER='),
                pckg.optional_format(PARA_STREET, prefix='STREET='),
                pckg.optional_format(PARA_CITY, prefix='CITY='),
                pckg.optional_format(PARA_STATE, prefix='STATE='),
                pckg.constant_format('USING'),
                pckg.variable_format(PARA_GEOCODER)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_KEY_REPAIR,
            name='Fix Key Column',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                para_materialize_input(2)
            ],
            format=[
                pckg.constant_format('KEY'),
                pckg.constant_format('REPAIR'),
                pckg.constant_format('FOR'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_MISSING_VALUE,
            name='Missing Value Lens',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMNS,
                    name='Columns',
                    data_type=pckg.DT_LIST,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_COLUMN,
                    name='Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=2,
                    parent=PARA_COLUMNS
                ),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMNS_CONSTRAINT,
                    name='Constraint',
                    data_type=pckg.DT_STRING,
                    index=3,
                    parent=PARA_COLUMNS,
                    required=False
                ),
                para_materialize_input(4)
            ],
            format=[
                pckg.constant_format('MISSING'),
                pckg.constant_format('VALUES'),
                pckg.constant_format('FOR'),
                pckg.group_format(
                    PARA_COLUMNS,
                    format=[
                        pckg.variable_format(pckg.PARA_COLUMN),
                        pckg.optional_format(PARA_COLUMNS_CONSTRAINT, prefix='WITH CONSTRAINT ')
                    ]
                ),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET),
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_MISSING_KEY,
            name='Fix Sequence',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                para_materialize_input(2)
            ],
            format=[
                pckg.constant_format('MISSING'),
                pckg.constant_format('KEYS'),
                pckg.constant_format('FOR'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_PICKER,
            name='Merge Columns',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_SCHEMA,
                    name='Columns',
                    data_type=pckg.DT_LIST,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=PARA_PICKFROM,
                    name='Input',
                    data_type=pckg.DT_COLUMN_ID,
                    index=2,
                    parent=PARA_SCHEMA
                ),
                pckg.parameter_declaration(
                    identifier=PARA_PICKAS,
                    name='Output',
                    data_type=pckg.DT_STRING,
                    index=3,
                    required=False
                ),
                para_materialize_input(4)
            ],
            format=[
                pckg.constant_format('PICK'),
                pckg.constant_format('FROM'),
                pckg.group_format(
                    PARA_SCHEMA,
                    format=[pckg.variable_format(PARA_PICKFROM)]
                ),
                pckg.optional_format(PARA_PICKAS, prefix='AS '),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_SCHEMA_MATCHING,
            name='Match Target Schema',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_SCHEMA,
                    name='Schema',
                    data_type=pckg.DT_LIST,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMN_NAME,
                    name='Column Name',
                    data_type=pckg.DT_STRING,
                    index=2,
                    parent=PARA_SCHEMA
                ),
                pckg.parameter_declaration(
                    identifier=PARA_TYPE,
                    name='Data Type',
                    data_type=pckg.DT_STRING,
                    index=3,
                    values=[
                        pckg.enum_value(value='int', is_default=True),
                        pckg.enum_value(value='varchar')
                    ],
                    parent=PARA_SCHEMA
                ),
                pckg.parameter_declaration(
                    identifier=PARA_RESULT_DATASET,
                    name='Store Result As ...',
                    data_type=pckg.DT_STRING,
                    index=4
                ),
                para_materialize_input(5)
            ],
            format=[
                pckg.constant_format('SCHEMA'),
                pckg.constant_format('MATCHING'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('(', rspace=False),
                pckg.group_format(
                    PARA_SCHEMA,
                    format=[
                        pckg.variable_format(pckg.PARA_COLUMN),
                        pckg.variable_format(PARA_TYPE)
                    ]
                ),
                pckg.constant_format(')', lspace=False),
                pckg.constant_format('AS'),
                pckg.variable_format(PARA_RESULT_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_TYPE_INFERENCE,
            name='Detect Field Types',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_PERCENT_CONFORM,
                    name='Percent Conform',
                    data_type=pckg.DT_DECIMAL,
                    default_value=0.5,
                    index=1
                ),
                para_materialize_input(2)
            ],
            format=[
                pckg.constant_format('TYPE'),
                pckg.constant_format('INFERENCE'),
                pckg.constant_format('FOR'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('WITH'),
                pckg.constant_format('PERCENT'),
                pckg.constant_format('CONFORM'),
                pckg.variable_format(PARA_PERCENT_CONFORM)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_SHAPE_DETECTOR,
            name='Shape Detector',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_MODEL_NAME,
                    name='Model Name',
                    data_type=pckg.DT_STRING,
                    index=1,
                    required=False
                )
            ],
            format=[
                pckg.constant_format('SHAPE'),
                pckg.constant_format('DETECTOR'),
                pckg.constant_format('FOR'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('WITH'),
                pckg.constant_format('model_name'),
                pckg.constant_format('='),
                pckg.variable_format(PARA_MODEL_NAME)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_COMMENT,
            name='Comment Lens',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_COMMENTS,
                    name='Comments',
                    data_type=pckg.DT_LIST,
                    index=1
                ),
                    pckg.parameter_declaration(
                        identifier=PARA_EXPRESSION,
                        name='Column',
                        data_type=pckg.DT_COLUMN_ID,
                        index=2,
                        parent=PARA_COMMENTS,
                        required=False
                    ),
                    pckg.parameter_declaration(
                        identifier=PARA_COMMENT,
                        name='Comment',
                        data_type=pckg.DT_STRING,
                        index=3,
                        parent=PARA_COMMENTS
                    ),
                    pckg.parameter_declaration(
                        identifier=PARA_ROWID,
                        name='Row or Condition',
                        data_type=pckg.DT_STRING,
                        index=4,
                        required=False,
                        parent=PARA_COMMENTS
                    ),
                para_materialize_input(5)
            ],
            format=[
                pckg.constant_format('COMMENT'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('(', rspace=False),
                pckg.group_format(
                    PARA_COMMENTS,
                    format=[
                        pckg.variable_format(PARA_EXPRESSION),
                        pckg.variable_format(PARA_COMMENT)
                    ]
                ),
                pckg.constant_format(')', lspace=False)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_PIVOT,
            name="Pivot",
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_COLUMN,
                    name='Pivot Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=PARA_VALUES,
                    name='Value Columns',
                    data_type=pckg.DT_LIST,
                    index=2
                ),
                pckg.parameter_declaration(
                    identifier=PARA_VALUE,
                    name='Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=3,
                    parent=PARA_VALUES
                ),
                pckg.parameter_declaration(
                    identifier=PARA_KEYS,
                    name='Key Columns',
                    data_type=pckg.DT_LIST,
                    index=4,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_KEY,
                    name='Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=5,
                    parent=PARA_KEYS
                ),
                pckg.parameter_declaration(
                    identifier=PARA_RESULT_DATASET,
                    name='Output (if different)',
                    data_type=pckg.DT_STRING,
                    index=6,
                    required=False
                ),
            ],
            format=[
                pckg.constant_format("PIVOT"),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format("ON"),
                pckg.variable_format(PARA_COLUMN_NAME)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_SHRED,
            name="Shred",
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMN_NAME,
                    name='Input Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=1
                ),
                pckg.parameter_declaration(
                    identifier=PARA_COLUMNS,
                    name='Rules',
                    data_type=pckg.DT_LIST,
                    index=2
                ),
                pckg.parameter_declaration(
                    identifier=PARA_OUTPUT_COLUMN,
                    name='Output Column',
                    data_type=pckg.DT_STRING,
                    index=3,
                    required=False,
                    parent=PARA_COLUMNS
                ),
                pckg.parameter_declaration(
                    identifier=PARA_TYPE,
                    name='Rule',
                    data_type=pckg.DT_STRING,
                    index=4,
                    values=[
                        pckg.enum_value(value="field"    , text="Delimited Field", is_default = True),
                        pckg.enum_value(value="pattern"  , text="Regular Expression"),
                        pckg.enum_value(value="explode"  , text="Explode Delimiter"),
                        pckg.enum_value(value="substring", text="Substring"),
                        pckg.enum_value(value="pass"     , text="Leave Unchanged")
                    ],
                    parent=PARA_COLUMNS
                ),
                pckg.parameter_declaration(
                    identifier=PARA_EXPRESSION,
                    name='Expression / Delimiter',
                    data_type=pckg.DT_STRING,
                    index=5,
                    required=False,
                    parent=PARA_COLUMNS
                ),
                pckg.parameter_declaration(
                    identifier=PARA_INDEX,
                    name='Group / Field (if needed)',
                    data_type=pckg.DT_INT,
                    index=6,
                    required=False,
                    default_value=1,
                    parent=PARA_COLUMNS
                ),
                pckg.parameter_declaration(
                    identifier=PARA_KEEP_ORIGINAL,
                    name='Keep Original Columns',
                    data_type=pckg.DT_BOOL,
                    default_value=False,
                    index=7
                ),
                pckg.parameter_declaration(
                    identifier=PARA_RESULT_DATASET,
                    name='Output Dataset (if different)',
                    data_type=pckg.DT_STRING,
                    index=8,
                    required=False
                )
            ],
            format=[
                pckg.constant_format("SHRED"),
                pckg.variable_format(pckg.PARA_DATASET),
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
        PACKAGE_MIMIR,
        MIMIR_LENSES,
        format=format
    )
