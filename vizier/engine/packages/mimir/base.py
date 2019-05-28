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

#Identifier for Mimir lenses.
MIMIR_DOMAIN = 'domain'
MIMIR_GEOCODE = 'geocode'
MIMIR_KEY_REPAIR ='key_repair'
MIMIR_MISSING_KEY ='missing_key'
MIMIR_MISSING_VALUE = 'missing_value'
MIMIR_PICKER ='picker'
MIMIR_SCHEMA_MATCHING ='schema_matching'
MIMIR_TYPE_INFERENCE ='type_inference'
MIMIR_SHAPE_DETECTOR ='shape_watcher'

# Command arguments
PARA_CITY = 'city'
PARA_COLUMN_NAME='column'
PARA_COLUMNS = 'columns'
PARA_COLUMNS_CONSTRAINT = 'constraint'
PARA_GEOCODER = 'geocoder'
PARA_HOUSE_NUMBER = 'strnumber'
PARA_MAKE_CERTAIN = 'makeInputCertain'
PARA_PERCENT_CONFORM = 'percentConform'
PARA_PICKAS = 'pickAs'
PARA_PICKFROM = 'pickFrom'
PARA_RESULT_DATASET = 'resultName'
PARA_SCHEMA = 'schema'
PARA_STATE = 'state'
PARA_STREET = 'strname'
PARA_TYPE = 'type'
PARA_MODEL_NAME = 'modelName'


"""Mimir lens specification schema."""
def para_make_input_certain(index):
    """Return dictionary for 'makeInputCertain' parameter of Mimir lenses.

    Returns
    -------
    dict
    """
    return pckg.parameter_declaration(
        identifier=PARA_MAKE_CERTAIN,
        name='Make Input Certain',
        data_type=pckg.DT_BOOL,
        index=index,
        required=False
    )


MIMIR_LENSES = pckg.package_declaration(
    identifier=PACKAGE_MIMIR,
    commands=[
        pckg.command_declaration(
            identifier=MIMIR_DOMAIN,
            name='Domain Lens',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                para_make_input_certain(2)
            ],
            format=[
                pckg.constant_format('DOMAIN'),
                pckg.constant_format('FOR'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_GEOCODE,
            name='Geocode Lens',
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
                para_make_input_certain(6)
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
            name='Key Repair Lens',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                para_make_input_certain(2)
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
                para_make_input_certain(4)
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
            name='Missing Key Lens',
            parameters=[
                pckg.para_dataset(0),
                pckg.para_column(1),
                para_make_input_certain(2)
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
            name='Picker Lens',
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
                    name='Pick From',
                    data_type=pckg.DT_COLUMN_ID,
                    index=2,
                    parent=PARA_SCHEMA
                ),
                pckg.parameter_declaration(
                    identifier=PARA_PICKAS,
                    name='Pick As',
                    data_type=pckg.DT_STRING,
                    index=3,
                    required=False
                ),
                para_make_input_certain(4)
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
            name='Schema Matching Lens',
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
                para_make_input_certain(5)
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
            name='Type Inference Lens',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=PARA_PERCENT_CONFORM,
                    name='Percent Conform',
                    data_type=pckg.DT_DECIMAL,
                    index=1
                ),
                para_make_input_certain(2)
            ],
            format=[
                pckg.constant_format('TYPE'),
                pckg.constant_format('INFERENCE'),
                pckg.constant_format('FOR'),
                pckg.constant_format('COLUMNS'),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('WITH'),
                pckg.constant_format('percent_conform'),
                pckg.constant_format('='),
                pckg.variable_format(PARA_PERCENT_CONFORM)
            ]
        ),
        pckg.command_declaration(
            identifier=MIMIR_SHAPE_DETECTOR,
            name='Shape Detector Adaptive Schema',
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
