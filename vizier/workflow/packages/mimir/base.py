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

"""Declaration of lenses in the Mimir package."""

import vizier.workflow.packages.base as pckg


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

# Command arguments
PARA_CITY = 'city'
PARA_CONSTRAINT = 'constraint'
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
                        pckg.enum_value('GOOGLE'),
                        pckg.enum_value('OSM')
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
                pckg.para_column(1),
                pckg.parameter_declaration(
                    identifier=PARA_CONSTRAINT,
                    name='Constraint',
                    data_type=pckg.DT_STRING,
                    index=2,
                    required=False
                ),
                para_make_input_certain(3)
            ],
            format=[
                pckg.constant_format('MISSING'),
                pckg.constant_format('VALUES'),
                pckg.constant_format('FOR'),
                pckg.variable_format(pckg.PARA_COLUMN),
                pckg.constant_format('IN'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.optional_format(PARA_CONSTRAINT, prefix='WITH CONSTRAINT ')
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
                    identifier=pckg.PARA_COLUMN,
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
                        pckg.enum_value('int'),
                        pckg.enum_value('varchar')
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
                pckg.constant_format('('),
                pckg.group_format(
                    PARA_SCHEMA,
                    format=[
                        pckg.variable_format(pckg.PARA_COLUMN),
                        pckg.variable_format(PARA_TYPE)
                    ]
                ),
                pckg.constant_format(')'),
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


def mimir_domain(dataset_name, column, make_input_certain=False):
    """Create a Mimir Missing Value Lens.  Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_DOMAIN,
        {
            pckg.PARA_DATASET : dataset_name,
            pckg.PARA_COLUMN: column,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


def mimir_geocode(
    dataset_name, geocoder, house_nr=None, street=None, city=None, state=None,
    make_input_certain=False
):
    """Create a Mimir Missing Value Lens. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    args = {
        pckg.PARA_DATASET : dataset_name,
        PARA_GEOCODER: geocoder,
        PARA_MAKE_CERTAIN: make_input_certain
    }
    if not house_nr is None:
        args[PARA_HOUSE_NUMBER] = house_nr
    if not street is None:
        args[PARA_STREET] = street
    if not city is None:
        args[PARA_CITY] = city
    if not state is None:
        args[PARA_STATE] = state
    return ModuleSpecification(PACKAGE_MIMIR, MIMIR_GEOCODE, args)


def mimir_key_repair(dataset_name, column, make_input_certain=False):
    """Create a Mimir Key Repair Lens. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_KEY_REPAIR,
        {
            pckg.PARA_DATASET : dataset_name,
            pckg.PARA_COLUMN: column,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


def mimir_missing_key(dataset_name, column, missing_only=None, make_input_certain=False):
    """Create a Mimir Missing Key Lens. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    missing_only: boolean, optional
        Optional MISSING_ONLY parameter
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_MISSING_KEY,
        {
            pckg.PARA_DATASET : dataset_name,
            pckg.PARA_COLUMN: column,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


def mimir_missing_value(dataset_name, column, constraint=None, make_input_certain=False):
    """Create a Mimir Missing Value Lens. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    constraint: string, optional
        Optional value constraint
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    args = {
        pckg.PARA_DATASET : dataset_name,
        pckg.PARA_COLUMN: column,
        PARA_MAKE_CERTAIN: make_input_certain
    }
    if not constraint is None:
        args[PARA_CONSTRAINT] = constraint
    return ModuleSpecification(PACKAGE_MIMIR, MIMIR_MISSING_VALUE, args)


def mimir_picker(dataset_name,  schema, pick_as=None, make_input_certain=False):
    """Create a Mimir Picker Lens. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    schema: list(dict)
        List of objects containing 'pickFrom' and 'pickAs' elements
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    args = {
        pckg.PARA_DATASET : dataset_name,
        PARA_SCHEMA: schema,
        PARA_MAKE_CERTAIN: make_input_certain
    }
    if not pick_as is None:
        args[PARA_PICKAS] = pick_as
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_PICKER,
        args
    )


def mimir_schema_matching(dataset_name, schema, result_name, make_input_certain=False):
    """Create a Mimir Schema Matching Lens. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    schema: list(dict)
        List of objects containing 'column' and 'type' elements
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_SCHEMA_MATCHING,
        {
            pckg.PARA_DATASET : dataset_name,
            PARA_SCHEMA: schema,
            PARA_RESULT_DATASET: result_name,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )


def mimir_type_inference(dataset_name, percent_conform, make_input_certain=False):
    """Create a Mimir Type Inference Lens. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    percent_conform: float
        Percent that conforms
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_MIMIR,
        MIMIR_TYPE_INFERENCE,
        {
            pckg.PARA_DATASET : dataset_name,
            PARA_PERCENT_CONFORM: percent_conform,
            PARA_MAKE_CERTAIN: make_input_certain
        }
    )
