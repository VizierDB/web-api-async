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
from .base import PARA_FILE, PARA_SCHEMA, PARA_SCHEMA_COLUMN, PARA_SCHEMA_TYPE
from .base import PARA_INFER_TYPES, PARA_DETECT_HEADERS, PARA_LOAD_DSE
from .base import PARA_UNLOAD_OPTIONS, PARA_LOAD_OPTIONS
from .base import PARA_LOAD_OPTION_KEY, PARA_LOAD_OPTION_VALUE, PARA_LOAD_FORMAT
from .base import PARA_UNLOAD_OPTION_KEY, PARA_UNLOAD_OPTION_VALUE, PARA_UNLOAD_FORMAT
from .base import VIZUAL_LOAD, VIZUAL_UNLOAD, VIZUAL_EMPTY_DS, VIZUAL_CLONE_DS

"""Global constants."""

# Package name
PACKAGE_DATA = 'data'
DATA_MATERIALIZE = 'materialize'

DATA_COMMANDS = pckg.package_declaration(
    identifier=PACKAGE_DATA,
    category="code",
    commands=[
        pckg.command_declaration(
            identifier=VIZUAL_LOAD,
            name='Load Dataset',
            suggest=False,
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
                        pckg.enum_value(value='orc', text='ORC'),
                        pckg.enum_value(value='org.vizierdb.publish.source', text='Published Dataset')
                    ],
                    index=2,
                    required=True
                ),
                pckg.parameter_declaration(
                    PARA_SCHEMA,
                    name='Schema (leave blank to guess)',
                    data_type=pckg.DT_LIST,
                    index=3,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_SCHEMA_COLUMN,
                    name='Column Name',
                    data_type=pckg.DT_STRING,
                    index=4,
                    parent=PARA_SCHEMA,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_SCHEMA_TYPE,
                    name='Data Type',
                    data_type=pckg.DT_STRING,
                    index=5,
                    parent=PARA_SCHEMA,
                    required=False,
                    values=[
                        pckg.enum_value(value=t, text=t, is_default=(t=="string"))
                        for t in [
                            "string",
                            "real",
                            "float",
                            "boolean",
                            "short",
                            "date",
                            "timestamp",
                            "int",
                            "long",
                            "byte"
                        ]
                    ]
                ),
                pckg.parameter_declaration(
                    PARA_INFER_TYPES,
                    name='Infer Types',
                    data_type=pckg.DT_BOOL,
                    index=6,
                    default_value=True,
                    hidden=True,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_DETECT_HEADERS,
                    name='File Has Headers',
                    data_type=pckg.DT_BOOL,
                    index=7,
                    default_value=True,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_LOAD_DSE,
                    name='Data Source Error Annotations',
                    data_type=pckg.DT_BOOL,
                    hidden=True,
                    index=8,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_LOAD_OPTIONS,
                    name='Load Options',
                    data_type=pckg.DT_LIST,
                    index=9,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_LOAD_OPTION_KEY,
                    name='Option Key',
                    data_type=pckg.DT_STRING,
                    index=10,
                    parent=PARA_LOAD_OPTIONS,
                    required=False
                ),
                pckg.parameter_declaration(
                    PARA_LOAD_OPTION_VALUE,
                    name='Option Value',
                    data_type=pckg.DT_STRING,
                    index=11,
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
            ],
        ),
        pckg.command_declaration(
            identifier=VIZUAL_EMPTY_DS,
            name='Empty Dataset',
            parameters=[
                pckg.parameter_declaration(
                    pckg.PARA_NAME,
                    name='Dataset Name',
                    data_type=pckg.DT_STRING,
                    required=True,
                    index=0
                )
            ],
            format=[
                pckg.constant_format('CREATE'),
                pckg.constant_format('EMPTY'),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_NAME)
            ],
            suggest=False
        ),
        pckg.command_declaration(
            identifier=VIZUAL_CLONE_DS,
            name='Clone Dataset',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    pckg.PARA_NAME,
                    name='Name of Copy',
                    data_type=pckg.DT_STRING,
                    required=True,
                    index=1
                )
            ],
            format=[
                pckg.constant_format('CLONE'),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('AS'),
                pckg.variable_format(pckg.PARA_NAME)
            ],
            suggest=False
        ),
        pckg.command_declaration(
            identifier=VIZUAL_UNLOAD,
            name='Export Dataset',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    PARA_UNLOAD_FORMAT,
                    name='Format',
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
                        pckg.enum_value(value='orc', text='ORC'),
                        pckg.enum_value(value='info.vizierdb.publish.local', text='Publish Locally')
                    ],
                    index=1,
                    required=True
                ),
                pckg.parameter_declaration(
                    PARA_UNLOAD_OPTIONS,
                    name='Options',
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
                pckg.constant_format('EXPORT'),
                pckg.variable_format(pckg.PARA_DATASET),
                pckg.constant_format('TO'),
                pckg.variable_format(PARA_UNLOAD_FORMAT)
            ],
            suggest=False
        ),
        pckg.command_declaration(
            identifier=DATA_MATERIALIZE,
            name='Materialize Dataset',
            parameters=[
                pckg.para_dataset(0),
            ],
            format=[
                pckg.constant_format('MATERIALIZE'),
                pckg.constant_format('DATASET'),
                pckg.variable_format(pckg.PARA_DATASET),
            ],
            suggest=False
        ),
    ]
)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------


def export_package(filename:str, format:str='YAML'):
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
        PACKAGE_DATA,
        DATA_COMMANDS,
        format=format
    )
