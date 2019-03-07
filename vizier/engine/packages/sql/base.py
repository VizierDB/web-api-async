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

"""Specification of parameters for SQL cell."""

import vizier.engine.packages.base as pckg


"""Global constants."""

# Package name
PACKAGE_SQL = 'sql'

# Command identifier (unique within the package)
SQL_QUERY = 'query'

# SQL command parameters
PARA_OUTPUT_DATASET = 'output_dataset'
PARA_SQL_SOURCE = 'source'


"""Define SQL command structure."""
SQL_COMMANDS = pckg.package_declaration(
    identifier=PACKAGE_SQL,
    commands=[
        pckg.command_declaration(
            identifier=SQL_QUERY,
            name='SQL Query',
            parameters=[
                pckg.parameter_declaration(
                    identifier=PARA_SQL_SOURCE,
                    name='SQL Code',
                    data_type=pckg.DT_CODE,
                    language='sql',
                    index=0
                ),
                pckg.parameter_declaration(
                    identifier=PARA_OUTPUT_DATASET,
                    name='Output Dataset',
                    data_type=pckg.DT_STRING,
                    index=1,
                    required=False
                )
            ],
            format=[
                pckg.variable_format(PARA_SQL_SOURCE),
                pckg.optional_format(PARA_OUTPUT_DATASET, prefix='AS ')
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
        PACKAGE_SQL,
        SQL_COMMANDS,
        format=format
    )
