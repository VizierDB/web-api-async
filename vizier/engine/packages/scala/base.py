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

"""Specification of parameters for Scala cell."""

import vizier.engine.packages.base as pckg


"""Global constants."""

# Package name
PACKAGE_SCALA = 'scala'

# Command identifier (unique within the package)
SCALA_CODE = 'code'

# Scala source code parameter
PARA_SCALA_SOURCE = 'source'


"""Define the scala cell command structure."""
SCALA_COMMANDS = pckg.package_declaration(
    identifier=PACKAGE_SCALA,
    commands=[
        pckg.command_declaration(
            identifier=SCALA_CODE,
            name='Scala Script',
            parameters=[
                pckg.parameter_declaration(
                    identifier=PARA_SCALA_SOURCE,
                    name='Scala Code',
                    data_type=pckg.DT_CODE,
                    language='scala',
                    index=0
                )
            ],
            format=[
                pckg.variable_format(PARA_SCALA_SOURCE)
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
        PACKAGE_SCALA,
        SCALA_COMMANDS,
        format=format
    )
