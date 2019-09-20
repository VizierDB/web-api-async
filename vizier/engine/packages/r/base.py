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

"""Specification of parameters for R cell."""

import vizier.engine.packages.base as pckg


"""Global constants."""

# Package name
PACKAGE_R = 'R'

# Command identifier (unique within the package)
R_CODE = 'code'

# R source code parameter
PARA_R_SOURCE = 'source'


"""Define the r cell command structure."""
R_COMMANDS = pckg.package_declaration(
    identifier=PACKAGE_R,
    commands=[
        pckg.command_declaration(
            identifier=R_CODE,
            name='R Script',
            parameters=[
                pckg.parameter_declaration(
                    identifier=PARA_R_SOURCE,
                    name='R Code',
                    data_type=pckg.DT_CODE,
                    language='r',
                    index=0
                )
            ],
            format=[
                pckg.variable_format(PARA_R_SOURCE)
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
        PACKAGE_R,
        R_COMMANDS,
        format=format
    )
