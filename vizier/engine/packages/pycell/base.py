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

"""Specification of parameters for Python cells."""

import vizier.engine.packages.base as pckg


"""Global constants."""

# Package name
PACKAGE_PYTHON = 'python'

# Command identifier (unique within the package)
PYTHON_CODE = 'code'

# Python source code parameter
PYTHON_SOURCE = 'source'


"""Define the python cell command structure."""
PYTHON_COMMANDS = pckg.package_declaration(
    identifier=PACKAGE_PYTHON,
    commands=[
        pckg.command_declaration(
            identifier=PYTHON_CODE,
            name='Python Script',
            parameters=[
                pckg.parameter_declaration(
                    identifier=PYTHON_SOURCE,
                    name='Python Code',
                    data_type=pckg.DT_CODE,
                    language='python',
                    index=0
                )
            ],
            format=[
                pckg.variable_format(PYTHON_SOURCE)
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
        PACKAGE_PYTHON,
        PYTHON_COMMANDS,
        format=format
    )
