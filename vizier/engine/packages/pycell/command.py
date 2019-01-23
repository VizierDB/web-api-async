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

"""Create instances of python package commands."""

import vizier.engine.packages.base as pckg
import vizier.engine.packages.pycell.base as pycell
import vizier.viztrail.command as md


def python_cell(source, validate=False):
    """Get command specification for a Python cell. Primarily intended for unit
    tests.

    Parameters
    ----------
    source: string
        Python code for cell body
    validate: bool, optional
        If true, the command is validated

    Returns
    -------
    vizier.viztrail.command.ModuleCommand
    """
    # If the validate flag is true create a package index that contains the
    # python cell package declaration
    if validate:
        packages = {pycell.PACKAGE_PYTHON: pckg.PackageIndex(pycell.PYTHON_COMMANDS)}
    else:
        packages = None
    return md.ModuleCommand(
        pycell.PACKAGE_PYTHON,
        pycell.PYTHON_CODE,
        arguments=[md.ARG(id=pycell.PYTHON_SOURCE, value=source)],
        packages=packages
    )
