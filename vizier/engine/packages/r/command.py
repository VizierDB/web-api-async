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

"""Create instances of r package commands."""

import vizier.engine.packages.base as pckg
import vizier.engine.packages.r.base as r
import vizier.viztrail.command as md


def r_cell(source, validate=False):
    """Get command specification for a R cell. Primarily intended for unit
    tests.

    Parameters
    ----------
    source: string
        R code for cell body
    validate: bool, optional
        If true, the command is validated

    Returns
    -------
    vizier.viztrail.command.ModuleCommand
    """
    # If the validate flag is true create a package index that contains the
    # r package declaration
    if validate:
        packages = {r.PACKAGE_R: pckg.PackageIndex(r.R_COMMANDS)}
    else:
        packages = None
    return md.ModuleCommand(
        r.PACKAGE_R,
        r.R_CODE,
        arguments=[md.ARG(id=r.PARA_R_SOURCE, value=source)],
        packages=packages
    )
