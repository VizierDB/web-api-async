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

"""Create instances of SQL package commands."""

import vizier.engine.packages.base as pckg
import vizier.engine.packages.sql.base as sql
import vizier.viztrail.command as md


def sql_cell(source, output_dataset=None, validate=False):
    """Get command specification for a SQL cell. Primarily intended for unit
    tests.

    Parameters
    ----------
    source: string
        SQL code for cell body
    output_dataset: string, optional
        Optional dataset name. If given result is materialized as new dataset.
    validate: bool, optional
        If true, the command is validated

    Returns
    -------
    vizier.viztrail.command.ModuleCommand
    """
    # If the validate flag is true create a package index that contains the
    # SQL package declaration
    if validate:
        packages = {sql.PACKAGE_SQL: pckg.PackageIndex(sql.SQL_COMMANDS)}
    else:
        packages = None
    arguments = [md.ARG(id=sql.PARA_SQL_SOURCE, value=source)]
    if not output_dataset is None:
        arguments.append(
            md.ARG(id=sql.PARA_OUTPUT_DATASET, value=output_dataset)
        )
    return md.ModuleCommand(
        sql.PACKAGE_SQL,
        sql.SQL_QUERY,
        arguments=arguments,
        packages=packages
    )
