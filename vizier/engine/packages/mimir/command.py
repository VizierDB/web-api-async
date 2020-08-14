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

"""Create instances of mimir package commands."""

import vizier.viztrail.command as md
import vizier.engine.packages.base as pckg
import vizier.engine.packages.mimir.base as mimir


def mimir_geocode(
    dataset_name, geocoder, house_nr=None, street=None, city=None, state=None,
    materialize_input=False, validate=False
):
    """Create instance of mimir missing value lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    materialize_input: bool, optional
        Flag indicating whether input should be materialized
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    arguments =[
        md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
        md.ARG(id=mimir.PARA_GEOCODER, value=geocoder),
        md.ARG(id=mimir.PARA_MATERIALIZE_INPUT, value=materialize_input)
    ]
    # Add optional arguments if given
    if not house_nr is None:
        arguments.append(md.ARG(id=mimir.PARA_HOUSE_NUMBER, value=house_nr))
    if not street is None:
        arguments.append(md.ARG(id=mimir.PARA_STREET, value=street))
    if not city is None:
        arguments.append(md.ARG(id=mimir.PARA_CITY, value=city))
    if not state is None:
        arguments.append(md.ARG(id=mimir.PARA_STATE, value=state))
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_GEOCODE,
        arguments=arguments,
        packages=PACKAGE(validate=validate)
    )


def mimir_key_repair(dataset_name, column, materialize_input=False, validate=False):
    """Create instance of mimir key repair lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    materialize_input: bool, optional
        Flag indicating whether input should be materialized
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_KEY_REPAIR,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=pckg.PARA_COLUMN, value=column),
            md.ARG(id=mimir.PARA_MATERIALIZE_INPUT, value=materialize_input)
        ],
        packages=PACKAGE(validate=validate)
    )


def mimir_missing_key(dataset_name, column, materialize_input=False, validate=False):
    """Create instance of mimir missing key lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    missing_only: boolean, optional
        Optional MISSING_ONLY parameter
    materialize_input: bool, optional
        Flag indicating whether input should be materialized
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_MISSING_KEY,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=pckg.PARA_COLUMN, value=column),
            md.ARG(id=mimir.PARA_MATERIALIZE_INPUT, value=materialize_input)
        ],
        packages=PACKAGE(validate=validate)
    )


def mimir_missing_value(dataset_name, columns, materialize_input=False, validate=False):
    """Create instance of mimir missing value lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    columns: list(dict)
        List of dictionaries containing at least entry 'column' and optional
        'constraint'
    constraint: string, optional
        Optional value constraint
    materialize_input: bool, optional
        Flag indicating whether input should be materialized
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    column_list = list()
    for col in columns:
        col_arg = [md.ARG(id=pckg.PARA_COLUMN, value=col['column'])]
        if 'constraint' in col:
            col_arg.append(md.ARG(id=mimir.PARA_COLUMNS_CONSTRAINT, value=col['constraint']))
        column_list.append(col_arg)
    arguments =[
        md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
        md.ARG(id=mimir.PARA_COLUMNS, value=column_list),
        md.ARG(id=mimir.PARA_MATERIALIZE_INPUT, value=materialize_input)
    ]
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_MISSING_VALUE,
        arguments=arguments,
        packages=PACKAGE(validate=validate)
    )


def mimir_picker(
    dataset_name, schema, pick_as=None, materialize_input=False, validate=False
):
    """Create instance of mimir picker lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    schema: list(dict)
        List of dictionaries containing 'pickFrom'
        elements
    pick_as: string, optional
        Optional output column name
    materialize_input: bool, optional
        Flag indicating whether input should be materialized
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    elements = list()
    for col in schema:
        elements.append([md.ARG(id=mimir.PARA_PICKFROM, value=col['pickFrom'])])
    arguments =[
        md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
        md.ARG(id=mimir.PARA_SCHEMA, value=elements),
        md.ARG(id=mimir.PARA_MATERIALIZE_INPUT, value=materialize_input)
    ]
    if not pick_as is None:
        arguments.append(md.ARG(id=mimir.PARA_PICKAS, value=pick_as))
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_PICKER,
        arguments=arguments,
        packages=PACKAGE(validate=validate)
    )


def mimir_type_inference(
    dataset_name, percent_conform, materialize_input=False, validate=False
):
    """Create instance of mimir type inference lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    percent_conform: float
        Percent that conforms
    materialize_input: bool, optional
        Flag indicating whether input should be materialized
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_TYPE_INFERENCE,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=mimir.PARA_PERCENT_CONFORM, value=percent_conform),
            md.ARG(id=mimir.PARA_MATERIALIZE_INPUT, value=materialize_input)
        ],
        packages=PACKAGE(validate=validate)
    )

def mimir_shape_detector(
    dataset_name, model_name, validate=False
):
    """Create instance of mimir type inference lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    model_name: float
        the mimir model name that gets created or compared
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_SHAPE_DETECTOR,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=mimir.PARA_MODEL_NAME, value=model_name)
        ],
        packages=PACKAGE(validate=validate)
    )

def mimir_comment(
    dataset_name, 
    comments, 
    result_columns, 
    materialize_input=False, 
    validate=False
):
    """Create instance of mimir comment lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    comments: list(dict)
        List of objects containing 'expression' and 'comment' elements
    comments: list(dict)
        List of objects containing 'column' elements for output
    materialize_input: bool, optional
        Flag indicating whether input should be materialized
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    comments = list()
    result_cols = list()
    
    for comment in comments:
        items = list()
        items.append(md.ARG(id=mimir.PARA_EXPRESSION, value=comment['expression']))
        items.append(md.ARG(id=mimir.PARA_COMMENT, value=comment['comment']))
        items.append(md.ARG(id=mimir.PARA_ROWID, value=comment['rowid']))
        comments.append(items)
    for col in result_columns:
        col_arg = [md.ARG(id=pckg.PARA_COLUMN, value=col['column'])]
        result_cols.append(col_arg)
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_COMMENT,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=mimir.PARA_COMMENTS, value=comments),
            md.ARG(id=mimir.PARA_RESULT_COLUMNS, value=result_cols),
            md.ARG(id=mimir.PARA_MATERIALIZE_INPUT, value=materialize_input)
        ],
        packages=PACKAGE(validate=validate)
    )
# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def PACKAGE(validate=False):
    """Depending on the validate flag return a package dictionary that contains
    the mimir package declaration or None.

    Parameters
    ----------
    validate: bool, optional

    Returns
    ------
    dict
    """
    if validate:
        return {mimir.PACKAGE_MIMIR: pckg.PackageIndex(mimir.MIMIR_LENSES)}
    else:
        return None
