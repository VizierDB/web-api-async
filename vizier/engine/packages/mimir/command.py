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


def mimir_domain(dataset_name, column, make_input_certain=False, validate=False):
    """Create instance of missing value lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_DOMAIN,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=pckg.PARA_COLUMN, value=column),
            md.ARG(id=mimir.PARA_MAKE_CERTAIN, value=make_input_certain)
        ],
        packages=PACKAGE(validate=validate)
    )


def mimir_geocode(
    dataset_name, geocoder, house_nr=None, street=None, city=None, state=None,
    make_input_certain=False, validate=False
):
    """Create instance of mimir missing value lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    arguments =[
        md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
        md.ARG(id=mimir.PARA_GEOCODER, value=geocoder),
        md.ARG(id=mimir.PARA_MAKE_CERTAIN, value=make_input_certain)
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


def mimir_key_repair(dataset_name, column, make_input_certain=False, validate=False):
    """Create instance of mimir key repair lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain
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
            md.ARG(id=mimir.PARA_MAKE_CERTAIN, value=make_input_certain)
        ],
        packages=PACKAGE(validate=validate)
    )


def mimir_missing_key(dataset_name, column, make_input_certain=False, validate=False):
    """Create instance of mimir missing key lens command.

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
            md.ARG(id=mimir.PARA_MAKE_CERTAIN, value=make_input_certain)
        ],
        packages=PACKAGE(validate=validate)
    )


def mimir_missing_value(dataset_name, columns, make_input_certain=False, validate=False):
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
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain
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
        md.ARG(id=mimir.PARA_MAKE_CERTAIN, value=make_input_certain)
    ]
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_MISSING_VALUE,
        arguments=arguments,
        packages=PACKAGE(validate=validate)
    )


def mimir_picker(
    dataset_name,  schema, pick_as=None, make_input_certain=False, validate=False
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
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain
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
        md.ARG(id=mimir.PARA_MAKE_CERTAIN, value=make_input_certain)
    ]
    if not pick_as is None:
        arguments.append(md.ARG(id=mimir.PARA_PICKAS, value=pick_as))
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_PICKER,
        arguments=arguments,
        packages=PACKAGE(validate=validate)
    )


def mimir_schema_matching(
    dataset_name, schema, result_name, make_input_certain=False, validate=False
):
    """Create instance of mimir schema matching lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    schema: list(dict)
        List of objects containing 'column' and 'type' elements
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.viztrail.module.ModuleCommand
    """
    elements = list()
    for col in schema:
        items = list()
        items.append(md.ARG(id=mimir.PARA_COLUMN_NAME, value=col['column']))
        items.append(md.ARG(id=mimir.PARA_TYPE, value=col['type']))
        elements.append(items)
    return md.ModuleCommand(
        mimir.PACKAGE_MIMIR,
        mimir.MIMIR_SCHEMA_MATCHING,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=mimir.PARA_SCHEMA, value=elements),
            md.ARG(id=mimir.PARA_RESULT_DATASET, value=result_name),
            md.ARG(id=mimir.PARA_MAKE_CERTAIN, value=make_input_certain)
        ],
        packages=PACKAGE(validate=validate)
    )


def mimir_type_inference(
    dataset_name, percent_conform, make_input_certain=False, validate=False
):
    """Create instance of mimir type inference lens command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    percent_conform: float
        Percent that conforms
    make_input_certain: bool, optional
        Flag indicating whether input should be made certain
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
            md.ARG(id=mimir.PARA_MAKE_CERTAIN, value=make_input_certain)
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
