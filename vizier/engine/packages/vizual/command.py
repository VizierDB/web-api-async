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

"""Create instances of vizual package commands."""

import vizier.engine.packages.base as pckg
import vizier.engine.packages.vizual.base as vizual
import vizier.viztrail.command as md


def delete_column(dataset_name, column, validate=False):
    """Create instance of delete dataset column command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: int
        Column identifier
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.command.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_DEL_COL,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=pckg.PARA_COLUMN, value=column)
        ],
        packages=PACKAGE(validate=validate)
    )


def delete_row(dataset_name, row, validate=False):
    """Create instance of delete dataset row command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    row: int
        Index for row that is being deleted
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.command.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_DEL_ROW,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=vizual.PARA_ROW, value=row)
        ],
        packages=PACKAGE(validate=validate)
    )


def drop_dataset(dataset_name, validate=False):
    """Create instance of drop a dataset command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_DROP_DS,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name)
        ],
        packages=PACKAGE(validate=validate)
    )


def insert_column(dataset_name, position, name, validate=False):
    """Create instance of insert column command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    position: int
        Index position where column is inserted
    name: string
        New column name
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_INS_COL,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=vizual.PARA_POSITION, value=position),
            md.ARG(id=pckg.PARA_NAME, value=name)
        ],
        packages=PACKAGE(validate=validate)
    )


def insert_row(dataset_name, position, validate=False):
    """Create instance of insert row command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    position: int
        Index position where row is inserted
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_INS_ROW,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=vizual.PARA_POSITION, value=position)
        ],
        packages=PACKAGE(validate=validate)
    )


def load_dataset(
    dataset_name, file, detect_headers=None, infer_types=None, load_format='csv',
    options=None, validate=False
):
    """Create instance of load dataset command.

    Parameters
    ----------
    dataset_name: string
        Name for the new dataset
    file: dict
        Dictionary containing at least one of 'fileId' or 'url' and optional
        'userName' and 'password'.
    detect_headers: bool, optional
        Detect column names in loaded file if True
    infer_types: bool, optional
        Infer column types for loaded dataset if True
    load_format: string, optional
        Format identifier
    options: list, optional
        Additional options for Mimirs load command
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    arguments = [
        md.ARG(id=vizual.PARA_FILE, value=file),
        md.ARG(id=pckg.PARA_NAME, value=dataset_name)
    ]
    if not detect_headers is None:
        arguments.append(
            md.ARG(id=vizual.PARA_DETECT_HEADERS, value=detect_headers)
        )
    if not infer_types is None:
        arguments.append(
            md.ARG(id=vizual.PARA_INFER_TYPES, value=infer_types)
        )
    if not load_format is None:
        arguments.append(
            md.ARG(id=vizual.PARA_LOAD_FORMAT, value=load_format)
        )
    if not options is None:
        arguments.append(
            md.ARG(id=vizual.PARA_LOAD_OPTIONS, value=options)
        )
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_LOAD,
        arguments=arguments,
        packages=PACKAGE(validate=validate)
    )


def move_column(dataset_name, column, position, validate=False):
    """Create instance of move column command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column that is being moves
    position: int
        Index position where column is moved to
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_MOV_COL,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=pckg.PARA_COLUMN, value=column),
            md.ARG(id=vizual.PARA_POSITION, value=position)
        ],
        packages=PACKAGE(validate=validate)
    )


def move_row(dataset_name, row, position, validate=False):
    """Create instance of move row command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    row: int
        Index of row that is being moved
    position: int
        Index position where row is moved
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_MOV_ROW,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=vizual.PARA_ROW, value=row),
            md.ARG(id=vizual.PARA_POSITION, value=position)
        ],
        packages=PACKAGE(validate=validate)
    )


def projection(dataset_name, columns, validate=False):
    """Create instance of projection command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    columns: list
        List of column references. Expects a list of dictionaries with two
        elements 'column' and 'name' (optional)
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    # Create list of projection columns. The output name for each column is
    # optional
    elements = list()
    for col in columns:
        items = list()
        items.append(md.ARG(id=vizual.PARA_COLUMNS_COLUMN, value=col['column']))
        if 'name' in col:
            items.append(md.ARG(id=vizual.PARA_COLUMNS_RENAME, value=col['name']))
        elements.append(items)
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_PROJECTION,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=vizual.PARA_COLUMNS, value=elements)
        ],
        packages=PACKAGE(validate=validate)
    )


def rename_column(dataset_name, column, name, validate=False):
    """Create instance of rename dataset column command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column that is being renamed
    name: string
        New column name
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_REN_COL,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=pckg.PARA_COLUMN, value=column),
            md.ARG(id=pckg.PARA_NAME, value=name)
        ],
        packages=PACKAGE(validate=validate)
    )


def rename_dataset(dataset_name, new_name, validate=False):
    """Create instance of rename dataset command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    new_name: string
        New dataset name
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_REN_DS,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=pckg.PARA_NAME, value=new_name)
        ],
        packages=PACKAGE(validate=validate)
    )


def sort_dataset(dataset_name, columns, validate=False):
    """Create instance of sort dataset command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    columns: list
        List of column references. Expects a list of dictionaries with two
        elements 'column' and 'order'
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    # Create list of projection columns. The output name for each column is
    # optional
    elements = list()
    for col in columns:
        items = list()
        items.append(md.ARG(id=vizual.PARA_COLUMNS_COLUMN, value=col['column']))
        items.append(md.ARG(id=vizual.PARA_COLUMNS_ORDER, value=col['order']))
        elements.append(items)
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_SORT,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=vizual.PARA_COLUMNS, value=elements)
        ],
        packages=PACKAGE(validate=validate)
    )


def update_cell(dataset_name, column, row, value, validate=False):
    """Create instance of update dataset cell command.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: int
        Cell column identifier
    row: int
        Unique row identifier for cell
    value: string
        New cell value
    validate: bool, optional
        Validate the created command specification (if true)

    Returns
    -------
    vizier.engine.module.ModuleCommand
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_UPD_CELL,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
            md.ARG(id=pckg.PARA_COLUMN, value=column),
            md.ARG(id=vizual.PARA_ROW, value=row),
            md.ARG(id=vizual.PARA_VALUE, value=value),
            ],
        packages=PACKAGE(validate=validate)
    )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def PACKAGE(validate=False):
    """Depending on the validate flag return a package dictionary that contains
    the vizual package declaration or None.

    Parameters
    ----------
    validate: bool, optional

    Returns
    ------
    dict
    """
    if validate:
        return {vizual.PACKAGE_VIZUAL: pckg.PackageIndex(vizual.VIZUAL_COMMANDS)}
    else:
        return None
