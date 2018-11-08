# Copyright (C) 2018 New York University,
#                    University at Buffalo,
#                    Illinois Institute of Technology.
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

"""Create instances of plot package commands."""

import vizier.workflow.module.command as md
import vizier.workflow.packages.base as pckg
import vizier.workflow.packages.vizual.base as vizual


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
    vizier.workflow.module.command.ModuleCommand
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
    vizier.workflow.module.command.ModuleCommand
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
    vizier.workflow.module.ModuleSpecification
    """
    return md.ModuleCommand(
        vizual.PACKAGE_VIZUAL,
        vizual.VIZUAL_DROP_DS,
        arguments =[
            md.ARG(id=pckg.PARA_DATASET, value=dataset_name)
        ],
        packages=PACKAGE(validate=validate)
    )




def insert_column(dataset_name, position, name):
    """Insert a column into a dataset. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    position: int
        Index position where column is inserted
    name: string
        New column name

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_INS_COL,
        {
            pckg.PARA_DATASET : dataset_name,
            PARA_POSITION: position,
            pckg.PARA_NAME: name
        }
    )


def insert_row(dataset_name, position):
    """Insert a row into a dataset. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    position: int
        Index position where row is inserted

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_INS_ROW,
        {
            pckg.PARA_DATASET : dataset_name,
            PARA_POSITION: position
        }
    )


def load_dataset(file_id, dataset_name, filename=None, url=None):
    """Load dataset from file. Expects file identifier and new dataset name.
    Primarily intended for unit tests.

    Parameters
    ----------
    file_id: string
        Unique file identifier
    dataset_name: string
        Name for the new dataset
    filename: string, optional
        Optional name of the source file
    url: string, optional
        Optional Url of the source file

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    file = {'fileid': file_id}
    if not filename is None:
        file['filename'] = filename
    if not url is None:
        file['url'] = url
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_LOAD,
        {
            PARA_FILE : file,
            pckg.PARA_NAME: dataset_name
        }
    )


def move_column(dataset_name, column, position):
    """Move a column in a dataset. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column that is being moves
    position: int
        Index position where column is moved to

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_MOV_COL,
        {
            pckg.PARA_DATASET : dataset_name,
            pckg.PARA_COLUMN: column,
            PARA_POSITION: position
        }
    )


def move_row(dataset_name, row, position):
    """Move a row in a dataset. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    row: int
        Index of row that is being moved
    position: int
        Index position where row is moved

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_MOV_ROW,
        {
            pckg.PARA_DATASET : dataset_name,
            PARA_ROW: row,
            PARA_POSITION: position
        }
    )


def rename_column(dataset_name, column, name):
    """Rename a dataset column. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Name or index for column that is being renamed
    name: string
        New column name

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_REN_COL,
        {
            pckg.PARA_DATASET : dataset_name,
            pckg.PARA_COLUMN: column,
            pckg.PARA_NAME: name
        }
    )


def rename_dataset(dataset_name, new_name):
    """Rename a dataset. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    new_name: string
        New dataset name

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_REN_DS,
        {
            pckg.PARA_DATASET : dataset_name,
            pckg.PARA_NAME: new_name
        }
    )


def update_cell(dataset_name, column, row, value):
    """Update a dataset cell value. Primarily intended for unit tests.

    Parameters
    ----------
    dataset_name: string
        Name of the dataset
    column: string or int
        Cell Columne name or index
    row: int
        Cell row index
    value: string
        New cell value

    Returns
    -------
    vizier.workflow.module.ModuleSpecification
    """
    return ModuleSpecification(
        PACKAGE_VIZUAL,
        VIZUAL_UPD_CELL,
        {
            pckg.PARA_DATASET : dataset_name,
            pckg.PARA_COLUMN: column,
            PARA_ROW: row,
            'value': value
        }
    )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def PACKAGE(validate=False):
    """Depending on the validate flag return a package dictionary that contains
    the VizUAL package declaration or None.

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
