# Copyright (C) 2017-2020 New York University,
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

"""parameterized error messages for server requests."""


def UNKNOWN_BRANCH(project_id, branch_id):
    """Error message for requests that access project branches.

    Parameters
    ----------
    project_id: string
        Unique project identifier.
    branch_id: string
        Unique branch identifier.

    Returns
    -------
    string
    """
    msg = "unknown branch '{}' or project '{}'"
    return msg.format(branch_id, project_id)


def UNKNOWN_DATASET(project_id, dataset_id):
    """Error message for requests that access datasets.

    Parameters
    ----------
    project_id: string
        Unique project identifier.
    dataset_id: string
        Unique dataset identifier.

    Returns
    -------
    string
    """
    msg = "unknown dataset '{}' or project '{}'"
    return msg.format(dataset_id, project_id)


def UNKNOWN_FILE(project_id, file_id):
    """Error message for requests that access project files.

    Parameters
    ----------
    project_id: string
        Unique project identifier.
    file_id: string
        Unique file identifier.

    Returns
    -------
    string
    """
    msg = "unknown file '{}' or project '{}'"
    return msg.format(file_id, project_id)


def UNKNOWN_PROJECT(project_id):
    """Error message for requests that access projects.

    Parameters
    ----------
    project_id: string
        Unique project identifier.

    Returns
    -------
    string
    """
    msg = "unknown project '{}'"
    return msg.format(project_id)


def UNKNOWN_MODULE(project_id, branch_id, module_id):
    """Error message for requests that access workflow modules.

    Parameters
    ----------
    project_id: string
        Unique project identifier.
    branch_id: string
        Unique branch identifier.
    module_id: int
        Unique module identifier.

    Returns
    -------
    string
    """
    msg = "unknown module '{}', branch '{}', or project '{}'"
    return msg.format(module_id, branch_id, project_id)


def UNKNOWN_WORKFLOW(project_id, branch_id, workflow_id):
    """Error message for requests that access workflows.

    Parameters
    ----------
    project_id: string
        Unique project identifier.
    branch_id: string
        Unique branch identifier.
    workflow_id: string
        Unique workflow identifier.

    Returns
    -------
    string
    """
    msg = "unknown workflow '{}', branch '{}', or project '{}'"
    return msg.format(workflow_id, branch_id, project_id)
