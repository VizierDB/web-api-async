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

"""This module contains helper methods for the webservice that are used to
serialize workflow modules.
"""

import vizier.api.serialize.base as serialize


def MODULE_HANDLE(project, branch, module, urls, include_self=True):
    """Dictionary serialization for a handle in the workflow at the branch
    head.

    The list of references will only contain a self referene if the include_self
    flag is True.

    Parameters
    ----------
    project: vizier.engine.project.ProjectHandle
        Handle for the containing project
    branch : vizier.viztrail.branch.BranchHandle
        Branch handle
    module: vizier.viztrail.module.base.ModuleHandle
        Module handle
    urls: vizier.api.webservice.routes.UrlFactory
        Factory for resource urls
    include_self: bool, optional
        Indicate if self link is included

    Returns
    -------
    dict
    """
    project_id = project.identifier
    branch_id = branch.identifier
    module_id = module.identifier
    cmd = module.command
    timestamp = module.timestamp
    obj = {
        'id': module_id,
        'state': module.state,
        'command': {
            'packageId': cmd.package_id,
            'commandId': cmd.command_id,
            'arguments': cmd.arguments.to_list()
        },
        'text': module.external_form,
        'timestamps': {
            'createdAt': timestamp.created_at.isoformat()
        },
        'links': serialize.HATEOAS({
            'branch:head': urls.get_branch_head(
                project_id=project_id,
                branch_id=branch_id
            )
        })
    }
    if include_self:
        obj['links'].extend(serialize.HATEOAS({
            'self': urls.get_workflow_module(
                project_id=project_id,
                branch_id=branch_id,
                module_id=module_id
            )
        }))
    if not timestamp.started_at is None:
        obj['timestamps']['startedAt'] = timestamp.started_at.isoformat()
    # Add outputs and datasets if module is not active. Else add cancel link
    if not module.is_active:
        obj['outputs'] = {
            'stdout': [serialize.OUTPUT(out) for out in module.outputs.stdout],
            'stderr': [serialize.OUTPUT(out) for out in module.outputs.stderr]
        }
        if not timestamp.finished_at is None:
            obj['timestamps']['finishedAt'] = timestamp.finished_at.isoformat()
    else:
        obj['links'].extend(serialize.HATEOAS({
            'cancel': urls.cancel_workflow(
                project_id=project_id,
                branch_id=branch_id
            )
        }))
    return obj


def MODULE_HANDLE_LIST(project, branch, modules, urls):
    """Serialize a list of module handles into a dictionary.

    Parameters
    ----------
    project: vizier.engine.project.ProjectHandle
        Handle for the containing project
    branch : vizier.viztrail.branch.BranchHandle
        Branch handle
    modules: list(vizier.viztrail.module.base.ModuleHandle)
        Module handle
    urls: vizier.api.webservice.routes.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    return {
        'modules': [
            MODULE_HANDLE(
                project=project,
                branch=branch,
                module=m,
                urls=urls
            ) for m in modules
        ]
    }