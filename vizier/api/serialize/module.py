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


def MODULE_HANDLE(project, branch, module, urls):
    """Dictionary serialization for a handle in the workflow at the branch
    head.

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
            'self': urls.get_workflow_module(
                project_id=project_id,
                branch_id=branch_id,
                module_id=module_id
            ),
            'cancel': urls.cancel_workflow(
                project_id=project_id,
                branch_id=branch_id
            ),
            'branch:head': urls.get_branch_head(
                project_id=project_id,
                branch_id=branch_id
            )
        })
    }
    if not module.is_active:
        obj['outputs'] = {
            'stdout': [serialize.OUTPUT(out) for out in module.outputs.stdout],
            'stderr': [serialize.OUTPUT(out) for out in module.outputs.stderr]
        }
    if not timestamp.started_at is None:
        obj['timestamps']['startedAt'] = timestamp.started_at.isoformat()
    if not timestamp.finished_at is None:
        obj['timestamps']['finishedAt'] = timestamp.finished_at.isoformat()
    return obj
