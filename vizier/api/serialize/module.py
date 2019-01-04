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
import vizier.api.serialize.dataset as serialds
import vizier.api.serialize.labels as labels


def MODULE_HANDLE(project, branch, module, urls, include_self=True):
    """Dictionary serialization for a handle in the workflow at the branch
    head.

    The list of references will only contain a self referene if the include_self
    flag is True.

    Parameters
    ----------
    project: vizier.engine.project.base.ProjectHandle
        Handle for the containing project
    branch : vizier.viztrail.branch.BranchHandle
        Branch handle
    module: vizier.viztrail.module.base.ModuleHandle
        Module handle
    urls: vizier.api.routes.base.UrlFactory
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
        labels.TIMESTAMPS: {
            labels.CREATED_AT: timestamp.created_at.isoformat()
        },
        labels.LINKS: serialize.HATEOAS({
            'branch:head': urls.get_branch_head(
                project_id=project_id,
                branch_id=branch_id
            )
        })
    }
    if include_self:
        obj[labels.LINKS].extend(serialize.HATEOAS({
            'self': urls.get_workflow_module(
                project_id=project_id,
                branch_id=branch_id,
                module_id=module_id
            ),
            'module:delete': urls.get_workflow_module(
                project_id=project_id,
                branch_id=branch_id,
                module_id=module_id
            )
        }))
    if not timestamp.started_at is None:
        obj[labels.TIMESTAMPS][labels.STARTED_AT] = timestamp.started_at.isoformat()
    # Add outputs and datasets if module is not active. Else add cancel link
    if not module.is_active:
        datasets = list()
        for name in module.datasets:
            datasets.append(
                serialds.DATASET_IDENTIFIER(
                    identifier=module.datasets[name].identifier,
                    name=name
                )
            )
        obj[labels.DATASETS] = datasets
        obj[labels.OUTPUTS] = serialize.OUTPUTS(module.outputs)
        if not timestamp.finished_at is None:
            obj[labels.TIMESTAMPS][labels.FINISHED_AT] = timestamp.finished_at.isoformat()
    else:
        obj[labels.LINKS].extend(serialize.HATEOAS({
            'workflow:cancel': urls.cancel_workflow(
                project_id=project_id,
                branch_id=branch_id
            )
        }))
    return obj


def MODULE_HANDLE_LIST(project, branch, modules, urls):
    """Serialize a list of module handles into a dictionary.

    Parameters
    ----------
    project: vizier.engine.project.base.ProjectHandle
        Handle for the containing project
    branch : vizier.viztrail.branch.BranchHandle
        Branch handle
    modules: list(vizier.viztrail.module.base.ModuleHandle)
        Module handle
    urls: vizier.api.routes.base.UrlFactory
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


def PROVENANCE(prov):
    """Serialize a module provenance object.

    Parameters
    ----------
    proc: vizier.viztrail.module.provenance.ModuleProvenance
        Module provenance information

    Returns
    -------
    dict
    """
    obj = {}
    if not prov.read is None:
        obj['read'] = list()
        for name in prov.read:
            obj['read'].append({labels.ID: prov.read[name], labels.NAME: name})
    if not prov.write is None:
        obj['write'] = list()
        for name in prov.write:
            obj['write'].append({labels.ID: prov.write[name], labels.NAME: name})
    if not prov.delete is None:
        obj['delete'] = prov.delete
    if not prov.resources is None:
        obj['resources'] = prov.resources
    return obj
