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

"""This module contains helper methods for the webservice that are used to
serialize projects.
"""
from typing import Optional, Dict, Any
import vizier.api.serialize.base as serialize
import vizier.api.serialize.branch as br
import vizier.api.serialize.hateoas as ref
import vizier.api.serialize.labels as labels
from vizier.engine.project.base import ProjectHandle
from vizier.api.routes.base import UrlFactory

def PROJECT_DESCRIPTOR(
        project: ProjectHandle, 
        urls: Optional[UrlFactory]
    ) -> Dict[str, Any]:
    """Dictionary serialization for project fundamental project metadata.

    Parameters
    ----------
    project : vizier.engine.project.base.ProjectHandle
        Project handle
    urls: vizier.api.routes.base.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    project_id = project.identifier
    ret = {
        'id': project_id,
        'createdAt': project.created_at.isoformat(),
        'lastModifiedAt': project.last_modified_at.isoformat(),
        'properties': project.viztrail.properties,
    }

    if project.viztrail.default_branch is not None:
        ret['defaultBranch'] = project.viztrail.default_branch.identifier
    
    if urls is not None:
        ret[labels.LINKS] = serialize.HATEOAS({
            ref.SELF: urls.get_project(project_id),
            ref.API_HOME: urls.service_descriptor(),
            ref.API_DOC: urls.api_doc(),
            ref.PROJECT_DELETE: urls.delete_project(project_id),
            ref.PROJECT_UPDATE: urls.update_project(project_id),
            ref.BRANCH_CREATE: urls.create_branch(project_id),
            ref.FILE_UPLOAD: urls.upload_file(project_id)
        })

    return ret


def PROJECT_HANDLE(
        project: ProjectHandle, 
        urls: Optional[UrlFactory], 
        deep_branch: bool   = False,
        deep_workflow: bool = False,
    ) -> Dict[str, Any]:
    """Dictionary serialization for project handle.

    Parameters
    ----------
    project : vizier.engine.project.base.ProjectHandle
        Project handle
    urls: vizier.api.routes.base.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    # Use project descriptor as the base
    obj = PROJECT_DESCRIPTOR(project, urls)
    # Add descriptors for project branches
    branches = list()
    for branch in project.viztrail.list_branches():
        if deep_branch:
            branches.append(
                br.BRANCH_HANDLE(
                    branch=branch,
                    project=project,
                    urls=urls,
                    deep_workflow = deep_workflow
                )
            )
        else:
            branches.append(
                br.BRANCH_DESCRIPTOR(
                    branch=branch,
                    project=project,
                    urls=urls
                )
            )
    obj['branches'] = branches
    return obj
