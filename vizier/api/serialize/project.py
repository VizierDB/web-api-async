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
serialize projects.
"""

import vizier.api.serialize.base as serialize
import vizier.api.serialize.branch as br


def PROJECT_DESCRIPTOR(project, urls):
    """Dictionary serialization for project fundamental project metadata.

    Parameters
    ----------
    project : vizier.engine.project.ProjectHandle
        Project handle
    urls: vizier.api.webservice.routes.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    project_id = project.identifier
    return {
        'id': project_id,
        'createdAt': project.created_at.isoformat(),
        'lastModifiedAt': project.last_modified_at.isoformat(),
        'properties': ANNOTATIONS(project.viztrail.properties),
        'links': HATEOAS({
            'self': urls.get_project(project_id),
            'project:delete': urls.delete_project(project_id),
            'project:update': urls.update_project(project_id),
            'file:upload': urls.file_upload(project_id)
        })
    }


def PROJECT_HANDLE(project, urls):
    """Dictionary serialization for project handle.

    Parameters
    ----------
    project : vizier.engine.project.ProjectHandle
        Project handle
    urls: vizier.api.webservice.routes.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    # Use project descriptor as the base
    obj = PROJECT_DESCRIPTOR(project, urls)
    # Ad descriptors for project branches
    branches = list()
    for branch in project.viztrail.list_branches()
        branches.append(
            br.BRANCH_DESCRIPTOR(
                branch=branch,
                urls=urls,
                project_id=project.identifier
            )
        )
    obj['branches'] = branches
    return obj
