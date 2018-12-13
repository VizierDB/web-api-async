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
import vizier.api.serialize.branch as branches


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
            'project:update': urls.update_project(project_id)
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
    project_id = project.identifier
    return {
        'id': project_id,
        'createdAt': project.created_at.isoformat(),
        'lastModifiedAt': project.last_modified_at.isoformat(),
        'properties': ANNOTATIONS(project.viztrail.properties),
        'branches': [
            branches.BRANCH_DESCRIPTOR(branch, urls)
                for branch in project.viztrail.list_branches()
        ],
        'links': HATEOAS({
            'self': urls.get_project(project_id),
            'project:delete': urls.delete_project(project_id),
            'project:update': urls.update_project(project_id)
        })
    }


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def ANNOTATIONS(annotations):
    """Serialize the content of an object annotation set.

    Parameters
    ----------
    annotations: vizier.core.annotation.base.ObjectAnnotationSet
        Set of object annotations

    Returns
    -------
    dict()
    """
    values = annotations.values()
    return [{'key': key, 'value': values[key]} for key in values]


def HATEOAS(links):
    """Convert a dictionary of key,value pairs into a list of references. Each
    list element is a dictionary that contains a 'rel' and 'href' element.

    Parameters
    ----------
    links: dict()
        Dictionary where the key defines the relationship and the value is the
        url.

    Returns
    -------
    list()
    """
    return [{'rel': key, 'href': links[key]} for key in links]
