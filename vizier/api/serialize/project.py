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
import vizier.api.serialize.labels as labels


def PROJECT_DESCRIPTOR(project, urls):
    """Dictionary serialization for project fundamental project metadata.

    Parameters
    ----------
    project : vizier.engine.project.ProjectHandle
        Project handle
    urls: vizier.api.routes.UrlFactory
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
        'properties': serialize.ANNOTATIONS(project.viztrail.properties),
        labels.LINKS: serialize.HATEOAS({
            'self': urls.get_project(project_id),
            'home': urls.service_descriptor(),
            'doc': urls.api_doc(),
            'project:delete': urls.delete_project(project_id),
            'project:update': urls.update_project(project_id),
            'branch:create': urls.create_branch(project_id),
            'file:upload': urls.upload_file(project_id)
        })
    }


def PROJECT_HANDLE(project, packages, urls):
    """Dictionary serialization for project handle.

    Parameters
    ----------
    project : vizier.engine.project.ProjectHandle
        Project handle
    packages: dict(vizier.engine.packages.base.PackageIndex)
        Index of available packages
    urls: vizier.api.routes.UrlFactory
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
        branches.append(
            br.BRANCH_DESCRIPTOR(
                branch=branch,
                project=project,
                urls=urls
            )
        )
    obj['branches'] = branches
    # Add listing of available packages
    package_listing = list()
    for pckg in packages.values():
        pckg_obj = {'id': pckg.identifier, 'name': pckg.name}
        if not pckg.description is None:
            pckg_obj['description'] = pckg.description
        pckg_commands = list()
        for cmd in pckg.commands.values():
            cmd_obj = {'id': cmd.identifier, 'name': cmd.name}
            if not cmd.description is None:
                cmd_obj['description'] = cmd.description
            cmd_obj['parameters'] = cmd.parameters
            pckg_commands.append(cmd_obj)
        pckg_obj['commands'] = pckg_commands
        package_listing.append(pckg_obj)
    obj['packages'] = package_listing
    return obj
