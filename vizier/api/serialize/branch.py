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
serialize branches.
"""

import vizier.api.serialize.base as serialize
import vizier.api.serialize.hateoas as ref
import vizier.api.serialize.labels as labels
import vizier.api.serialize.workflow as workflow


def BRANCH_DESCRIPTOR(project, branch, urls):
    """Dictionary serialization for branch descriptor.

    Parameters
    ----------
    project: vizier.engine.project.base.ProjectHandle
        Handle for the containing project
    branch : vizier.viztrail.branch.BranchHandle
        Branch handle
    urls: vizier.api.routes.base.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    project_id = project.identifier
    branch_id = branch.identifier
    return {
        'id': branch_id,
        'createdAt': branch.provenance.created_at.isoformat(),
        'lastModifiedAt': branch.last_modified_at.isoformat(),
        'isDefault': project.viztrail.is_default_branch(branch_id),
        'properties': serialize.ANNOTATIONS(branch.properties),
        labels.LINKS: serialize.HATEOAS({
            ref.SELF: urls.get_branch(project_id, branch_id),
            ref.BRANCH_DELETE: urls.delete_branch(project_id, branch_id),
            ref.BRANCH_HEAD: urls.get_branch_head(project_id, branch_id),
            ref.BRANCH_UPDATE: urls.update_branch(project_id, branch_id)
        })
    }


def BRANCH_HANDLE(project, branch, urls):
    """Dictionary serialization for branch handle. Extends branch descriptor
    with timestamps and workflow descriptors from branch history.

    Parameters
    ----------
    project: vizier.engine.project.base.ProjectHandle
        Handle for the containing project
    branch : vizier.viztrail.branch.BranchHandle
        Branch handle
    urls: vizier.api.routes.base.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    project_id = project.identifier
    branch_id = branch.identifier
    # Use the branch descriptor as basis
    obj = BRANCH_DESCRIPTOR(project=project, branch=branch, urls=urls)
    # Add descriptors for workflows in the branch history
    obj['workflows'] = [
        workflow.WORKFLOW_DESCRIPTOR(
            project=project,
            branch=branch,
            workflow=wf,
            urls=urls
        ) for wf in branch.get_history()
    ]
    return obj
