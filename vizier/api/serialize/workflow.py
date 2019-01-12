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
serialize workflow resources.
"""

import vizier.api.serialize.base as serialize
import vizier.api.serialize.labels as labels
import vizier.api.serialize.dataset as serialds
import vizier.api.serialize.module as serialmd


def EMPTY_WORKFLOW_HANDLE(project, branch, urls):
    """Dictionary serialization for a an empty workflow. Sets most values to
    None or empty lists.

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
        'id': None,
        'createdAt': None,
        'state': -1,
        'modules': list(),
        'datasets': list(),
        'charts': list(),
        'readOnly': False,
        labels.LINKS: serialize.HATEOAS({
            'workflow:append': urls.workflow_append(
                project_id=project_id,
                branch_id=branch_id
            ),
            'workflow:branch': urls.get_branch(
                project_id=project_id,
                branch_id=branch_id
            ),
            'workflow:project': urls.get_project(project_id)
        })
    }


def WORKFLOW_DESCRIPTOR(project, branch, workflow, urls):
    """Dictionary serialization for a workflow descriptor.

    Parameters
    ----------
    project: vizier.engine.project.base.ProjectHandle
        Handle for the containing project
    branch : vizier.viztrail.branch.BranchHandle
        Branch handle
    workflow: vizier.viztrail.workflow.WorkflowDescriptor
        Workflow descriptor
    urls: vizier.api.routes.base.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    project_id = project.identifier
    branch_id = branch.identifier
    workflow_id = workflow.identifier
    return {
        'id': workflow_id,
        'createdAt': workflow.created_at.isoformat(),
        'action': workflow.action,
        'packageId': workflow.package_id,
        'commandId': workflow.command_id,
        labels.LINKS: serialize.HATEOAS({
            'self': urls.get_workflow(
                project_id=project_id,
                branch_id=branch_id,
                workflow_id=workflow_id
            ),
            'workflow:branch': urls.get_branch(
                project_id=project_id,
                branch_id=branch_id
            ),
            'workflow:project': urls.get_project(project_id)
        })
    }


def WORKFLOW_HANDLE(project, branch, workflow, urls):
    """Dictionary serialization for a workflow handle.

    Parameters
    ----------
    project: vizier.engine.project.base.ProjectHandle
        Handle for the containing project
    branch : vizier.viztrail.branch.BranchHandle
        Branch handle
    workflow: vizier.viztrail.workflow.WorkflowHandle
        Workflow handle
    urls: vizier.api.routes.base.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    project_id = project.identifier
    branch_id = branch.identifier
    workflow_id = workflow.identifier
    descriptor = workflow.descriptor
    read_only = (branch.head.identifier != workflow_id)
    # Create lists of module handles and dataset handles
    modules = list()
    datasets = dict()
    charts = dict()
    for m in workflow.modules:
        if not m.provenance.charts is None:
            for c_handle in m.provenance.charts:
                charts[c_handle.chart_name.lower()] = c_handle
        available_charts = list()
        # Only include charts for modules that completed successful
        if m.is_success:
            for c_handle in charts.values():
                if c_handle.dataset_name in m.datasets:
                    available_charts.append(c_handle)
        modules.append(
            serialmd.MODULE_HANDLE(
                project=project,
                branch=branch,
                workflow=workflow,
                module=m,
                charts=available_charts,
                urls=urls,
                include_self=(not read_only)
            )
        )
        for name in m.datasets:
            ds = m.datasets[name]
            if not ds.identifier in datasets:
                datasets[ds.identifier] = serialds.DATASET_DESCRIPTOR(
                    dataset=ds,
                    name=name,
                    project=project,
                    urls=urls
                )
    return {
        'id': workflow_id,
        'createdAt': descriptor.created_at.isoformat(),
        'action': descriptor.action,
        'packageId': descriptor.package_id,
        'commandId': descriptor.command_id,
        'state': workflow.get_state().state,
        'modules': modules,
        'datasets': datasets.values(),
        'readOnly': read_only,
        labels.LINKS: serialize.HATEOAS({
            'workflow:append': urls.workflow_append(
                project_id=project_id,
                branch_id=branch_id
            ),
            'workflow:branch': urls.get_branch(
                project_id=project_id,
                branch_id=branch_id
            ),
            'workflow:cancel': urls.cancel_workflow(
                project_id=project_id,
                branch_id=branch_id
            ),
            'workflow:project': urls.get_project(project_id)
        })
    }
