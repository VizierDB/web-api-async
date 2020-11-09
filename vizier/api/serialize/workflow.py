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
serialize workflow resources.
"""
from typing import TYPE_CHECKING, Dict, Any, Optional
if TYPE_CHECKING:
    from vizier.engine.project.base import ProjectHandle
    from vizier.viztrail.branch import BranchHandle
    from vizier.viztrail.workflow import WorkflowHandle, WorkflowDescriptor

from vizier.api.routes.base import UrlFactory
import vizier.api.serialize.base as serialize
import vizier.api.serialize.labels as labels
import vizier.api.serialize.dataset as serialds
import vizier.api.serialize.hateoas as ref
import vizier.api.serialize.module as serialmd


def EMPTY_WORKFLOW_HANDLE(
        project: "ProjectHandle", 
        branch: "BranchHandle", 
        urls: Optional[UrlFactory]
    ) -> Dict[str, Any]:
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
    ret:Dict[str,Any] = {
        'id': None,
        'createdAt': None,
        'state': -1,
        'modules': list(),
        'datasets': list(),
        'charts': list(),
        'readOnly': False
    }
    if urls is not None:
        ret[labels.LINKS] = WORKFLOW_HANDLE_LINKS(
            project_id=project_id,
            branch_id=branch_id,
            urls=urls
        )
    return ret


def WORKFLOW_DESCRIPTOR(
        project: "ProjectHandle", 
        branch: "BranchHandle", 
        workflow: "WorkflowDescriptor", 
        urls: Optional[UrlFactory]
    ) -> Dict[str, Any]:
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
    ret = {
        'id': workflow_id,
        'createdAt': workflow.created_at.isoformat(),
        'action': workflow.action,
        labels.COMMAND_PACKAGE: workflow.package_id,
        labels.COMMAND_ID: workflow.command_id,
    }
    if urls is not None:
        ret[labels.LINKS] = WORKFLOW_HANDLE_LINKS(
            project_id=project_id,
            branch_id=branch_id,
            workflow_id=workflow_id,
            urls=urls
        )
    return ret


def WORKFLOW_HANDLE(
        project: "ProjectHandle", 
        branch: "BranchHandle", 
        workflow: "WorkflowHandle", 
        urls: Optional[UrlFactory]
    ) -> Dict[str, Any]:
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
    read_only = (branch.get_head().identifier != workflow_id)
    # Create lists of module handles and dataset handles
    modules = list()
    datasets = dict()
    dataset_names = list()
    dataobjects = dict()
    charts = dict()
    for m in workflow.modules:
        if not m.provenance.charts is None:
            for chart_name, chart in m.provenance.charts:
                charts[chart_name] = chart
        available_charts = list()
        # Only include charts for modules that completed successful
        for artifact in m.artifacts:
            if artifact.is_dataset:
                datasets[artifact.identifier] = serialds.DATASET_DESCRIPTOR(
                    dataset=artifact,
                    project=project,
                    urls=urls
                )
                dataset_names.append(artifact.name)
            else:
                dataobjects[artifact.identifier] = serialds.ARTIFACT_DESCRIPTOR(
                    artifact=artifact,
                    project=project,
                    urls=urls
                )
        if m.is_success:
            for c_handle in list(charts.values()):
                if c_handle.dataset_name in dataset_names:
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
    handle_links: Optional[Dict[str,Optional[str]]] = None
    if workflow.is_active and urls is not None:
        handle_links = {
            ref.WORKFLOW_CANCEL: urls.cancel_workflow(
                project_id=project_id,
                branch_id=branch_id
            )
        }
    links = {}
    if urls is not None:
        links = { labels.LINKS : WORKFLOW_HANDLE_LINKS(
                    project_id=project_id,
                    branch_id=branch_id,
                    workflow_id=workflow_id,
                    urls=urls,
                    links=handle_links
                )}
    return {
        'id': workflow_id,
        'createdAt': descriptor.created_at.isoformat(),
        'action': descriptor.action,
        labels.COMMAND_PACKAGE: descriptor.package_id,
        labels.COMMAND_ID: descriptor.command_id,
        'state': workflow.get_state().state,
        'modules': modules,
        'datasets': list(datasets.values()),
        'dataobjects': list(dataobjects.values()),
        'readOnly': read_only,
        **links
    }


def WORKFLOW_HANDLE_LINKS(
        urls: UrlFactory, 
        project_id: str, 
        branch_id: str, 
        workflow_id: Optional[str] = None, 
        links: Optional[Dict[str,Optional[str]]] = None):
    """Get basic set of HATEOAS references for workflow handles.

    For an empty workflow the identifier is None. In that case the result will
    not contain a self reference.

    Parameters
    ----------
    urls: vizier.api.routes.base.UrlFactory
        Factory for resource urls
    project_id: string
        Unique project identifier
    branch_id: string
        Unique branch identifier
    workflow_id: string, optional
        Unique workflow identifier

    Returns
    -------
    dict
    """
    if links is None:
        links = dict()
    links[ref.WORKFLOW_APPEND] = urls.workflow_module_append(
        project_id=project_id,
        branch_id=branch_id
    )
    # References to the workflow branch
    links[ref.WORKFLOW_BRANCH] = urls.get_branch(
        project_id=project_id,
        branch_id=branch_id
    )
    links[ref.BRANCH_HEAD] = urls.get_branch_head(
        project_id=project_id,
        branch_id=branch_id
    )
    links[ref.WORKFLOW_PROJECT] = urls.get_project(project_id)
    links[ref.FILE_UPLOAD] = urls.upload_file(project_id)
    # Only include self reference if workflow identifier is given
    if not workflow_id is None:
        links[ref.SELF] = urls.get_workflow(
            project_id=project_id,
            branch_id=branch_id,
            workflow_id=workflow_id
        )
    return serialize.HATEOAS(links)
