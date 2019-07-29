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

"""Blueprint for the Vizier web service application. Implements the requests
for the Vizier Web API as documented in:

    http://cds-swg1.cims.nyu.edu/vizier/api/v1/doc/
"""

import csv
import os
import StringIO
import tarfile

from flask import Blueprint, jsonify, make_response, request, send_file, send_from_directory
from werkzeug.utils import secure_filename

from vizier.api.routes.base import PAGE_LIMIT, PAGE_OFFSET
from vizier.api.webservice.base import VizierApi
from vizier.config.app import AppConfig
from vizier.datastore.annotation.dataset import DatasetMetadata

import vizier.api.base as srv
import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.labels as labels
import vizier.config.app as app
import pkg_resources

# -----------------------------------------------------------------------------
#
# App Blueprint
#
# -----------------------------------------------------------------------------

"""Get application configuration parameters from environment variables."""
config = AppConfig()

webui_file_dir = os.getenv('WEB_UI_STATIC_FILES', "./web-ui/build/")#pkg_resources.resource_filename(__name__, os.getenv('WEB_UI_STATIC_FILES', "./web-ui/build/"))
print(webui_file_dir)

global api
api = VizierApi(config, init=True)

# Create the application blueprint
bp = Blueprint('app', __name__, url_prefix=config.webservice.app_path, static_folder=webui_file_dir)


# ------------------------------------------------------------------------------
#
# Routes
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Service
# ------------------------------------------------------------------------------
@bp.route('/')
def service_descriptor():
    """Retrieve essential information about the web service including relevant
    links to access resources and interact with the service.
    """
    return jsonify(api.service_descriptor)


# ------------------------------------------------------------------------------
# Projects
# ------------------------------------------------------------------------------
@bp.route('/projects')
def list_projects():
    """Get a list of descriptors for all projects that are currently being
    managed by the API.
    """
    return jsonify(api.projects.list_projects())


@bp.route('/projects', methods=['POST'])
def create_project():
    """Create a new project. The request is expected to contain a Json object
    with an optional list of properties as (key,value)-pairs.

    Request
    -------
    {
      "properties": [
        {
          "key": "string",
          "value": "string"
        }
      ]
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or if any
    # of the provided project properties is not a dict with key and value.
    obj = srv.validate_json_request(request, required=['properties'])
    if not isinstance(obj['properties'], list):
        raise srv.InvalidRequest('expected a list of properties')
    properties = deserialize.PROPERTIES(obj['properties'])
    # Create project and return the project descriptor
    try:
        return jsonify(api.projects.create_project(properties)), 201
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))


@bp.route('/web-ui', defaults={'path': ''})
@bp.route('/web-ui/<path:path>')
def static_files(path):
    if path != "" and os.path.exists(webui_file_dir + path):
        return send_from_directory(webui_file_dir, path)
    else:
        return send_from_directory(webui_file_dir, 'index.html')

@bp.route('/projects/<string:project_id>/export')
def export_project(project_id):
    """Export the project data files as tar.gz.
    """
    # Get the handle for the dataset with given identifier. The result is None
    # if no dataset with given identifier exists.
    base_dir = config.engine.data_dir
    vistrails_dir  = os.path.join(base_dir, app.DEFAULT_VIZTRAILS_DIR)
    filestores_dir = os.path.join(base_dir, app.DEFAULT_FILESTORES_DIR)
    datastores_dir = os.path.join(base_dir, app.DEFAULT_DATASTORES_DIR)
    si = StringIO.StringIO()
    with tarfile.open(fileobj=si, mode="w:gz") as tar:
        tar.add(datastores_dir+os.path.sep+project_id, arcname=os.path.sep+"ds"+os.path.sep+project_id+os.path.sep)
        tar.add(filestores_dir+os.path.sep+project_id, arcname=os.path.sep+"fs"+os.path.sep+project_id+os.path.sep)
        tar.add(vistrails_dir+os.path.sep+project_id, arcname=os.path.sep+"vt"+os.path.sep+project_id+os.path.sep)
    
    # Return the tar file 
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename="+project_id+".tar.gz"
    output.headers["Content-type"] = "application/x-gzip"
    return output

@bp.route('/projects/import', methods=['POST'])
def import_project():
    """Upload file (POST) - Upload a data files for a project.
    """
    # The upload request may contain a file object or an Url from where to
    # download the data.
    if request.files and 'file' in request.files:
        file = request.files['file']
        # A browser may submit a empty part without filename
        if file.filename == '':
            raise srv.InvalidRequest('empty file name')
        # Save uploaded file to temp directory
        filename = secure_filename(file.filename)
        try:
            base_dir = config.engine.data_dir
            vistrails_dir  = os.path.join(base_dir, app.DEFAULT_VIZTRAILS_DIR)
            filestores_dir = os.path.join(base_dir, app.DEFAULT_FILESTORES_DIR)
            datastores_dir = os.path.join(base_dir, app.DEFAULT_DATASTORES_DIR)
            si = StringIO.StringIO()
            file.save(dst=si)
            si.seek(0)
            project_id = ""
            with tarfile.open(fileobj=si, mode="r:gz") as tar:
                for tarinfo in tar:
                    if tarinfo.name.startswith("ds/"):
                        project_id = tarinfo.name.split('/')[1]
                        break
            
                def ds_files(members):
                    for tarinfo in members:
                        if tarinfo.name.startswith("ds/"):
                            yield tarinfo
                
                def fs_files(members):
                    for tarinfo in tar:
                        if tarinfo.name.startswith("fs/"):
                            yield tarinfo
                
                def vt_files(members):
                    for tarinfo in tar:
                        if tarinfo.name.startswith("vt/"):
                            yield tarinfo
                
                
                tar.extractall(path=base_dir,members=ds_files(tar))
                tar.extractall(path=base_dir,members=fs_files(tar))
                tar.extractall(path=base_dir,members=vt_files(tar))
            vtfpath = base_dir+os.path.sep+"vt"+os.path.sep+"viztrails"
            vtf = open(vtfpath, "r")
            contents = "".join(vtf.readlines())
            vtf.close()
            vtf = open(vtfpath, "w")
            vtf.write(contents.replace(']',', "'+project_id+'"]'))
            vtf.close()
            global api
            api = VizierApi(config, init=True)
            pj = api.projects.get_project(project_id)
            if not pj is None:
                return jsonify(pj)
        except ValueError as ex:
            raise srv.InvalidRequest(str(ex))
    else:
        raise srv.InvalidRequest('no file or url specified in request')
    raise srv.ResourceNotFound('unknown project format')

@bp.route('/projects/<string:project_id>')
def get_project(project_id):
    """Retrieve information for project with given identifier."""
    # Retrieve project serialization. If project does not exist the result
    # will be none.
    pj = api.projects.get_project(project_id)
    if not pj is None:
        return jsonify(pj)
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\'')


@bp.route('/projects/<string:project_id>', methods=['DELETE'])
def delete_project(project_id):
    """Delete an existing project."""
    if api.projects.delete_project(project_id):
        return '', 204
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\'')


@bp.route('/projects/<string:project_id>', methods=['PUT'])
def update_project(project_id):
    """Update the set of user-defined properties. Expects a Json object with
    a list of property update statements. These statements are (key,value)
    pairs, where the value is optional (i.e., missing value for delete
    statements).

    Request
    -------
    {
      "properties": [
        {
          "key": "string",
          "value": "scalar or list of scalars"
        }
      ]
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or if any
    # of the project property update statements are invalid.
    obj = srv.validate_json_request(request, required=['properties'])
    if not isinstance(obj['properties'], list):
        raise srv.InvalidRequest('expected a list of properties')
    # Update project and return the project descriptor. If no project with
    # the given identifier exists the update result will be None
    properties = deserialize.PROPERTIES(obj['properties'], allow_null=True)
    try:
        pj = api.projects.update_project(project_id, properties)
        if not pj is None:
            return jsonify(pj)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\'')


# ------------------------------------------------------------------------------
# Branches
# ------------------------------------------------------------------------------
@bp.route('/projects/<string:project_id>/branches', methods=['POST'])
def create_branch(project_id):
    """Create a new branch for a project. Expects a description of the parent
    workflow in the request body together with an optional list of branch
    properties (e.g., containing a branch name).

    Request
    -------
    {
      "source": {
        "branchId": "string",
        "workflowId": "string"
        "moduleId": "string"
      },
      "properties": [
        {
          "key": "string",
          "value": "string"
        }
      ]
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain the expected elements.
    obj = srv.validate_json_request(request, required=['properties'], optional=['source'])
    # Get the branch point. If the source is given the dictionary should at
    # most contain the three identifier
    branch_id = None
    workflow_id = None
    module_id = None
    if 'source' in obj:
        source = obj['source']
        for key in source:
            if key == 'branchId':
                branch_id = source[key]
            elif key == 'workflowId':
                workflow_id = source[key]
            elif key ==  'moduleId':
                module_id = source[key]
            else:
                raise srv.InvalidRequest('invalid element \'' + key + '\' for branch point')
    # Get the properties for the new branch
    properties = deserialize.PROPERTIES(obj['properties'])
    # Create a new workflow. The result is the descriptor for the new workflow
    # or None if the specified project does not exist. Will raise a ValueError
    # if the specified workflow version or module do not exist
    try:
        branch = api.branches.create_branch(
            project_id=project_id,
            branch_id=branch_id,
            workflow_id=workflow_id,
            module_id=module_id,
            properties=properties
        )
        if not branch is None:
            return jsonify(branch)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>', methods=['DELETE'])
def delete_branch(project_id, branch_id):
    """Delete branch from a given project."""
    try:
        success = api.branches.delete_branch(project_id=project_id, branch_id=branch_id)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    if success:
        return '', 204
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>')
def get_branch(project_id, branch_id):
    """Get handle for a branch in a given project."""
    # Get the branch handle. The result is None if the project or the branch
    # do not exist.
    branch = api.branches.get_branch(project_id, branch_id)
    if not branch is None:
        return jsonify(branch)
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>', methods=['PUT'])
def update_branch(project_id, branch_id):
    """Update properties for a given project workflow branch. Expects a set of
    key,value-pairs in the request body. Properties with given key but missing
    value will be deleted.

    Request
    -------
    {
      "properties": [
        {
          "key": "string",
          "value": "string"
        }
      ]
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain a properties key.
    obj = srv.validate_json_request(request, required=['properties'])
    # Update properties for the given branch and return branch descriptor.
    properties = deserialize.PROPERTIES(obj['properties'], allow_null=True)
    try:
        # Result is None if project or branch are not found.
        branch = api.branches.update_branch(
            project_id=project_id,
            branch_id=branch_id,
            properties=properties
        )
        if not branch is None:
            return jsonify(branch)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


# ------------------------------------------------------------------------------
# Workflows
# ------------------------------------------------------------------------------

@bp.route('/projects/<string:project_id>/branches/<string:branch_id>/head')
def get_branch_head(project_id, branch_id):
    """Get handle for a workflow at the HEAD of a given project branch."""
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    workflow = api.workflows.get_workflow(project_id=project_id, branch_id=branch_id)
    if not workflow is None:
        return jsonify(workflow)
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>/head', methods=['POST'])
def append_branch_head(project_id, branch_id):
    """Append a module to the workflow that is at the HEAD of the given branch.

    Request
    -------
    {
      "packageId": "string",
      "commandId": "string",
      "arguments": []
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain the expected elements.
    cmd = srv.validate_json_request(
        request,
        required=['packageId', 'commandId', 'arguments']
    )
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project or branch are not found
        module = api.workflows.append_workflow_module(
            project_id=project_id,
            branch_id=branch_id,
            package_id=cmd['packageId'],
            command_id=cmd['commandId'],
            arguments=cmd['arguments']
        )
        if not module is None:
            return jsonify(module)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>/head/cancel', methods=['POST'])
def cancel_workflow(project_id, branch_id):
    """Cancel execution for all running and pending modules in the head
    workflow of a given project branch.
    """
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    workflow = api.workflows.cancel_workflow(
        project_id=project_id,
        branch_id=branch_id
    )
    if not workflow is None:
        return jsonify(workflow)
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<string:workflow_id>')
def get_workflow(project_id, branch_id, workflow_id):
    """Get handle for a workflow in a given project branch."""
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    workflow = api.workflows.get_workflow(
        project_id=project_id,
        branch_id=branch_id,
        workflow_id=workflow_id
    )
    if not workflow is None:
        return jsonify(workflow)
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or workflow \'' + workflow_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>/head/modules/<string:module_id>')
def get_workflow_module(project_id, branch_id, module_id):
    """Get handle for a module in the head workflow of a given project branch.
    """
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    module = api.workflows.get_workflow_module(
        project_id=project_id,
        branch_id=branch_id,
        module_id=module_id
    )
    if not module is None:
        return jsonify(module)
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or module \'' + module_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>/head/modules/<string:module_id>', methods=['DELETE'])
def delete_workflow_module(project_id, branch_id, module_id):
    """Delete a module in the head workflow of a given project branch."""
    result = api.workflows.delete_workflow_module(
        project_id=project_id,
        branch_id=branch_id,
        module_id=module_id
    )
    if not result is None:
        return jsonify(result)
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or module \'' + module_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>/head/modules/<string:module_id>', methods=['POST'])
def insert_workflow_module(project_id, branch_id, module_id):
    """Insert a module into a workflow branch before the specified module and
    execute the resulting workflow.

    Request
    -------
    {
      "packageId": "string",
      "commandId": "string",
      "arguments": []
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain the expected elements.
    cmd = srv.validate_json_request(
        request,
        required=['packageId', 'commandId', 'arguments']
    )
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project, branch or module are not found.
        modules = api.workflows.insert_workflow_module(
            project_id=project_id,
            branch_id=branch_id,
            before_module_id=module_id,
            package_id=cmd['packageId'],
            command_id=cmd['commandId'],
            arguments=cmd['arguments']
        )
        if not modules is None:
            return jsonify(modules)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or module \'' + module_id + '\'')


@bp.route('/projects/<string:project_id>/branches/<string:branch_id>/head/modules/<string:module_id>', methods=['PUT'])
def replace_workflow_module(project_id, branch_id, module_id):
    """Replace a module in the current project workflow branch and execute the
    resulting workflow.

    Request
    -------
    {
      "packageId": "string",
      "commandId": "string",
      "arguments": []
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain the expected elements.
    cmd = srv.validate_json_request(
        request,
        required=['packageId', 'commandId', 'arguments']
    )
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project, branch or module are not found.
        modules = api.workflows.replace_workflow_module(
            project_id=project_id,
            branch_id=branch_id,
            module_id=module_id,
            package_id=cmd['packageId'],
            command_id=cmd['commandId'],
            arguments=cmd['arguments']
        )
        if not modules is None:
            return jsonify(modules)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or module \'' + module_id + '\'')


# ------------------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------------------

@bp.route('/tasks/<string:task_id>', methods=['PUT'])
def update_task_state(task_id):
    """Update the state of a running task."""
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain the expected elements.
    obj = srv.validate_json_request(
        request,
        required=[labels.STATE],
        optional=[
            labels.STARTED_AT,
            labels.FINISHED_AT,
            labels.OUTPUTS,
            labels.PROVENANCE
        ]
    )
    # Update task state. The contents of the request body depend on the value of
    # the new task state. The request body is evaluated by the API. The API will
    # raise a ValueError if the request body is invalid. The result is None if
    # the project or task are unknown.
    try:
        # Result is None if task is not found.
        result = api.tasks.update_task_state(
            task_id=task_id,
            state=obj[labels.STATE],
            body=obj
        )
        if not result is None:
            return jsonify(result)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown task \'' + task_id + '\'')


# ------------------------------------------------------------------------------
# Datasets
# ------------------------------------------------------------------------------

@bp.route('/projects/<string:project_id>/datasets', methods=['POST'])
def create_dataset(project_id):
    """Create a new dataset in the datastore for the given project. The dataset
    schema and rows are given in the request body. Dataset annotations are
    optional. The expected request body format is:

    {
      "columns": [
        {
          "id": 0,
          "name": "string",
          "type": "string"
        }
      ],
      "rows": [
        {
          "id": 0,
          "values": [
            "string"
          ]
        }
      ],
      "annotations": [
        {
          "columnId": 0,
          "rowId": 0,
          "key": "string",
          "value": "string"
        }
      ]
    }
    """
    # Validate the request
    obj = srv.validate_json_request(
        request,
        required=[labels.COLUMNS, labels.ROWS],
        optional=[labels.ANNOTATIONS]
    )
    columns = deserialize.DATASET_COLUMNS(obj[labels.COLUMNS])
    rows = [deserialize.DATASET_ROW(row) for row in obj[labels.ROWS]]
    annotations = None
    if labels.ANNOTATIONS in obj:
        annotations = DatasetMetadata()
        for anno in obj[labels.ANNOTATIONS]:
            a = deserialize.ANNOTATION(anno)
            if a.column_id is None:
                annotations.rows.append(a)
            elif a.row_id is None:
                annotations.columns.append(a)
            else:
                annotations.cells.append(a)
    try:
        dataset = api.datasets.create_dataset(
            project_id=project_id,
            columns=columns,
            rows=rows,
            annotations=annotations
        )
        if not dataset is None:
            return jsonify(dataset)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\'')


@bp.route('/projects/<string:project_id>/datasets/<string:dataset_id>')
def get_dataset(project_id, dataset_id):
    """Get the dataset with given identifier that has been generated by a
    curation workflow.
    """
    # Get dataset rows with offset and limit parameters
    try:
        dataset = api.datasets.get_dataset(
            project_id=project_id,
            dataset_id=dataset_id,
            offset=request.args.get(PAGE_OFFSET),
            limit=request.args.get(PAGE_LIMIT)
        )
        if not dataset is None:
            return jsonify(dataset)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or dataset \'' + dataset_id + '\'')


@bp.route('/projects/<string:project_id>/datasets/<string:dataset_id>/annotations')
def get_dataset_annotations(project_id, dataset_id):
    """Get annotations that are associated with the given dataset.
    """
    # Expects at least a column or row identifier
    column_id = request.args.get(labels.COLUMN, type=int)
    row_id = request.args.get(labels.ROW, type=int)
    # Get annotations for dataset with given identifier. The result is None if
    # no dataset with given identifier exists.
    annotations = api.datasets.get_annotations(
        project_id=project_id,
        dataset_id=dataset_id,
        column_id=column_id,
        row_id=row_id
    )
    if not annotations is None:
        return jsonify(annotations)
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or dataset \'' + dataset_id + '\'')


@bp.route('/projects/<string:project_id>/datasets/<string:dataset_id>/descriptor')
def get_dataset_descriptor(project_id, dataset_id):
    """Get the descriptor for the dataset with given identifier."""
    try:
        dataset = api.datasets.get_dataset_descriptor(
            project_id=project_id,
            dataset_id=dataset_id
        )
        if not dataset is None:
            return jsonify(dataset)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or dataset \'' + dataset_id + '\'')


@bp.route('/projects/<string:project_id>/datasets/<string:dataset_id>/annotations', methods=['POST'])
def update_dataset_annotation(project_id, dataset_id):
    """Update an annotation that is associated with a component of the given
    dataset.

    Request
    -------
    {
      "columnId": 0,
      "rowId": 0,
      "key": "string",
      "oldValue": "string", or "int", or "float"
      "newValue": "string", or "int", or "float"
    }
    """
    # Validate the request
    obj = srv.validate_json_request(
        request,
        required=['key'],
        optional=['columnId', 'rowId', 'key', 'oldValue', 'newValue']
    )
    # Create update statement and execute. The result is None if no dataset with
    # given identifier exists.
    key = obj[labels.KEY] if labels.KEY in obj else None
    column_id = obj[labels.COLUMN_ID] if labels.COLUMN_ID in obj else None
    row_id = obj[labels.ROW_ID] if labels.ROW_ID in obj else None
    old_value = obj[labels.OLD_VALUE] if labels.OLD_VALUE in obj else None
    new_value = obj[labels.NEW_VALUE] if labels.NEW_VALUE in obj else None
    try:
        annotations = api.datasets.update_annotation(
            project_id=project_id,
            dataset_id=dataset_id,
            key=key,
            column_id=column_id,
            row_id=row_id,
            old_value=old_value,
            new_value=new_value
        )
        if not annotations is None:
            return jsonify(annotations)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or dataset \'' + dataset_id + '\'')


@bp.route('/projects/<string:project_id>/datasets/<string:dataset_id>/csv')
def download_dataset(project_id, dataset_id):
    """Get the dataset with given identifier in CSV format.
    """
    # Get the handle for the dataset with given identifier. The result is None
    # if no dataset with given identifier exists.
    _, dataset = api.datasets.get_dataset_handle(project_id, dataset_id)
    if dataset is None:
        raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or dataset \'' + dataset_id + '\'')
    # Read the dataset into a string buffer in memory
    si = StringIO.StringIO()
    cw = csv.writer(si)
    cw.writerow([col.name for col in dataset.columns])
    with dataset.reader() as reader:
        for row in reader:
            cw.writerow(row.values)
    # Return the CSV file file
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    return output


# ------------------------------------------------------------------------------
# Views
# ------------------------------------------------------------------------------
@bp.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<string:workflow_id>/modules/<string:module_id>/charts/<string:chart_id>')
def get_dataset_chart_view(project_id, branch_id, workflow_id, module_id, chart_id):
    """Get content of a dataset chart view for a given workflow module.
    """
    try:
        view = api.views.get_dataset_chart_view(
            project_id=project_id,
            branch_id=branch_id,
            workflow_id=workflow_id,
            module_id=module_id,
            chart_id=chart_id
        )
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    if not view is None:
        return jsonify(view)
    raise srv.ResourceNotFound(
        ''.join([
            'unknown project \'' + project_id,
            '\', branch \'' + branch_id,
            '\', workflow \'' + workflow_id,
            '\', module \'' + module_id,
            '\' or chart \'' + chart_id + '\''
        ])
    )


# ------------------------------------------------------------------------------
# Files
# ------------------------------------------------------------------------------

@bp.route('/projects/<string:project_id>/files', methods=['POST'])
def upload_file(project_id):
    """Upload file (POST) - Upload a data file to the project's filestore.
    """
    # The upload request may contain a file object or an Url from where to
    # download the data.
    if request.files and 'file' in request.files:
        file = request.files['file']
        # A browser may submit a empty part without filename
        if file.filename == '':
            raise srv.InvalidRequest('empty file name')
        # Save uploaded file to temp directory
        filename = secure_filename(file.filename)
        try:
            f_handle = api.files.upload_file(
                project_id=project_id,
                file=file,
                file_name=filename
            )
            if not f_handle is None:
                return jsonify(f_handle), 201
        except ValueError as ex:
            raise srv.InvalidRequest(str(ex))
    else:
        raise srv.InvalidRequest('no file or url specified in request')
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\'')


@bp.route('/projects/<string:project_id>/files/<string:file_id>')
def download_file(project_id, file_id):
    """Download file from file server."""
    # Get handle for file from the project's filestore
    f_handle = api.files.get_file(project_id, file_id)
    if not f_handle is None:
        # Use send_file to send the contents of the file
        if f_handle.compressed:
            mimetype = 'application/gzip'
        else:
            mimetype = f_handle.mimetype
        return send_file(
            f_handle.filepath,
            mimetype=mimetype,
            attachment_filename=f_handle.file_name,
            as_attachment=True
        )
    raise srv.ResourceNotFound('unknown project \'' + project_id + '\' or file \'' + file_id + '\'')
