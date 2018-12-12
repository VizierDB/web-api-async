# Copyright (C) 2018 New York University
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

"""Vizier Web Server - Implements the requests for the Vizier Web API as
documented in http://cds-swg1.cims.nyu.edu/doc/vizier-db.
"""
from flask import Flask, jsonify, make_response, request, send_file
from flask_cors import CORS
import csv
import gzip
import os
import StringIO
import shutil
from werkzeug.utils import secure_filename
import tempfile
import urllib2
import yaml

from vizier.api import VizierWebService
from vizier.config import AppConfig, ENGINEENV_DEFAULT, ENGINEENV_MIMIR
from vizier.core.util import LOGGER_ENGINE
from vizier.datastore.federated import FederatedDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.hateoas import PAGE_LIMIT, PAGE_OFFSET
from vizier.workflow.module import ModuleSpecification
from vizier.workflow.repository.fs import FileSystemViztrailRepository


# -----------------------------------------------------------------------------
#
# App Initialization
#
# -----------------------------------------------------------------------------

"""Read configuration parameter from a config file. The configuration file is
expected to be in YAML format. Attempts to read the file specified in the
environment variable VIZIERSERVER_CONFIG first. If the variable is not set
(or if the specified file does not exist) an attempt is made to read file
config.yaml in the current working directory. If neither exists the the default
configuration is used.
"""
config = AppConfig()

# Create the app and enable cross-origin resource sharing
app = Flask(__name__)
app.config['APPLICATION_ROOT'] = config.api.app_path
app.config['DEBUG'] = config.debug
# Set size limit for uploaded files
app.config['MAX_CONTENT_LENGTH'] = config.fileserver.max_file_size

CORS(app)

# Application-wide file server. There currently is only one default file server.
fileserver = DefaultFileServer(config.fileserver.directory)

# Create execution environments and the datastore for the API. Different
# environments may use different data stores. The API needs to be able to serve
# datasets from all of them. Thus, if more than one execution environment is
# specified we need to use a federated datastore.
environments = list()
datastores = list()
for env_id in config.envs:
    exec_env = ExecEnv(config.envs[env_id], config.packages, fileserver)
    environments.append(exec_env)
    datastores.append(exec_env.datastore)

# Federate data stores if more than one was given
if len(datastores) > 1:
    datastore = FederatedDataStore(datastores)
else:
    datastore = datastores[0]

# Initialize the Web Service API.
api = VizierWebService(
    repo=FileSystemViztrailRepository(config.viztrails.directory, config.envs),
    datastore=datastore,
    fileserver=fileserver,
    environments=environments,
    config=config
)


# ------------------------------------------------------------------------------
#
# Routes
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Service
# ------------------------------------------------------------------------------
@app.route('/')
def service_overview():
    """Retrieve essential information about the web service including relevant
    links to access resources and interact with the service.
    """
    return jsonify(api.service_overview())


@app.route('/build')
def system_build():
    """Retrieve information about individual system components (e.g., version
    information for software components).
    """
    return jsonify(api.system_build())


# ------------------------------------------------------------------------------
# Files
# ------------------------------------------------------------------------------
@app.route('/files')
def list_files():
    """Get a listing of handles for all uploaded files."""
    return jsonify(api.list_files())


@app.route('/files/upload', methods=['POST'])
def upload_file():
    """Upload CSV file (POST) - Upload a CSV or TSV file containing a full
    dataset.
    """
    # The upload request may contain a file object or an Url from where to
    # download the data.
    if request.files and 'file' in request.files:
        file = request.files['file']
        # A browser may submit a empty part without filename
        if file.filename == '':
            raise InvalidRequest('empty file name')
        # Save uploaded file to temp directory
        temp_dir = tempfile.mkdtemp()
        filename = secure_filename(file.filename)
        upload_file = os.path.join(temp_dir, filename)
        file.save(upload_file)
        prov = {'filename': file.filename}
    elif request.json and 'url' in request.json:
        obj = validate_json_request(request, required=['url'])
        url = obj['url']
        # Save uploaded file to temp directory
        temp_dir = tempfile.mkdtemp()
        try:
            response = urllib2.urlopen(url)
            filename = get_download_filename(url, response.info())
            upload_file = os.path.join(temp_dir, secure_filename(filename))
            mode = 'w'
            if filename.endswith('.gz'):
                mode += 'b'
            with open(upload_file, mode) as f:
                f.write(response.read())
        except Exception as ex:
            shutil.rmtree(temp_dir)
            raise InvalidRequest(str(ex))
        if os.stat(upload_file).st_size > config.fileserver.max_file_size:
            shutil.rmtree(temp_dir)
            raise InvalidRequest('file size exceeds limit')
        prov = {'url': url}
    else:
        raise InvalidRequest('no file or url specified in request')
    try:
        result = jsonify(api.upload_file(upload_file, provenance=prov)), 201
        shutil.rmtree(temp_dir)
        return result
    except ValueError as ex:
        shutil.rmtree(temp_dir)
        raise InvalidRequest(str(ex))


@app.route('/files/<string:file_id>')
def get_file(file_id):
    """Retrieve handle for uploaded file."""
    f_handle = api.get_file(file_id)
    if not f_handle is None:
        return jsonify(f_handle)
    raise ResourceNotFound('unknown file \'' + file_id + '\'')


@app.route('/files/<string:file_id>', methods=['DELETE'])
def delete_file(file_id):
    """Delete uploaded file from file server."""
    if api.delete_file(file_id):
        return '', 204
    else:
        raise ResourceNotFound('unknown file \'' + file_id + '\'')


@app.route('/files/<string:file_id>/download')
def download_file(file_id):
    """Download file from file server in CSV format."""
    # Create a string buffer writer for the requested output format
    si = StringIO.StringIO()
    file_format = request.args.get('format')
    if not file_format is None:
        if file_format == 'csv':
            writer = csv.writer(si)
        elif file_format == 'tsv':
            writer = csv.writer(si, delimiter='\t')
        else:
            raise InvalidRequest('unknown file format \'' + file_format + '\'')
    else:
        file_format = 'csv'
        writer = csv.writer(si)
    f_handle = api.get_file_handle(file_id)
    if not f_handle is None:
        # Return a csv/tsv file if the requested file has been verified as a
        # valid csv (it is expected to be in tab-delimited format)
        if f_handle.is_verified_csv:
            filename = f_handle.base_name + '.' + file_format
            with f_handle.open() as f:
                for row in csv.reader(f, delimiter=f_handle.delimiter, skipinitialspace=True):
                    writer.writerow(row)
            output = make_response(si.getvalue())
            output.headers["Content-Disposition"] = "attachment; filename=" + filename
            output.headers["Content-type"] = "text/csv"
            return output
        else:
            # Send the file as it was uploaded (with original name)
            try:
                return send_file(
                    f_handle.filepath,
                    attachment_filename=f_handle.upload_name,
                    as_attachment=True
                )
            except Exception as ex:
                raise InvalidRequest(str(ex))
    raise ResourceNotFound('unknown file \'' + file_id + '\'')


@app.route('/files/<string:file_id>', methods=['POST'])
def rename_file(file_id):
    """Rename an existing file.

    Request
    -------
    {
      "name": "string"
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or if the
    # new file name is not given.
    obj = validate_json_request(request, required=['name'])
    try:
        f_handle = api.rename_file(file_id, obj['name'])
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    if not f_handle is None:
        return jsonify(f_handle)
    else:
        raise ResourceNotFound('unknown file \'' + file_id + '\'')


# ------------------------------------------------------------------------------
# Datasets
# ------------------------------------------------------------------------------
@app.route('/datasets/<string:dataset_id>')
def get_dataset(dataset_id):
    """Get the dataset with given identifier that has been generated by a
    curation workflow.
    """
    # Get dataset rows with offset and limit parameters
    try:
        dataset = api.get_dataset(
            dataset_id,
            offset=request.args.get(PAGE_OFFSET),
            limit=request.args.get(PAGE_LIMIT)
        )
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    if not dataset is None:
        return jsonify(dataset)
    raise ResourceNotFound('unknown dataset \'' + dataset_id + '\'')


@app.route('/datasets/<string:dataset_id>/annotations')
def get_dataset_annotations(dataset_id):
    """Get annotations that are associated with the given dataset.
    """
    # Expects at least a column or row identifier
    column_id = request.args.get('column', -1, type=int)
    row_id = request.args.get('row', -1, type=int)
    if column_id < 0 and row_id < 0:
        raise InvalidRequest('missing identifier for column and row')
    # Get annotations for dataset with given identifier. The result is None if
    # no dataset with given identifier exists.
    annotations = api.get_dataset_annotations(
        dataset_id,
        column_id=column_id,
        row_id=row_id
    )
    if not annotations is None:
        return jsonify(annotations)
    raise ResourceNotFound('unknown dataset \'' + dataset_id + '\'')


@app.route('/datasets/<string:dataset_id>/annotations', methods=['POST'])
def update_dataset_annotation(dataset_id):
    """Update an annotation that is associated with a component of the given
    dataset.

    Request
    -------
    {
      "annoId": 0,
      "columnId": 0,
      "rowId": 0,
      "key": "string",
      "value": "string"
    }
    """
    # Validate the request
    obj = validate_json_request(
        request,
        required=[],
        optional=['annoId', 'columnId', 'rowId', 'key', 'value']
    )
    # Create update statement and execute. The result is None if no dataset with
    # given identifier exists.
    key = obj['key'] if 'key' in obj else None
    anno_id = obj['annoId'] if 'annoId' in obj else -1
    column_id = obj['columnId'] if 'columnId' in obj else -1
    row_id = obj['rowId'] if 'rowId' in obj else -1
    value = obj['value'] if 'value' in obj else None
    annotations = api.update_dataset_annotation(
        dataset_id,
        column_id=column_id,
        row_id=row_id,
        anno_id=anno_id,
        key=key,
        value=value
    )
    if not annotations is None:
        return jsonify(annotations)
    raise ResourceNotFound('unknown dataset \'' + dataset_id + '\'')


@app.route('/datasets/<string:dataset_id>/csv')
def download_dataset(dataset_id):
    """Get the dataset with given identifier in CSV format.
    """
    # Get the handle for the dataset with given identifier. The result is None
    # if no dataset with given identifier exists.
    dataset = api.get_dataset_handle(dataset_id)
    if dataset is None:
        raise ResourceNotFound('unknown dataset \'' + dataset_id + '\'')
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
# Projects
# ------------------------------------------------------------------------------
@app.route('/projects', methods=['POST'])
def create_project():
    """Create a new project. The request is expected to contain a Json object
    with an optional list of properties as (key,value)-pairs.

    Request
    -------
    {
      "environment": "string",
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
    obj = validate_json_request(request, required=['environment', 'properties'])
    if not isinstance(obj['properties'], list):
        raise InvalidRequest('expected a list of properties')
    properties = dict()
    for prop in obj['properties']:
        if not isinstance(prop, dict):
            raise InvalidRequest('expected property to be a dictionary')
        for key in ['key', 'value']:
            if not key in prop:
                raise InvalidRequest('missing property element \'' + key + '\'')
        properties[prop['key']] = prop['value']
    # Create project and return the project descriptor
    try:
        return jsonify(api.create_project(obj['environment'], properties)), 201
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/projects/<string:project_id>', methods=['DELETE'])
def delete_project(project_id):
    """Delete an existing project."""
    if api.delete_project(project_id):
        return '', 204
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


@app.route('/projects/<string:project_id>')
def get_project(project_id):
    """Retrieve information for project with given identifier."""
    # Retrieve project serialization. If project does not exist the result
    # will be none.
    pj = api.get_project(
        project_id,
        branch_id=request.args.get('branch'),
        version=request.args.get('version')
    )
    if not pj is None:
        return jsonify(pj)
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


@app.route('/projects')
def list_projects():
    """Get a list of descriptors for all projects that are currently being
    managed by the API.
    """
    return jsonify(api.list_projects())


@app.route('/projects/<string:project_id>/modulespecs')
def list_module_specifications_for_project(project_id):
    """Retrieve list of parameter specifications for all supported modules for
    the given project.
    """
    # Retrieve project module specifications. the result will be None if the
    # project does not exist.
    modules = api.list_module_specifications_for_project(project_id)
    if not modules is None:
        return jsonify(modules)
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


@app.route('/projects/<string:project_id>/properties', methods=['POST'])
def update_project_properties(project_id):
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
          "value": "string"
        }
      ]
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or if any
    # of the project property update statements are invalid.
    obj = validate_json_request(request, required=['properties'])
    if not isinstance(obj['properties'], list):
        raise InvalidRequest('expected a list of properties')
    properties = dict()
    for prop in obj['properties']:
        if not isinstance(prop, dict):
            raise InvalidRequest('expected property to be a dictionary')
        if not 'key' in prop:
            raise InvalidRequest('missing property element: ' + key)
        if 'value' in prop:
            properties[prop['key']] = prop['value']
        else:
            properties[prop['key']] = None
    # Update project and return the project descriptor. If no project with
    # the given identifier exists the update result will be None
    try:
        pj = api.update_project_properties(project_id, properties)
        if not pj is None:
            return jsonify(pj)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


# ------------------------------------------------------------------------------
# Workflows
# ------------------------------------------------------------------------------
@app.route('/projects/<string:project_id>/branches')
def list_branches(project_id):
    """Get a list of all branches for a given project."""
    # Get branches. The result is None if the project does not exist.
    branches = api.list_branches(project_id)
    if not branches is None:
        return jsonify(branches)
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


@app.route('/projects/<string:project_id>/branches', methods=['POST'])
def create_branch(project_id):
    """Create a new branch for a project. Expects a description of the parent
    workflow in the request body together with an optional list of branch
    properties (e.g., containing a branch name).

    Request
    -------
    {
      "source": {
        "branch": "string",
        "version": "int"
        "moduleId": 0
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
    obj = validate_json_request(request, required=['source', 'properties'])
    for key in ['branch', 'version', 'moduleId']:
        if not key in obj['source']:
            raise InvalidRequest('missing element \'' + key + '\' in request source')
    branch_id = obj['source']['branch']
    workflow_version = obj['source']['version']
    module_id = obj['source']['moduleId']
    properties = dict()
    for prop in obj['properties']:
        for key in ['key', 'value']:
            if not key in prop:
                raise InvalidRequest('missing element \'' + key + '\' in request property')
        properties[prop['key']] = prop['value']
    # Create a new workflow. The result is the descriptor for the new workflow
    # or None if the specified project does not exist. Will raise a ValueError
    # if the specified workflow version or module do not exist
    try:
        wf = api.create_branch(
            project_id,
            branch_id,
            workflow_version,
            module_id,
            properties
        )
        if not wf is None:
            return jsonify(wf)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>')
def get_branch(project_id, branch_id):
    """Get handle for a branch in a given project."""
    # Get the branch handle. The result is None if the project or the branch
    # do not exist.
    branch = api.get_branch(project_id, branch_id)
    if not branch is None:
        return jsonify(branch)
    raise ResourceNotFound('unknown branch \'' + project_id + ':' + branch_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>', methods=['DELETE'])
def delete_branch(project_id, branch_id):
    """Delete branch from a given project."""
    try:
        success = api.delete_branch(project_id, branch_id)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    if success:
        return '', 204
    raise ResourceNotFound('unknown branch \'' + project_id + ':' + branch_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/head')
def get_branch_head(project_id, branch_id):
    """Get handle for a workflow at the HEAD of a given project branch."""
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    wf = api.get_workflow(project_id, branch_id)
    if not wf is None:
        return jsonify(wf)
    raise ResourceNotFound('unknown workflow \'' + project_id + ':' + branch_id + ':head\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/head/modules', methods=['POST'])
def append_branch_head(project_id, branch_id):
    """Append a module to the workflow that is at the HEAD of the given branch.

    Request
    -------
    {
      "type": "string",
      "id": "string",
      "arguments": {}
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain a command key.
    cmd = validate_json_request(
        request,
        required=['type', 'id', 'arguments']
    )
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project or workflow version are not found.
        wf = api.append_module(
            project_id,
            branch_id,
            -1,
            ModuleSpecification(cmd['type'], cmd['id'], cmd['arguments']),
            before_id=-1
        )
        if not wf is None:
            return jsonify(wf)
        raise ResourceNotFound('unknown workflow \'' + project_id + ':' + branch_id + ':head\'')
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/properties', methods=['POST'])
def update_branch_properties(project_id, branch_id):
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
    obj = validate_json_request(request, required=['properties'])
    properties = dict()
    for prop in obj['properties']:
        if not 'key' in prop:
            raise InvalidRequest('missing element \'key\' in request property')
        if not 'value' in prop:
            properties[prop['key']] = None
        else:
            properties[prop['key']] = prop['value']
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project or workflow version are not found.
        wf = api. update_branch(
            project_id,
            branch_id,
            properties
        )
        if not wf is None:
            return jsonify(wf)
        raise ResourceNotFound('unknown workflow \'' + project_id + ':' + branch_id + '\'')
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<int:version>')
def get_workflow(project_id, branch_id, version):
    """Get handle for a workflow in a given project branch."""
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    wf = api.get_workflow(project_id, branch_id, version)
    if not wf is None:
        return jsonify(wf)
    raise ResourceNotFound('unknown workflow \'' + project_id + ':' + branch_id + ':' + str(version) + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<int:version>/modules')
def workflow_modules(project_id, branch_id, version):
    """Get list of modules in a given workflow."""
    # Get workflow modules. the result is None if the project, bramch, or
    # workflow do not exist.
    wf = api.get_workflow_modules(project_id, branch_id, version)
    if not wf is None:
        return jsonify(wf)
    raise ResourceNotFound('unknown workflow \'' + project_id + ':' + branch_id + ':' + str(version) + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<int:version>/modules', methods=['POST'])
def append_module(project_id, branch_id, version):
    """Append a module to a workflow branch and execute the resulting workflow.

    Request
    -------
    {
      "type": "string",
      "id": "string",
      "arguments": {}
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain a command key.
    cmd = validate_json_request(
        request,
        required=['type', 'id', 'arguments'],
        optional=['includeDataset']
    )
    # The optional offset argument is used to include the updated dataset in the
    # response.
    includeDataset = None
    if 'includeDataset' in cmd:
        includeDataset = cmd['includeDataset']
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project or workflow version are not found.
        wf = api.append_module(
            project_id,
            branch_id,
            version,
            ModuleSpecification(cmd['type'], cmd['id'], cmd['arguments']),
            before_id=-1,
            includeDataset=includeDataset
        )
        if not wf is None:
            return jsonify(wf)
        raise ResourceNotFound('unknown workflow \'' + project_id + ':' + branch_id + ':' + str(version) + '\'')
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<int:version>/modules/<int:module_id>', methods=['DELETE'])
def delete_module(project_id, branch_id, version, module_id):
    """Delete a module in a workflow branch and execute the resulting workflow.
    """
    wf = api.delete_module(project_id, branch_id, version, module_id)
    if not wf is None:
        return jsonify(wf)
    raise ResourceNotFound('unknown workflow module \'' + project_id + ':' + branch_id + ':' + str(version) + '[' + str(module_id) + ']\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<int:version>/modules/<int:module_id>', methods=['POST'])
def insert_module(project_id, branch_id, version, module_id):
    """Insert a module into a workflow branch before the specified module and
    execute the resulting workflow.

    Request
    -------
    {
      "type": "string",
      "id": "string",
      "arguments": {}
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain a command key.
    cmd = validate_json_request(
        request,
        required=['type', 'id', 'arguments']
    )
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project or workflow version are not found.
        wf = api.append_module(
            project_id,
            branch_id,
            version,
            ModuleSpecification(cmd['type'], cmd['id'], cmd['arguments']),
            before_id=module_id
        )
        if not wf is None:
            return jsonify(wf)
        raise ResourceNotFound('unknown workflow module \'' + project_id + ':' + branch_id + ':' + str(version)  + '[' + str(module_id) + ']\'')
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<int:version>/modules/<int:module_id>', methods=['PUT'])
def replace_module(project_id, branch_id, version, module_id):
    """Replace a module in the current project workflow branch and execute the
    resulting workflow.

    Request
    -------
    {
      "type": "string",
      "id": "string",
      "arguments": {}
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain a command key.
    cmd = validate_json_request(
        request,
        required=['type', 'id', 'arguments'],
        optional=['includeDataset']
    )
    # The optional include dataset argument is used to include the updated
    # dataset in the response.
    includeDataset = None
    if 'includeDataset' in cmd:
        includeDataset = cmd['includeDataset']
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project or workflow version are not found.
        wf = api.replace_module(
            project_id,
            branch_id,
            version,
            module_id,
            ModuleSpecification(cmd['type'], cmd['id'], cmd['arguments']),
            includeDataset=includeDataset
        )
        if not wf is None:
            return jsonify(wf)
        raise ResourceNotFound('unknown workflow module \'' + project_id + ':' + branch_id + ':' + str(module_id) + '\'')
    except ValueError as ex:
        raise InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Notebooks
# ------------------------------------------------------------------------------
@app.route('/notebooks')
def get_notebook():
    """Get notebook handle for a given workflow. The workflow is specified using
    arguments in the request query.
    """
    try:
        version = request.args.get('version')
        if not version is None:
            version = int(version)
        notebook = api.get_notebook(
            project_id=request.args.get('project'),
            branch_id=request.args.get('branch'),
            version=version
        )
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    if not notebook is None:
        return jsonify(notebook)
    raise ResourceNotFound('could not find the requested project or workflow version')


# ------------------------------------------------------------------------------
# Views
# ------------------------------------------------------------------------------
@app.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<int:version>/modules/<int:module_id>/views/<string:view_id>')
def get_dataset_chart_view(project_id, branch_id, version, module_id, view_id):
    """Get content of a dataset chart view for a given workflow module.
    """
    try:
        view = api.get_dataset_chart_view(
            project_id,
            branch_id,
            version,
            module_id,
            view_id
        )
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    if not view is None:
        return jsonify(view)
    raise ResourceNotFound('unknown dataset view \'' + project_id + ':' + branch_id + ':' + str(version) + ':' + str(module_id) + ':' + view_id + '\'')


# ------------------------------------------------------------------------------
#
# Initialize
#
# ------------------------------------------------------------------------------

@app.before_first_request
def initialize():
    """Initialize the connection to the Mimir gateway if Mimir execution
    environment is used.
    """
    if ENGINEENV_MIMIR in config.envs:
        try:
            import vistrails.packages.mimir.init as mimir
            mimir.initialize()
        except Exception as ex:
            pass


# ------------------------------------------------------------------------------
#
# Exceptions
#
# ------------------------------------------------------------------------------

class ServerRequestException(Exception):
    """Base class for API exceptions."""
    def __init__(self, message, status_code):
        """Initialize error message and status code.

        Parameters
        ----------
        message : string
            Error message.
        status_code : int
            Http status code.
        """
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code

    def to_dict(self):
        """Dictionary representation of the exception.

        Returns
        -------
        Dictionary
        """
        return {'message' : self.message}


class InvalidRequest(ServerRequestException):
    """Exception for invalid requests that have status code 400."""
    def __init__(self, message):
        """Initialize the message and status code (400) of super class.

        Parameters
        ----------
        message : string
            Error message.
        """
        super(InvalidRequest, self).__init__(message, 400)


class NoJsonInRequest(InvalidRequest):
    """Exception to signal that a request body does not contain the expected
    Json element.
    """
    def __init__(self):
        """Set message of the super class BAD REQUEST response."""
        super(NoJsonInRequest, self).__init__('invalid request body format')


class ResourceNotFound(ServerRequestException):
    """Exception for file not found situations that have status code 404."""
    def __init__(self, message):
        """Initialize the message and status code (404) of super class.

        Parameters
        ----------
        message : string
            Error message.
        """
        super(ResourceNotFound, self).__init__(message, 404)


# ------------------------------------------------------------------------------
#
# Error Handler
#
# ------------------------------------------------------------------------------

@app.errorhandler(ServerRequestException)
def invalid_request_or_resource_not_found(error):
    """JSON response handler for invalid requests or requests that access
    unknown resources.

    Parameters
    ----------
    error : Exception
        Exception thrown by request Handler

    Returns
    -------
    Http response
    """
    app.logger.error(error.message)
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.errorhandler(413)
def upload_error(exception):
    """Exception handler for file uploads that exceed the file size limit."""
    app.logger.error(exception)
    return make_response(jsonify({'error': str(exception)}), 413)


@app.errorhandler(500)
def internal_error(exception):
    """Exception handler that logs exceptions."""
    app.logger.error(exception)
    return make_response(jsonify({'error': str(exception)}), 500)


# ------------------------------------------------------------------------------
#
# Helper Methods
#
# ------------------------------------------------------------------------------

def get_download_filename(url, info):
    """Extract a file name from a given Url or request info header.

    Parameters
    ----------
    url: string
        Url that was opened using urllib2.urlopen
    info: dict
        Header information returned by urllib2.urlopen

    Returns
    -------
    string
    """
    # Try to extract the filename from the Url first
    filename = url[url.rfind('/') + 1:]
    if '.' in filename:
        return filename
    else:
        if 'Content-Disposition' in info:
            content = info['Content-Disposition']
            if 'filename="' in content:
                filename = content[content.rfind('filename="') + 11:]
                return filename[:filename.find('"')]
    return 'download'


def validate_json_request(request, required=None, optional=None):
    """Validate the body of the given request. Ensures that the request contains
    a Json object and that this object contains at least the required keys and
    at most the required and optional keys. Returns the validated Json body.

    Raises NoJsonInRequest exception if request does not contain a Json object
    and InvalidRequest exception if a required key is missing or if a key is
    present that is not required or optional.

    Parameters
    ----------
    request: Http request
        The Http request object
    required: list(string), optional
        List of mandatory keys in the request body
    optional: list(string), optional
        List of optional keys in the request body

    Returns
    -------
    dict()
    """
    # Verify that the request contains a Json object
    if not request.json:
        raise NoJsonInRequest()
    obj = request.json
    # Ensure that all required elements are present
    possible_keys = []
    if not required is None:
        possible_keys = required
        for key in required:
            if not key in obj:
                raise InvalidRequest('missing element \'' + key + '\'')
    # Ensure that no unwanted elements are in the request body
    if not optional is None:
        possible_keys += optional
    for key in obj:
        if not key in possible_keys:
            raise InvalidRequest('unknown element \'' + key + '\'')
    return obj


# ------------------------------------------------------------------------------
#
# Main
#
# ------------------------------------------------------------------------------

if __name__ == '__main__':
    # Relevant documents:
    # http://werkzeug.pocoo.org/docs/middlewares/
    # http://flask.pocoo.org/docs/patterns/appdispatch/
    from werkzeug.serving import run_simple
    from werkzeug.wsgi import DispatcherMiddleware
    # Switch logging on
    import logging
    from logging.handlers import RotatingFileHandler
    log_dir = os.path.abspath(config.logs)
    # Create the directory if it does not exist
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    # File handle for server logs
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, 'vizier-webapi.log'),
        maxBytes=1024 * 1024 * 100,
        backupCount=20
    )
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    app.logger.addHandler(file_handler)
    # Logger for monitoring purposes. If the log_engine flag is false use a
    # null handler.
    logger = logging.getLogger(LOGGER_ENGINE)
    if config.settings.log_engine:
        monitor_handler = RotatingFileHandler(
            os.path.join(log_dir, 'engine.log'),
            maxBytes=1024 * 1024 * 100,
            backupCount=20
        )
        monitor_handler.setFormatter(
            logging.Formatter("%(message)s")
        )
        logger.addHandler(monitor_handler)
    else:
        logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.INFO)
    # Load a dummy app at the root URL to give 404 errors.
    # Serve app at APPLICATION_ROOT for localhost development.
    application = DispatcherMiddleware(Flask('dummy_app'), {
        app.config['APPLICATION_ROOT']: app,
    })
    run_simple(
        '0.0.0.0',
        config.api.server_local_port,
        application,
        use_reloader=config.debug
    )
