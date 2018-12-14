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

"""Vizier Web Service - The web service is the main access point for the Vizier
front end and for any other (remote) clients. Implements the requests for the
Vizier Web API as documented in http://cds-swg1.cims.nyu.edu/vizier/api/v1/doc/.
"""

import os

from flask import Flask, jsonify, make_response, request, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename

from vizier.api.webservice.base import VizierApi
from vizier.config import AppConfig


# -----------------------------------------------------------------------------
#
# App Initialization
#
# -----------------------------------------------------------------------------

"""Read configuration parameter from a config file. The configuration file is
expected to be in JSON or YAML format. Attempts first to read the file specified
in the environment variable VIZIERSERVER_CONFIG first. If the variable is not
set (or if the specified file does not exist) an attempt is made to read file
config.yaml in the current working directory. If neither exists the the default
configuration is used.
"""
config = AppConfig()

# Create the app and enable cross-origin resource sharing
app = Flask(__name__)
app.config['APPLICATION_ROOT'] = config.webservice.app_path
app.config['DEBUG'] = config.debug
# Set size limit for uploaded files
app.config['MAX_CONTENT_LENGTH'] = config.webservice.defaults.max_file_size

CORS(app)

# Get the Web Service API.
api = VizierApi(config)


# ------------------------------------------------------------------------------
#
# Routes
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Service
# ------------------------------------------------------------------------------
@app.route('/')
def service_descriptor():
    """Retrieve essential information about the web service including relevant
    links to access resources and interact with the service.
    """
    return jsonify(api.service_overview())


# ------------------------------------------------------------------------------
# Projects
# ------------------------------------------------------------------------------
@app.route('/projects')
def list_projects():
    """Get a list of descriptors for all projects that are currently being
    managed by the API.
    """
    return jsonify(api.list_projects())


@app.route('/projects', methods=['POST'])
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
    obj = validate_json_request(request, required=['properties'])
    if not isinstance(obj['properties'], list):
        raise InvalidRequest('expected a list of properties')
    properties = convert_properties(obj['properties'])
    # Create project and return the project descriptor
    try:
        return jsonify(api.create_project(properties)), 201
    except ValueError as ex:
        raise InvalidRequest(str(ex))


@app.route('/projects/<string:project_id>')
def get_project(project_id):
    """Retrieve information for project with given identifier."""
    # Retrieve project serialization. If project does not exist the result
    # will be none.
    pj = api.get_project(project_id)
    if not pj is None:
        return jsonify(pj)
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


@app.route('/projects/<string:project_id>', methods=['DELETE'])
def delete_project(project_id):
    """Delete an existing project."""
    if api.delete_project(project_id):
        return '', 204
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


@app.route('/projects/<string:project_id>', methods=['PUT'])
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
    obj = validate_json_request(request, required=['properties'])
    if not isinstance(obj['properties'], list):
        raise InvalidRequest('expected a list of properties')
    # Update project and return the project descriptor. If no project with
    # the given identifier exists the update result will be None
    properties = convert_properties(obj['properties'], allow_null=True)
    try:
        pj = api.update_project(project_id, properties)
        if not pj is None:
            return jsonify(pj)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


# ------------------------------------------------------------------------------
# Branches
# ------------------------------------------------------------------------------
@app.route('/projects/<string:project_id>/branches', methods=['POST'])
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
    obj = validate_json_request(request, required=['properties'], optional=['source'])
    # Get the branch point. If the source is given the dictionary should at
    # most contain the three identifier
    branch_id = None
    workflow_id = None
    module_id = None
    if 'source' in obj:
        for key in obj:
            if key == 'branchId':
                branch_id = obj[key]
            elif key == 'workflowId':
                workflow_id = obj[key]
            elif key ==  'moduleId':
                module_id = obj[key]
            else:
                raise InvalidRequest('invalid element \'' + key + '\' for branch point')
    # Get the properties for the new branch
    properties = convert_properties(obj['properties'])
    # Create a new workflow. The result is the descriptor for the new workflow
    # or None if the specified project does not exist. Will raise a ValueError
    # if the specified workflow version or module do not exist
    try:
        branch = api.create_branch(
            project_id=project_id,
            branch_id=branch_id,
            workflow_id=workflow_id,
            module_id=module_id,
            properties=properties
        )
        if not branch is None:
            return jsonify(branch)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>', methods=['DELETE'])
def delete_branch(project_id, branch_id):
    """Delete branch from a given project."""
    try:
        success = api.delete_branch(project_id=project_id, branch_id=branch_id)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    if success:
        return '', 204
    raise ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>', methods=['PUT'])
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
    obj = validate_json_request(request, required=['properties'])
    # Update properties for the given branch and return branch descriptor.
    properties = convert_properties(obj['properties'], allow_null=True)
    try:
        # Result is None if project or branch are not found.
        branch = api.update_branch(
            project_id=project_id,
            branch_id=branch_id,
            properties=properties
        )
        if not branch is None:
            return jsonify(branch)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    raise ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


# ------------------------------------------------------------------------------
# Workflows
# ------------------------------------------------------------------------------

@app.route('/projects/<string:project_id>/branches/<string:branch_id>/head')
def get_branch_head(project_id, branch_id):
    """Get handle for a workflow at the HEAD of a given project branch."""
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    workflow = api.get_workflow(project_id=project_id, branch_id=branch_id)
    if not workflow is None:
        return jsonify(workflow)
    raise ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/head', methods=['POST'])
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
    cmd = validate_json_request(
        request,
        required=['packageId', 'commandId', 'arguments']
    )
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project or branch are not found
        module = api.append_workflow_module(
            project_id=project_id,
            branch_id=branch_id,
            package_id=cmd['packageId'],
            command_id=cmd['commandId'],
            arguments=cmd['arguments']
        )
        if not module is None:
            return jsonify(module)
    except ValueError as ex:
        raise InvalidRequest(str(ex))
    raise ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/head/cancel', methods=['POST'])
def cancel_workflow(project_id, branch_id):
    """Cancel execution for all running and pending modules in the head
    workflow of a given project branch.
    """
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    workflow = api.cancel_workflow(
        project_id=project_id,
        branch_id=branch_id
    )
    if not workflow is None:
        return jsonify(workflow)
    raise ResourceNotFound('unknown project \'' + project_id + '\' or branch \'' + branch_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<string:workflow_id>')
def get_workflow(project_id, branch_id, workflow_id):
    """Get handle for a workflow in a given project branch."""
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    workflow = api.get_workflow(
        project_id=project_id,
        branch_id=branch_id,
        workflow_id=workflow_id
    )
    if not workflow is None:
        return jsonify(workflow)
    raise ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or workflow \'' + workflow_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/head/modules/<string:module_id>')
def get_workflow_module(project_id, branch_id, module_id):
    """Get handle for a module in the head workflow of a given project branch.
    """
    # Get the workflow handle. The result is None if the project, branch or
    # workflow do not exist.
    module = api.get_workflow_module(
        project_id=project_id,
        branch_id=branch_id,
        module_id=module_id
    )
    if not module is None:
        return jsonify(module)
    raise ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or module \'' + module_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/head/modules/<string:module_id>', methods=['DELETE'])
def delete_workflow_module(project_id, branch_id, module_id):
    """Delete a module in the head workflow of a given project branch."""
    result = api.delete_workflow_module(
        project_id=project_id,
        branch_id=branch_id,
        module_id=module_id
    )
    if not result is None:
        return jsonify(result)
    raise ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or module \'' + module_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/head/modules/<string:module_id>', methods=['POST'])
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
    cmd = validate_json_request(
        request,
        required=['packageId', 'commandId', 'arguments']
    )
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project, branch or module are not found.
        modules = api.insert_workflow_module(
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
        raise InvalidRequest(str(ex))
    raise ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or module \'' + module_id + '\'')


@app.route('/projects/<string:project_id>/branches/<string:branch_id>/head/modules/<string:module_id>', methods=['PUT'])
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
    cmd = validate_json_request(
        request,
        required=['packageId', 'commandId', 'arguments']
    )
    # Extend and execute workflow. This will throw a ValueError if the command
    # cannot be parsed.
    try:
        # Result is None if project, branch or module are not found.
        modules = api.replace_workflow_module(
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
        raise InvalidRequest(str(ex))
    raise ResourceNotFound('unknown project \'' + project_id + '\' branch \'' + branch_id + '\' or module \'' + module_id + '\'')


# ------------------------------------------------------------------------------
# Files
# ------------------------------------------------------------------------------

@app.route('/projects/<string:project_id>/files', methods=['POST'])
def upload_file(project_id):
    """Upload file (POST) - Upload a data file to the project's filestore.
    """
    # The upload request may contain a file object or an Url from where to
    # download the data.
    if request.files and 'file' in request.files:
        file = request.files['file']
        # A browser may submit a empty part without filename
        if file.filename == '':
            raise InvalidRequest('empty file name')
        # Save uploaded file to temp directory
        filename = secure_filename(file.filename)
        try:
            f_handle = api.upload_file(
                project_id=project_id,
                file=file,
                file_name=filename
            )
            if not f_handle is None:
                return jsonify(f_handle), 201
        except ValueError as ex:
            raise InvalidRequest(str(ex))
    else:
        raise InvalidRequest('no file or url specified in request')
    raise ResourceNotFound('unknown project \'' + project_id + '\'')


@app.route('/projects/<string:project_id>/files/<string:file_id>')
def download_file(project_id, file_id):
    """Download file from file server."""
    # Get handle for file from the project's filestore
    f_handle = api.get_file(project_id, file_id)
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
    raise ResourceNotFound('unknown project \'' + project_id + '\' or file \'' + file_id + '\'')


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

def convert_properties(properties, allow_null=False):
    """Convert a list of properties from request format into a dictionary.

    Raises InvalidRequest if an invalid list of properties is given.

    Parameters
    ----------
    properties: list
        List of key,value pairs defining object properties
    allow_null: bool, optional
        Allow None values for properties if True

    Returns
    -------
    dict()
    """
    result = dict()
    for prop in properties:
        if not isinstance(prop, dict):
            raise InvalidRequest('expected property to be a dictionary')
        name = None
        value = None
        for key in prop:
            if key  == 'key':
                name = prop[key]
            elif key == 'value':
                value = prop[key]
            else:
                raise InvalidRequest('invalid property element \'' + key + '\'')
        if name is None:
            raise InvalidRequest('missing element \'key\' in property')
        if value is None and not allow_null:
            raise InvalidRequest('missing property value for \'' + name + '\'')
        result[name] = value
    return result


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
# Initialize
#
# ------------------------------------------------------------------------------

@app.before_first_request
def initialize():
    """Initialize the API before the first request. This can help avoid loading
    data twice since the API object is usually instatiated twice when the server
    starts.
    """
    api.init()


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
    log_dir = os.path.abspath(config.logs.server)
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
    # Load a dummy app at the root URL to give 404 errors.
    # Serve app at APPLICATION_ROOT for localhost development.
    application = DispatcherMiddleware(Flask('dummy_app'), {
        app.config['APPLICATION_ROOT']: app,
    })
    run_simple(
        '0.0.0.0',
        config.webservice.server_local_port,
        application,
        use_reloader=config.debug
    )
