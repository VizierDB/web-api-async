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

"""Vizier Container Web Service - The web service is the main access point for
Vizier backends that run individual projects in their own containers as well as
the front end to access datasets and upload files. Implements the requests for
the Vizier Container Web API that are documented in
http://cds-swg1.cims.nyu.edu/doc/vizier-db-container/.
"""

import csv
import os
import StringIO

from flask import Flask, jsonify, make_response, request, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename

from vizier.api.routes.base import PAGE_LIMIT, PAGE_OFFSET
from vizier.api.webservice.container.base import VizierContainerApi
from vizier.config.container import ContainerAppConfig

import vizier.api.base as srv
import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.labels as labels


# -----------------------------------------------------------------------------
#
# App Initialization
#
# -----------------------------------------------------------------------------

"""Read configuration parameter from a config file. The configuration file is
expected to be in JSON or YAML format. Attempts first to read the file specified
in the environment variable VIZIERCONTAINERSERVER_CONFIG first. If the variable
is not set (or if the specified file does not exist) an attempt is made to read
the file config.json or config.yaml in the current working directory. If neither
exists an exception is thrown.
"""
config = ContainerAppConfig()

# Create the app and enable cross-origin resource sharing
app = Flask(__name__)
app.config['APPLICATION_ROOT'] = config.webservice.app_path
app.config['DEBUG'] = config.debug
# Set size limit for uploaded files
app.config['MAX_CONTENT_LENGTH'] = config.webservice.defaults.max_file_size

CORS(app)

# Get the Web Service API.
api = VizierContainerApi(config)


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
# Task
# ------------------------------------------------------------------------------
@app.route('/tasks/<string:task_id>/cancel', methods=['POST'])
def cancel_task(task_id):
    """Cancel execution for a given task."""
    # Cancel the given task. The result is None if no task with the given
    # identifier exists.
    result = api.tasks.cancel_task(task_id=task_id)
    if not result is None:
        return jsonify(result)
    raise srv.ResourceNotFound('unknown task \'' + task_id + '\'')


@app.route('/tasks/execute', methods=['POST'])
def execute_task():
    """Execute a task against a given project context.

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
            project_id=config.project_id,
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
        dataset = api.datasets.get_dataset(
            project_id=config.project_id,
            dataset_id=dataset_id,
            offset=request.args.get(PAGE_OFFSET),
            limit=request.args.get(PAGE_LIMIT)
        )
        if not dataset is None:
            return jsonify(dataset)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown dataset \'' + dataset_id + '\'')


@app.route('/datasets/<string:dataset_id>/annotations')
def get_dataset_annotations(dataset_id):
    """Get annotations that are associated with the given dataset.
    """
    # Expects at least a column or row identifier
    column_id = request.args.get(labels.COLUMN, -1, type=int)
    row_id = request.args.get(labels.ROW, -1, type=int)
    if column_id < 0 and row_id < 0:
        raise srv.InvalidRequest('missing identifier for column or row')
    # Get annotations for dataset with given identifier. The result is None if
    # no dataset with given identifier exists.
    annotations = api.datasets.get_annotations(
        project_id=config.project_id,
        dataset_id=dataset_id,
        column_id=column_id,
        row_id=row_id
    )
    if not annotations is None:
        return jsonify(annotations)
    raise srv.ResourceNotFound('unknown dataset \'' + dataset_id + '\'')


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
    obj = srv.validate_json_request(
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
    annotations = api.datasets.update_annotation(
        project_id=config.project_id,
        dataset_id=dataset_id,
        column_id=column_id,
        row_id=row_id,
        anno_id=anno_id,
        key=key,
        value=value
    )
    if not annotations is None:
        return jsonify(annotations)
    raise srv.ResourceNotFound('unknown dataset \'' + dataset_id + '\'')


@app.route('/datasets/<string:dataset_id>/csv')
def download_dataset(dataset_id):
    """Get the dataset with given identifier in CSV format.
    """
    # Get the handle for the dataset with given identifier. The result is None
    # if no dataset with given identifier exists.
    _, dataset = api.datasets.get_dataset_handle(config.project_id, dataset_id)
    if dataset is None:
        raise srv.ResourceNotFound('unknown dataset \'' + dataset_id + '\'')
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
@app.route('/projects/<string:project_id>/branches/<string:branch_id>/workflows/<string:workflow_id>/modules/<string:module_id>/views/<string:view_id>')
def get_dataset_chart_view(project_id, branch_id, workflow_id, module_id, view_id):
    """Get content of a dataset chart view for a given workflow module.
    """
    try:
        view = api.get_dataset_chart_view(
            project_id=None,
            branch_id=branch_id,
            workflow_id=workflow_id,
            module_id=module_id,
            view_id=view_id
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
            '\' or view \'' + view_id + '\''
        ])
    )


# ------------------------------------------------------------------------------
# Files
# ------------------------------------------------------------------------------

@app.route('/files', methods=['POST'])
def upload_file():
    """Upload file (POST) - Upload a data file to the filestore."""
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
                project_id=config.project_id,
                file=file,
                file_name=filename
            )
            return jsonify(f_handle), 201
        except ValueError as ex:
            raise srv.InvalidRequest(str(ex))
    else:
        raise srv.InvalidRequest('no file or url specified in request')


@app.route('/files/<string:file_id>')
def download_file(file_id):
    """Download file from file server."""
    # Get handle for file from the filestore
    f_handle = api.files.get_file(project_id=config.project_id, file_id=file_id)
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
    raise srv.ResourceNotFound('unknown file \'' + file_id + '\'')


# ------------------------------------------------------------------------------
#
# Error Handler
#
# ------------------------------------------------------------------------------

@app.errorhandler(srv.ServerRequestException)
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
