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
from vizier.config.container import ContainerConfig
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.viztrail.command import ModuleCommand

import vizier.api.base as srv
import vizier.api.serialize.deserialize as deserialize
import vizier.api.serialize.labels as labels
import vizier.config.base as const


# -----------------------------------------------------------------------------
#
# App Initialization
#
# -----------------------------------------------------------------------------

"""Get application configuration parameters from environment variables."""
config = ContainerConfig()

# Create the app and enable cross-origin resource sharing
app = Flask(__name__)
app.config['APPLICATION_ROOT'] = config.webservice.app_path
app.config['DEBUG'] = config.run.debug
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
    return jsonify(api.service_descriptor)


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
      "id": "string",
      "command": {
        "packageId": "string",
        "commandId": "string",
        "arguments": [
          null
        ]
      },
      "context": [
        {
          "id": "string",
          "name": "string"
        }
      ],
      "resources": {}
    }
    """
    # Abort with BAD REQUEST if request body is not in Json format or does not
    # contain the expected elements.
    obj = srv.validate_json_request(
        request,
        required=[labels.ID, labels.COMMAND, labels.CONTEXT],
        optional=[labels.RESOURCES]
    )
    # Validate module command
    cmd = obj[labels.COMMAND]
    for key in [labels.COMMAND_PACKAGE, labels.COMMAND_ID, labels.COMMAND_ARGS]:
        if not key in cmd:
            raise srv.InvalidRequest('missing element \'' + key + '\' in command specification')
    # Get database state
    context = dict()
    for ds in obj[labels.CONTEXT]:
        for key in [labels.ID, labels.NAME]:
            if not key in ds:
                raise srv.InvalidRequest('missing element \'' + key + '\' in dataset identifier')
        context[ds[labels.NAME]] = ds[labels.ID]
    try:
        # Execute module. Result should not be None.
        result = api.tasks.execute_task(
            project_id=config.project_id,
            task_id=obj[labels.ID],
            command=ModuleCommand(
                package_id=cmd[labels.COMMAND_PACKAGE],
                command_id=cmd[labels.COMMAND_ID],
                arguments=cmd[labels.COMMAND_ARGS],
                packages=api.engine.packages
            ),
            context=context,
            resources=obj[labels.RESOURCES] if labels.RESOURCES in obj else None
        )
        return jsonify(result)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))


# ------------------------------------------------------------------------------
# Datasets
# ------------------------------------------------------------------------------

@app.route('/datasets', methods=['POST'])
def create_dataset():
    """Create a new dataset in the datastore for the project. The dataset
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
            project_id=config.project_id,
            columns=columns,
            rows=rows,
            annotations=annotations
        )
        return jsonify(dataset)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))


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
    column_id = request.args.get(labels.COLUMN, type=int)
    row_id = request.args.get(labels.ROW, type=int)
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


@app.route('/datasets/<string:dataset_id>/descriptor')
def get_dataset_descriptor(dataset_id):
    """Get the descriptor for the dataset with given identifier."""
    try:
        dataset = api.datasets.get_dataset_descriptor(
            project_id=config.project_id,
            dataset_id=dataset_id
        )
        if not dataset is None:
            return jsonify(dataset)
    except ValueError as ex:
        raise srv.InvalidRequest(str(ex))
    raise srv.ResourceNotFound('unknown dataset \'' + dataset_id + '\'')


@app.route('/datasets/<string:dataset_id>/annotations', methods=['POST'])
def update_dataset_annotation(dataset_id):
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
            project_id=config.project_id,
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
@app.route('/branches/<string:branch_id>/workflows/<string:workflow_id>/modules/<string:module_id>/charts/<string:chart_id>')
def get_dataset_chart_view(branch_id, workflow_id, module_id, chart_id):
    """Get content of a dataset chart view for a given workflow module.
    """
    try:
        view = api.views.get_dataset_chart_view(
            project_id=config.project_id,
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
            'unknown branch \'' + branch_id,
            '\', workflow \'' + workflow_id,
            '\', module \'' + module_id,
            '\' or chart \'' + chart_id + '\''
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
    # Initialize the Mimir gateway if using Mimir engine
    if config.engine.identifier == const.MIMIR_ENGINE:
        import vizier.mimir as mimir
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
        use_reloader=config.run.debug
    )
