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

"""Vizier web service application object. We currently use Flask microframework
to build the web service. The web service is the main access point for the
Vizier front end and for any other (remote) clients.
"""

import logging
import os

from flask import Flask, jsonify, make_response
from flask_cors import CORS
from logging.handlers import RotatingFileHandler

from vizier.config.app import AppConfig

import vizier.api.base as srv
import vizier.config.base as const


def create_app():
    """Factory pattern for Flask. Initialize the Flask application object.

    Returns
    -------
    Flask
    """
    #Get application configuration parameters from environment variables.
    config = AppConfig()
    # Create the app and enable cross-origin resource sharing
    app = Flask(__name__)
    #app.config['APPLICATION_ROOT'] = config.webservice.app_path
    #app.config['DEBUG'] = True
    # Set size limit for uploaded files
    app.config['MAX_CONTENT_LENGTH'] = config.webservice.defaults.max_file_size
    # Enable CORS
    CORS(app)
    # Switch logging on
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

    # --------------------------------------------------------------------------
    #
    # Error Handler
    #
    # --------------------------------------------------------------------------

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

    # Register the API blueprint
    from . import server
    app.register_blueprint(server.bp)
    # Return the applicatio object

    # --------------------------------------------------------------------------
    #
    # Initialize
    #
    # --------------------------------------------------------------------------

    @app.before_first_request
    def initialize():
        """Initialize Mimir gateway (if necessary) before the first request.
        """
        # Initialize the Mimir gateway if using Mimir engine
        if config.engine.identifier == const.MIMIR_ENGINE:
            import vizier.mimir as mimir

    return app
