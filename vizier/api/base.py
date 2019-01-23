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

"""Collection of classes and methods that are shared between different web
server instances.
"""

from vizier.viztrail.base import PROPERTY_NAME


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
# Helper Methods
#
# ------------------------------------------------------------------------------

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



def validate_name(properties, message='not a valid name'):
    """Ensure that a name property (if given) is not None or the empty string.
    Will raise a ValueError in case of an invalid name.

    Parameters
    ----------
    properties: dict
        Dictionary of resource properties
    message: string, optional
        Error message if exception is thrown
    """
    if PROPERTY_NAME in properties:
        name = properties[PROPERTY_NAME]
        if not name is None:
            if name.strip() == '':
                raise ValueError(message)
