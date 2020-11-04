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

"""Classes and helper methods for configration objects."""

import os
from typing import Tuple, List, Optional, Dict, Any

"""Default directory for all server resources."""
ENV_DIRECTORY = './.vizierdb'


"""Identifier for supported backends."""
BACKEND_CELERY = 'CELERY'
BACKEND_CONTAINER = 'CONTAINER'
BACKEND_MULTIPROCESS = 'MULTIPROCESS'

BACKENDS = [BACKEND_CELERY, BACKEND_CONTAINER, BACKEND_MULTIPROCESS]


"""Default engines."""
CONTAINER_ENGINE = 'CLUSTER'
DEV_ENGINE = 'DEV'
HISTORE_ENGINE = 'HISTORE'
MIMIR_ENGINE = 'MIMIR'

ENGINES = [DEV_ENGINE, HISTORE_ENGINE, MIMIR_ENGINE]

"""Supported attribute types."""
BOOL = 'bool'
FLOAT = 'float'
INTEGER = 'int'
LIST = 'list'  # List of integers
STRING = 'str'

ATTRIBUTE_TYPES = [BOOL, FLOAT, INTEGER, LIST, STRING]

"""Configuration default values for various properties."""
DEFAULT_MAX_ROW_LIMIT = 1000
DEFAULT_MAX_DOWNLOAD_ROW_LIMIT = 5000

class ConfigObject(object):
    """Object whose attributes contain the values of environment variables that
    are used by vizier for to configure API components.
    """
    def __init__(self, 
            attributes:List[Tuple[str, Optional[str], str]], 
            default_values: Optional[Dict[str, Any]] = None
        ):
        """Initialize the object from a list of 3-tuples containing (1) the
        attribute name, (2) the name of the environment variable that contains
        the attibute value, and (3) the attibute type (string, bool ,float, or
        int).

        If the environment variable is not set (or the name of the environemnt
        variable is None), the value will be taken from the default values
        dictionary. If the default values are not given the global default
        dictionary is used.

        Parameters
        ----------
        attributes: list
            List of tuples containing attribute name, name of the environment
            variable, and attribute type.
        default_values: dict, optional
            Dictionary of default values.
        """
        for attr in attributes:
            name, var, type = attr
            val = get_config_value(
                attribute_name=name,
                attribute_type=type,
                env_variable=var,
                default_values=default_values
            )
            setattr(self, name, val)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_config_value(
        env_variable: Optional[str], 
        attribute_name: Optional[str] = None, 
        attribute_type: str = STRING, 
        default_values: Optional[Dict[str, Any]] = None
    ) -> Any:
    """Get the value for a configuration parameter. The value is expected to be
    set in the given environment variable. If the variable is not set the value
    is taken from the given default settings or the global default settings.

    If the environment variable name is None the attribute name is used to
    get the value from the default values or the global default settings.

    Raises ValueError if the attribute type is invalid.

    Parameters
    ----------
    env_variable: string
        Name of the environment variable
    attribute_name: string
        Name of the attibute that is being set
    attribute_type: string
        Expected type of the attribute value
    default_values: dict, optional
        Dictionary of default values.

    Returns
    -------
    scalar value
    """
    # Raise an exception if the attribute type is unknown
    if attribute_type not in ATTRIBUTE_TYPES:
        raise ValueError('unknown attribute type \'' + str(attribute_type) + '\' for \'' + str(attribute_name) + '\'')
    # Get the value for the environment variable. If the variable is
    # None or if the variable is not set use the default settings
    val: Any = None
    if env_variable is not None:
        val = os.getenv(env_variable)
        if val is None or val.strip() == '':
            if default_values is not None and env_variable in default_values:
                val = default_values[env_variable]
    elif default_values is not None and attribute_name in default_values:
        val = default_values[attribute_name]
    else:
        val = None
    if val is not None:
        if attribute_type == BOOL and not isinstance(val, bool):
            val = val.lower() == 'true'
        elif attribute_type == FLOAT and not isinstance(val, float):
            try:
                val = float(val)
            except ValueError:
                raise ValueError('expected float value for \'' + str(env_variable) + '\'')
        elif attribute_type == INTEGER and not isinstance(val, int):
            try:
                val = int(val)
            except ValueError:
                raise ValueError('expected integer value for \'' + str(env_variable) + '\'')
        elif attribute_type == LIST and isinstance(val, str):
            int_list = list()
            try:
                for token in val.split(','):
                    if '-' in token:
                        start = int(token[:token.find('-')].strip())
                        end = int(token[token.find('-')+1:].strip())
                        int_list.extend(list(range(int(start), int(end))))
                    else:
                        int_list.append(int(token))
            except ValueError:
                raise ValueError('expected integer list for \'' + str(env_variable) + '\'')
            val = int_list
    return val
