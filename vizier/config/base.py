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

"""Classes and helper methods for configration objects."""

import json
import os
import yaml


"""Default directory for API data."""
ENV_DIRECTORY = './.vizierdb'


class ConfigObject(object):
    """Create a Python class instance from a given dictionary of variables and
    a dictionary of default values. Performs a nested merge of dictionaries in
    values and default values.

    For lists the policy is all or nothing. List elements are not merged. If the
    configuration object contains a list value all items in the list are copied
    to the configuration as is.
    """
    def __init__(self, values, default_values=dict()):
        """Initialize the object from the given dictionary of values and default
        values.

        Parameters
        ----------
        values: dict
            Dictionary of object variables. Dictionary keys are used as variable
            names.
        default_values: dict
            Dictionary of default values.
        """
        #print json.dumps(values, sort_keys=True, indent=4)
        #print json.dumps(default_values, sort_keys=True, indent=4)
        self.keys = set()
        for key in values:
            if isinstance(values[key], dict):
                if key in default_values:
                    def_val = default_values[key]
                    if not isinstance(def_val, dict):
                        def_val = dict()
                else:
                    def_val = dict()
                setattr(
                    self,
                    key,
                     ConfigObject(values[key], default_values=def_val)
                )
            elif isinstance(values[key], list):
                elements = list()
                for el in values[key]:
                    if isinstance(el, dict):
                        elements.append(ConfigObject(el))
                    else:
                        elements.append(el)
                setattr(self, key,  elements)
            else:
                setattr(self, key,  values[key])
            self.keys.add(key)
        for key in default_values:
            if not key in self.keys:
                if isinstance(default_values[key], dict):
                    setattr(
                        self,
                        key,
                        ConfigObject(dict(), default_values=default_values[key])
                    )
                elif isinstance(default_values[key], list):
                    elements = list()
                    for el in default_values[key]:
                        if isinstance(el, dict):
                            elements.append(ConfigObject(el))
                        else:
                            elements.append(el)
                    setattr(self, key,  elements)
                else:
                    setattr(self, key,  default_values[key])
                self.keys.add(key)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def read_object_from_file(filename):
    """Read dictionary serialization from file. The file format is expected to
    by Yaml unless the filename ends with .json.

    Parameters
    ----------
    filename: string
        Name of the input file

    Returns
    -------
    dict
    """
    with open(filename, 'r') as f:
        if filename.endswith('.json'):
            return json.loads(f.read())
        else:
            return yaml.load(f.read())
