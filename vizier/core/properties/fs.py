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

"""Implementation for an object properties set that is persistet to disk as a
file in Json format.
"""

import json
import os

from vizier.core.properties.base import ObjectPropertiesSet


class PersistentObjectProperties(ObjectPropertiesSet):
    """Object properties set that is being materialized as a Json file on disk.
    Maintains a copy of the properties in memory as a dictinary. Every property
    update is written to disk immediately. The assumption here is that updates
    of object properties are rare events.
    """
    def __init__(self, filename, properties=None):
        """Initialize the file that maintains the properties. Properties are
        read from file (if it exists) and manitained in an internal dictionary.

        If the file does not exist it is assumed that the set of default
        properties is not None. If the file exists the default properties are
        expected to be None. Proeprties are read from file.

        Parameters
        ----------
        filename: string
            Path to properties file
        """
        self.filename = os.path.abspath(filename)
        # Create the file from the given set of properties if it does not exist.
        if not os.path.isfile(self.filename):
            if properties is None:
                self.properties = dict()
            else:
                self.properties = dict(properties)
            write_properties(self.filename, self.properties)
        elif not properties is None:
            raise ValueError('cannot set default properties for existing set')
        else:
            self.properties = dict()
            with open(self.filename, 'r') as f:
                obj = json.load(f)
                for prop in obj:
                    self.properties[prop['key']] = prop['value']

    def delete_property(self, key):
        """Delete the property with the given key. Returns True if a property
        with the given key existed and False otherwise.

        Parameters
        ----------
        key: string
            Unique property key

        Returns
        -------
        bool
        """
        if key in self.properties:
            self.set_property(key)
            return True
        return False

    def get_property(self, key, default_value=None):
        """Get the value for the property with the given key. If no property
        with the given key exists the default value is returned.

        Parameters
        ----------
        key: string
            Unique property key
        default_value: any, optional
            Default value that is returned if the property has not been set

        Returns
        -------
        any
        """
        if key in self.properties:
            return self.properties[key]
        else:
            return default_value

    def set_property(self, key, value=None):
        """Update the value for the property with the given key. If the value
        is None the property will be deleted.

        Parameters
        ----------
        key: string
            Unique property key
        value: any, optional
            New property value
        """
        if key in self.properties:
            if value is None:
                del self.properties[key]
            else:
                self.properties[key] = value
        else:
            self.properties[key] = value
        # Write modified properties set to file
        write_properties(self.filename, self.properties)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def write_properties(filename, properties):
    """Write properties dictionary to file.

    Parameters
    ----------
    filename: string
        Path to the output file
    properties: dict()
        Dictionary of properties
    """
    with open(filename, 'w') as f:
        json.dump([{'key': key, 'value': properties[key]} for key in properties], f)
