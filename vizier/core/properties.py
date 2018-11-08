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

"""Manager for objects with user-defined properties.

Different types of objects in Vizier have user-defined properties associated
with them (e.g., Worktrail and Branches). The classes in this module define
an interface and default implementation to persistently manage and update such
properties.
"""

from abc import abstractmethod
import os
import yaml


class ObjectProperty(object):
    """Object properties are (key, value)-pairs.

    Attributes
    ----------
    key: string
        Property key
    value: any
        Property value
    """
    def __init__(self, key, value):
        """Initialize the (key, value)-pair.

        Parameters
        ----------
        key: string
            Property key
        value: any
            Property value
        """
        self.key = key
        self.value = value

    @staticmethod
    def from_dict(doc):
        """Create an object property instance from a dictionary representation.

        Parameters
        ----------
        doc: dict
            Dictionary representaion of an object property

        Returns
        -------
        vizier.core.properties.ObjectProperty
        """
        return ObjectProperty(doc['key'], doc['value'])

    def to_dict(self):
        """Dictionary serialization of the (key, value)-pair.

        Returns
        -------
        dict
        """
        return {'key': self.key, 'value': self.value}


class ObjectPropertiesHandler(object):
    """Interface for accessing and updating user-defined properties that are
    associated with a Vizier object.
    """
    @abstractmethod
    def delete_properties(self):
        """Delete the full set of object properties. This method is usually
        called when the object itself is being deleted.
        """
        raise NotImplementedError

    @abstractmethod
    def get_properties(self):
        """Get the dictionary of user-defined properties that are associated
        with an object.

        Returns
        -------
        dict
        """
        raise NotImplementedError

    @abstractmethod
    def update_properties(self, properties):
        """Update the set of user-defined properties associated with an object.

        Existing properties will be replaced with corresponding entries
        in the given dictionary. If the value associated with a key in the
        argument dictionary is None the corresponding property will be deleted.
        Existing properties for which no new value is given are unaffected.

        Parameters
        ----------
        properties : dict
            Dictionary of properties that are to be updated
        """
        raise NotImplementedError


class FilePropertiesHandler(ObjectPropertiesHandler):
    """Default implementation for an object properties handler that stores the
    properties in a file in Yaml format.
    """
    def __init__(self, filename, properties=None):
        """Initialize the file that contains the properties. If the file does
        not exist it is assumed that the set of properties is not None. If the
        file exists the properties dictionary is used as a set of defaults.

        Parameters
        ----------
        filename: string
            Name of the file storing the object properties
        properties: dict, optional
            Initial set of default properties.
        """
        self.filename = os.path.abspath(filename)
        # Create the file from the given set of properties if it does not exist.
        if not os.path.isfile(self.filename):
            if properties is None:
                raise ValueError('missing default properties')
            self.properties = dict(properties)
            with open(self.filename, 'w') as f:
                yaml.dump(self.properties, f, default_flow_style=False)
        elif not properties is None:
            self.properties = dict(properties)
        else:
            self.properties = dict()

    def __getitem__(self, key):
        """Shortcut to be able to use the properties handler as a dictionary.

        Parameters
        ----------
        key: string
            Property key

        Returns
        -------
        Property value (any)
        """
        return self.get_properties()[key]

    def delete_properties(self):
        """Delete the file containing the properties.
        """
        if os.path.isfile(self.filename):
            os.remove(self.filename)

    def get_properties(self):
        """Get the dictionary of user-defined properties that are associated
        with an object.

        Returns
        -------
        dict
        """
        with open(self.filename, 'r') as f:
            properties = yaml.load(f.read())
        for key in self.properties:
            if not key in properties:
                properties[key] = self.properties[key]
        return properties

    def update_properties(self, properties):
        """Update the set of user-defined properties associated with an object.

        Existing properties will be replaced with corresponding entries
        in the given dictionary. If the value associated with a key in the
        argument dictionary is None the corresponding property will be deleted.
        Existing properties for which no new value is given are unaffected.

        Parameters
        ----------
        properties : dict
            Dictionary of properties that are to be updated
        """
        obj_properties = self.get_properties()
        # Update list of properties according to the given argument
        for key in properties:
            val = properties[key]
            # Delete the property if the given value is None
            if val is None:
                if key in obj_properties:
                    del obj_properties[key]
                # Also make sure to delete the property from the default properties
                if key in self.properties:
                    del self.properties[key]
            elif not val is None:
                obj_properties[key] = val
        # Write modified properties to file
        with open(self.filename, 'w') as f:
            yaml.dump(obj_properties, f, default_flow_style=False)
