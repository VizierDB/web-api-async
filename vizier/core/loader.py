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

"""The class loader is used to create object instances for different
configuration options.
"""

import importlib


class ClassLoader(object):
    """The class loader is used to instantiate an object from a specified class
    in a uniform way. The class expects a dictionary that contains at least
    the two elements 'moduleName' and 'className'. The values for these elements
    are used to identify the class that is to be imported.

    The values dictionary may contain another element 'properties'. The value
    of the properties element is passed to the new object as argument when a
    class is instantiated.
    """
    def __init__(self, values):
        """Initialize the class loader. Raises ValueError if either of the
        mandatory elements is not present.

        Parameters
        ----------
        values: dict
            Dictionary that specifies a new class instance
        """
        # Ensure that the mandatory elements are present in the dictionary
        if 'moduleName' in values:
            self.module_name = values['moduleName']
        else:
            raise ValueError('missing \'moduleName\' in class specification')
        if 'className' in values:
            self.class_name = values['className']
        else:
            raise ValueError('missing \'className\' in class specification')
        # The properties element is optional.
        if 'properties' in values:
            self.properties = values['properties']
        else:
            self.properties = None

    def get_instance(self, properties=None, **kwargs):
        """Get an instance of the defined class. If the properties argument is
        not None it overwrites the properties that were extracted from the
        values dictionary. Returns a new instance of the specified class.

        Parameters
        ----------
        properties: dict
            Dictionary of properties passed to the new class instance.

        Returns
        -------
        object
        """
        module = importlib.import_module(self.module_name)
        if not properties is None:
            return getattr(module, self.class_name)(properties=properties, **kwargs)
        elif not self.properties is None:
            return getattr(module, self.class_name)(properties=self.properties, **kwargs)
        else:
            return getattr(module, self.class_name)(**kwargs)


    @staticmethod
    def to_dict(module_name, class_name, properties=None):
        """Create a dictionary serialization from which an instance of the
        class loader can be created.

        Parameters
        ----------
        module_name: string
            Name of the Python module that contains the reference class
        class_name: string
            Name of the class that is being referenced
        properties: dict, optional
            Optional dictionary of class-specific configuration parameters

        Returns
        -------
        dict()
        """
        obj = {'moduleName': module_name, 'className': class_name}
        if not properties is None:
            obj['properties'] = properties
        return obj


class DummyClass(object):
    """Dummy class for test purposes."""
    def __init__(self, api=None, properties=None, names=None):
        self.properties = properties
        self.names = names
