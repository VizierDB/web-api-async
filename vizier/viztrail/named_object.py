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

from vizier.core.annotation.base import ObjectAnnotationSet

"""Key's for default viztrail properties."""
# Human readable object name for viztrails and viztrail branches
PROPERTY_NAME = 'name'



class NamedObject(object):
    """Viztrails and branches are named objects. A named object maintains a set
    of user-defined annotations. The annotations with the key defined in
    PROPERTY_NAME is interpreted as the human-readable object name.

    This base class provides getter and setter methods to access and manipulate
    the human-readable object name.

    Attributes
    ----------
    name: string
        Human readable viztrail name
    properties: dict(string, any)
        Set of user-defined properties that are associated with this viztrail
    """
    def __init__(self, properties: ObjectAnnotationSet):
        """Initialize the object's properties set.

        Parameters
        ----------
        properties: dict(string, any)
            Handler for user-defined properties
        """
        if properties is None:
            raise Exception("NO PROPERTIES SET")
        self.properties = properties

    @property
    def name(self):
        """Get the value of the object property with key 'name'. The result is
        None if no such property exists.

        Returns
        -------
        string
        """
        return self.properties.get(PROPERTY_NAME, None)

    @name.setter
    def name(self, value):
        """Set the value of the object property with key 'name'.

        Parameters
        ----------
        name: string
            Human-readable name for the viztrail
        """
        self.properties[PROPERTY_NAME] = str(value)