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

"""Interface for user-defined object properties.

Different types of objects in Vizier have user-defined properties associated
with them (e.g., viztrails and branches). In this module we define the interface
for the property manager that persistently manages properties for an individual
object.
"""

from abc import abstractmethod


class ObjectPropertiesSet(object):
    """Interface for accessing and updating user-defined properties that are
    associated with a Vizier object.
    """
    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError
