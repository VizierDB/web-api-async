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

"""Interface for user-defined object annotations. Different types of objects in
Vizier have user-defined annotations associated with them (e.g., datasets and
viztrails). Annotations are (key,value) pairs. For each key, an object may be
annotated with multiple distinct values, e.g., to allow multiple tags to be
associated with an object. Values are scalars, i.e., string, int, or float
values.

In this module we define the interface for the class that manages annotations
for an individual object in a persistent manner.
"""

from abc import abstractmethod


class ObjectAnnotationSet(object):
    """Interface for accessing and manipulating user-defined annotations.
    Annotations are (key,value) pairs. For each key we maintain a list of
    multiple distinct values.
    """
    @abstractmethod
    def add(self, key, value, replace=False):
        """Associate the given key with the given value. If the replace flag is
        True all other values that are currently associated with the key are
        removed. If the replace flag is False the value will be added to the
        set of values that are associated with the key.

        Parameters
        ----------
        key: string
            Unique property key
        value: scalar
            New property value
        replace: bool, optional
            Replace all previously associated values for key if True
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, key, value=None):
        """Delete the annotations for the given key. If the optional value is
        None all annotated values for the given key are removed. If the value is
        not None only the resulting (key,value) pair will be removed.

        Returns True if at least one mathcing annotation was removed and False
        oterwise.

        Parameters
        ----------
        key: string
            Unique property key
        value: scalar, optional

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def find_all(self, key, default_value=[]):
        """Get the list of values that are associated with the given key. If no
        value is associated with the key the default value is returned.

        Parameters
        ----------
        key: string
            Unique property key
        default_value: list, optional
            Default value that is returned if the property has not been set

        Returns
        -------
        list
        """
        raise NotImplementedError

    @abstractmethod
    def find_one(self, key, default_value=None, raise_error_on_multi_value=True):
        """Get a single value that is associated with the given key. If no value
        is associated with the key the default value is returned. If multiple
        values are associated with the given key a ValueError is raised unless
        the raise_error_on_multi_value flag is False. In the latter case one of
        the associated values is returned.

        Parameters
        ----------
        key: string
            Unique property key
        default_value: any, optional
            Default value that is returned if the property has not been set
        raise_error_on_multi_value: bool, optional
            Raises a ValueError if True and the given key is associated with
            multiple values.

        Returns
        -------
        scalar
        """
        raise NotImplementedError

    def replace(self, key, value):
        """Update the value for the property with the given key. If the value
        is None the property will be deleted.

        Parameters
        ----------
        key: string
            Unique property key
        value: any, optional
            New property value
        """
        self.add(key=key, value=value, replace=True)

    @abstractmethod
    def update(self, properties):
        """Update the current properties based on the values in the dictionary.

        The (key, value)-pairs in the properties dictionary define the update
        operations. Values are expected to be either None, a scalar value (i.e.,
        int, float, or string) or a list of scalar values. If the value is None
        the corresponding project property is deleted. Otherwise, the
        corresponding property will be replaced by the value or the values in a
        given list of values.
        """
        raise NotImplementedError


class AnnotationStore(object):
    """Interface for ann otation store that manages persistent object
    annotations.
    """
    @abstractmethod
    def store(self, annotations):
        """Persist the given dictionary of object annotations. Each key in the
        dictionary is expected to be associated with either a scalar value or
        a list of scalar values.

        Parameters
        ----------
        annotations: dict, optional
            Dictionary of object annotations
        """
        raise NotImplementedError

    @abstractmethod
    def values(self):
        """Get a dictionary of key,value pairs that contains all annotations.

        Returns
        -------
        dict()
        """
        raise NotImplementedError


class DefaultAnnotationSet(ObjectAnnotationSet):
    """Default implementation for the object annotation set. Maintains a
    dictionary of keys. With each key either a scalar value or a list of scalar
    values is associated.

    An optional AnnotationStore is used to persist the annotation set whenever
    if is updated by a call to the .add() or .delete() method.
    """
    def __init__(self, elements=None, writer=None):
        """Initialize the set of annotations. Expects a dictionary of keys
        where values are either scalars or list of scalars.

        Parameters
        ----------
        elements: dict, optional
            Dictionary of annotated keys
        writer: vizier.core.annotation.AnnotationStore optional
            Optional store to persist changes
        """
        self.elements = elements if not elements is None else dict()
        self.writer = writer

    def add(self, key, value, replace=False, persist=True):
        """Associate the given key with the given value. If the replace flag is
        True all other values that are currently associated with the key are
        removed. If the replace flag is False the value will be added to the
        set of values that are associated with the key.

        The optional persist flag allows for bulk update without writing changes
        to disk after each individual update.

        Parameters
        ----------
        key: string
            Unique property key
        value: scalar
            New property value
        replace: bool, optional
            Replace all previously associated values for key if True
        persist: bool, optional
            Flag indicating whether the changes are to be persisted immediately
        """
        # Ensure that the value is a scalar value
        if value is None or not type(value) in [int, float, str, basestring, unicode]:
            raise ValueError('invalid annotation value type \'' + str(type(value)) + '\'')
        # Set the value if the replace flag is True or no prior annotation for
        # the given key exists
        if not key in self.elements:
            self.elements[key] = value
        else:
            # Check if any of the annotated values matches the given value. In
            # case that is the case return without any changes
            el = self.elements[key]
            if isinstance(el, list):
                if replace:
                    self.elements[key] = value
                else:
                    # Find the given value in the list of associated values
                    for i in range(len(el)):
                        if el[i] == value:
                            # No changes
                            return
                    el.append(value)
            elif el == value:
                # No changes
                return
            else:
                if replace:
                    self.elements[key] = value
                else:
                    # Prior annotation is a scalar value and the resulting
                    # annotation is a list of values
                    self.elements[key] = [el, value]
        # The set of annotations was modified. Persist them if a store is
        # defined and the persist flag is True.
        if not self.writer is None and persist:
            self.writer.store(self.elements)

    def delete(self, key, value=None, persist=True):
        """Delete the annotations for the given key. If the optional value is
        None all annotated values for the given key are removed. If the value is
        not None only the resulting (key,value) pair will be removed.

        Returns True if at least one mathcing annotation was removed and False
        oterwise.

        Parameters
        ----------
        key: string
            Unique property key
        value: scalar, optional
        persist: bool, optional
            Flag indicating whether the changes are to be persisted immediately

        Returns
        -------
        bool
        """
        if key in self.elements:
            if not value is None:
                el = self.elements[key]
                if isinstance(el, list):
                    # Find the given value in the list of associated values
                    index = -1
                    for i in range(len(el)):
                        if el[i] == value:
                            index = i
                            break
                    if index > -1:
                        # The value was found in the list
                        del el[index]
                    else:
                        # No changes
                        return False
                else:
                    # If the given value equals the annotation value remove the
                    # whole entry from the dictionary
                    if el == value:
                        del self.elements[key]
                    else:
                        # No changes
                        return False
            else:
                del self.elements[key]
            # This part is only reached if the annotation set has changed. If
            # the annotation store is set we persist the changes
            if not self.writer is None and persist:
                self.writer.store(self.elements)
            return True
        return False

    def find_all(self, key, default_value=[]):
        """Get the list of values that are associated with the given key. If no
        value is associated with the key the default value is returned.

        Parameters
        ----------
        key: string
            Unique property key
        default_value: list, optional
            Default value that is returned if the property has not been set

        Returns
        -------
        list
        """
        if key in self.elements:
            el = self.elements[key]
            if isinstance(el, list):
                # Return a copy of the list of values
                return list(el)
            else:
                # The value is assumed to be a scalar value. Return it as a list
                # with a single value.
                return [el]
        else:
            # Unknown key. Return default value
            return default_value

    def find_one(self, key, default_value=None, raise_error_on_multi_value=True):
        """Get a single value that is associated with the given key. If no value
        is associated with the key the default value is returned. If multiple
        values are associated with the given key a ValueError is raised unless
        the raise_error_on_multi_value flag is False. In the latter case one of
        the associated values is returned.

        Parameters
        ----------
        key: string
            Unique property key
        default_value: any, optional
            Default value that is returned if the property has not been set
        raise_error_on_multi_value: bool, optional
            Raises a ValueError if True and the given key is associated with
            multiple values.

        Returns
        -------
        scalar
        """
        if key in self.elements:
            el = self.elements[key]
            if isinstance(el, list):
                # Check if there is more than one value associated with the key.
                # It is assumed that at least one value is in the list.
                if raise_error_on_multi_value and len(el) > 1:
                    raise ValueError('multiple annotation values for \'' + str(key) + '\'')
                else:
                    return el[0]
            else:
                # The value is assumed to be a scalar value
                return el
        else:
            # Unknown key. Return default value
            return default_value

    def update(self, properties):
        """Update the current properties based on the values in the dictionary.

        The (key, value)-pairs in the properties dictionary define the update
        operations. Values are expected to be either None, a scalar value (i.e.,
        int, float, or string) or a list of scalar values. If the value is None
        the corresponding project property is deleted. Otherwise, the
        corresponding property will be replaced by the value or the values in a
        given list of values.
        """
        for key in properties:
            value = properties[key]
            if value is None:
                self.delete(key=key, persist=False)
            elif isinstance(value, list):
                if len(value) > 0:
                    self.add(
                        key=key,
                        value=value[0],
                        replace=True,
                        persist=False
                    )
                    for i in range(1, len(value)):
                        self.add(
                            key=key,
                            value=value[i],
                            replace=False,
                            persist=False
                        )
                else:
                    self.delete(key=key, persist=False)
            else:
                self.add(key=key, value=value, replace=True, persist=False)
        # Persist changes
        if not self.writer is None:
            self.writer.store(self.elements)

    def values(self):
        """Get a dictionary of key,value pairs that contains all annotations.

        Returns
        -------
        dict()
        """
        return self.elements
