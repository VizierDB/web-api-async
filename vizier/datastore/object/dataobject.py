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

"""Dataset maintain annotations for three obj_type of resources: columns, rows, and
cells. Vizier does not reason about annotations at this point and therefore
there is only limited functionality provided to query annotations. The dataset
metadata object is barely a wrapper around three lists of resource annotations.
"""

import json
import os

from vizier.datastore.object.base import DataObject


class DataObjectMetadata(object):
    """Collection of annotations for a dataset object. For each of the three
    resource types a list of annotations is maintained.
    """
    def __init__(self, objects=None):
        """Initialize the metadata lists for the three different types of
        dataset resources that can be annotated.

        Parameters
        ----------
        objects: list(vizier.datastpre.object.base.DataObject), optional
            objects
        """
        self.objects = objects if not objects is None else list()
        
    def add(self, key, value, identifier, obj_type):
        """Add a new annotation for a dataset resource. The resource obj_type is
        determined based on the column and row identifier values. At least one
        of them has to be not None. Otherwise, a ValueError is raised.

        Parameters
        ----------
        key: string
            DataObject key
        value: scalar
            DataObject value
        identifier: string
            Unique identifier
        """
        # Create the annotation object. This will raise an exception if the
        # resource identifier is invalid.
        obj = DataObject(
            key=key,
            value=value,
            identifier=identifier,
            obj_type=obj_type
        )
        self.objects.append(obj)


    def find_all(self, values, key):
        """Get the list of annotations that are associated with the given key.
        If no annotation is associated with the key an empty list is returned.

        Parameters
        ----------
        values: list(vizier.datastore.object.base.DataObject)
            List of objects
        key: string
            Unique property key

        Returns
        -------
        list(vizier.datastore.annotation.base.DatasetAnnotation)
        """
        result = list()
        for obj in values:
            if obj.key == key:
                result.append(obj)
        return result

    def filter(self, obj_types):
        """Filter objects to keep only those that reference existing
        resources. Returns a new data object metadata object.

        Parameters
        ----------
        columns: list(string)
            List of object obj_types
     
        Returns
        -------
        vizier.datastore.object.dataset.DataObjectMetadata
        """
        result = DataObjectMetadata(
            objects=list(),
        )
        
        for obj in self.objects:
            if obj.obj_type in obj_types:
                result.objects.append(obj)
        return result

    def find_one(self, values, key, raise_error_on_multi_value=True):
        """Get a single annotation that is associated with the given key. If no
        annotation is associated with the keyNone is returned. If multiple
        annotations are associated with the given key a ValueError is raised
        unless the raise_error_on_multi_value flag is False. In the latter case
        one of the found annotations is returned.

        Parameters
        ----------
        values: list(vizier.datastore.annotation.base.DatasetAnnotation)
            List of annotations
        key: string
            Unique property key
        raise_error_on_multi_value: bool, optional
            Raises a ValueError if True and the given key is associated with
            multiple values.

        Returns
        -------
        vizier.datastore.annotation.base.DatasetAnnotation
        """
        result = self.find_all(values=values, key=key)
        if len(result) == 0:
            return None
        elif len(result) == 1 or not raise_error_on_multi_value:
            return result[0]
        else:
            raise ValueError('multiple annotation values for \'' + str(key) + '\'')

    def for_id(self, identifier):
        """Get list of annotations for the specified cell

        Parameters
        ----------
        column_id: int
            Unique column identifier
        row_id: int
            Unique row identifier

        Returns
        -------
        list(vizier.datastpre.annotation.base.DatasetAnnotation)
        """
        result = list()
        for obj in self.objects:
            if not obj is None and obj.identifier == identifier:
                result.append(obj)
        return result

    def for_key(self, key):
        """Get object metadata set for a key.

        Parameters
        ----------
        column_id: int
            Unique column identifier

        Returns
        -------
        list(vizier.datastpre.object.base.DataObject)
        """
        result = list()
        for obj in self.objects:
            if not obj is None and obj.key == key:
                result.append(obj)
        return result

    def for_type(self, obj_type):
        """Get object metadata set for a dataset row.

        Parameters
        ----------
        obj_type: string
            Unique obj_type

        Returns
        -------
        list(vizier.datastpre.object.base.DataObject)
        """
        result = list()
        for obj in self.objects:
            if not obj is None and obj.obj_type == obj_type:
                result.append(obj)
        return result

    @staticmethod
    def from_file(filename):
        """Read dataset annotations from file. Assumes that the file has been
        created using the default serialization (to_file), i.e., is in Json
        format.

        Parameters
        ----------
        filename: string
            Name of the file to read from

        Returns
        -------
        vizier.database.object.dataset.DataObjectMetadata
        """
        # Return an empty annotation set if the file does not exist
        if not os.path.isfile(filename):
            return DataObjectMetadata()
        with open(filename, 'r') as f:
            doc = json.loads(f.read())
        objects = None
        if 'objects' in doc:
            objects = [DataObject.from_dict(a) for a in doc['objects']]
        
        return DataObjectMetadata(
            objects=objects
        )

    @staticmethod
    def from_list(values):
        """Create dataset metadata instance from list of dataset annotations

        Parameters
        ----------
        values: list(vizier.datastore.object.base.DataObject)
            List containing all annotations for a dataset

        Returns
        -------
        vizier.database.object.dataset.DataObjectMetadata
        """
        objects = list()
        for obj in values:
            if obj.identifier is None or obj.obj_type is None or obj.key is None:
                raise ValueError('invalid data object')
            objects.append(obj)
            
        return DataObjectMetadata(
            objects=objects 
        )

    def remove(self, key=None, value=None, identifier=None, obj_type=None):
        """Remove annotations for a dataset resource. The resource obj_type is
        determined based on the column and row identifier values. At least one
        of them has to be not None. Otherwise, a ValueError is raised.

        If the key and/or value are given they are used as additional filters.
        Otherwise, all annotations for the resource are removed.

        Parameters
        ----------
        key: string, optional
            object key
        value: scalar, optional
            object value
        identifier: string, optional
            Unique identifier
        obj_type: string, optional
            obj_type
        """
            
        nunz = 0
        if key is None:
            nunz = nunz + 1
        if value is None:
            nunz = nunz + 1  
        if identifier is None:
            nunz = nunz + 1 
        if obj_type is None:
            nunz = nunz + 1
        
        if nunz is not 1:
            raise ValueError('only one param must not be none')
        
        objects = list()
        for obj in self.objects:
            if key is not None and obj.key != key:
                objects.append(obj)
            if value is not None and obj.value != value:
                objects.append(obj)
            if identifier is not None and obj.identifier != identifier:
                objects.append(obj)
            if obj_type is not None and obj.obj_type != obj_type:
                objects.append(obj)
        self.objects = objects
        

    def to_file(self, filename):
        """Write current annotations to file in default file format. The default
        serializartion format is Json.

        Parameters
        ----------
        filename: string
            Name of the file to write
        """
        doc = dict()
        if len(self.objects) > 0:
            doc['objects'] = [a.to_dict() for a in deduplicate(self.objects)]
        with open(filename, 'w') as f:
            json.dump(doc, f)

    @property
    def values(self):
        """List all annotations in the metadata set.

        Returns
        -------
        list(vizier.datastore.annotation.base.DatasetAnnotation)
        """
        return self.columns + self.rows + self.cells


#  -----------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def deduplicate(elements):
    """Remove duplicate entries in a list of dataset annotations.

    Parameters
    ----------
    elements: list(vizier.datastore.annotation.base.DatasetAnnotation)
        List of dataset annotations

    Returns
    -------
    list(vizier.datastore.annotation.base.DatasetAnnotation)
    """
    if len(elements) < 2:
        return elements
    s = sorted(elements, key=lambda a: (a.identifier, a.obj_type, a.key, a.value))
    result = s[:1]
    for a in s[1:]:
        l = result[-1]
        if a.identifier != l.identifier or a.obj_type != l.obj_type or a.key != l.key or a.value != l.value:
            result.append(a)
    return result
