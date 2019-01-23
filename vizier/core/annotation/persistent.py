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

"""Implementation for an object properties set that is maintained using an
object store.
"""

from vizier.core.annotation.base import AnnotationStore, DefaultAnnotationSet
from vizier.core.io.base import DefaultObjectStore
from vizier.core.util import init_value


class PersistentAnnotationStore(AnnotationStore):
    """Persistent store for object annotations. Materializes object annotations
    using a given object store.
    """
    def __init__(self, object_path, object_store=None):
        """Initialize the path to the resource in the object store. By default
        annotation stes are persisted as files on the locak file system.

        Parameters
        ----------
        object_path: string
            Path to the resource
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store to materialize annotations
        """
        self.object_path = object_path
        self.object_store = init_value(object_store, DefaultObjectStore())

    def store(self, annotations):
        """Persist the given dictionary of object annotations. Each key in the
        dictionary is expected to be associated with either a scalar value or
        a list of scalar values. Stores annotations as an array of key value
        pairs.

        Parameters
        ----------
        annotations: dict
            Dictionary of object annotations
        """
        self.object_store.write_object(
            object_path=self.object_path,
            content=[
                {
                    'key': key, 'value': annotations[key]
                } for key in annotations
            ]
        )


class PersistentAnnotationSet(DefaultAnnotationSet):
    """Object annotation set that is associated with a persistent annotation
    store. This class is a shortcut to instantiate an object annotation store
    with a persistent annotation store defined in this module.
    """
    def __init__(self, object_path, object_store=None, annotations=None):
        """Initialize the file that maintains the annotations. Annotations are
        read from file (if it exists).

        Provides the option to load an initial set of annotations from a given
        dictionary. If the file exists and the annotations dictionary is not
        None an exception is thrown.

        Parameters
        ----------
        object_path: string
            Path to resource
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store to materialize annotations
        annotations: dict, optional
            Dictionary with initial set of annotations
        """
        # Ensure that the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        if not annotations is None:
            # Initialize annotations from the given dictionary. The persistent
            # set can only be initialized once.
            if object_store.exists(object_path):
                raise ValueError('cannot initialize existing annotation set')
            # Initialize the default object annotation set
            super(PersistentAnnotationSet, self).__init__(
                writer=PersistentAnnotationStore(
                    object_path=object_path,
                    object_store=object_store
                )
            )
            for key in annotations:
                value = annotations[key]
                if isinstance(value, list):
                    for val in value:
                        self.add(key, val, persist=False)
                else:
                    self.add(key, value, persist=False)
            self.writer.store(self.elements)
        else:
            # Read annotations from disk if the annotation file exists
            elements = dict()
            if object_store.exists(object_path):
                obj = object_store.read_object(object_path)
                for anno in obj:
                    elements[anno['key']] = anno['value']
            # Initialize the default object annotation set
            super(PersistentAnnotationSet, self).__init__(
                elements=elements,
                writer=PersistentAnnotationStore(
                    object_path=object_path,
                    object_store=object_store
                )
            )
