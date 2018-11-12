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

from vizier.core.annotation.base import AnnotationStore, DefaultAnnotationSet


class PersistentAnnotationStore(AnnotationStore):
    """Persistent store for object annotations. Materializes object annotations
    in a file on disk in Json format.
    """
    def __init__(self, filename):
        """initialize the path to the storage file on disk.

        Parameters
        ----------
        filename: string
            Path to file on disk
        """
        self.filename = filename

    def store(self, annotations):
        """Persist the given dictionary of object annotations. Each key in the
        dictionary is expected to be associated with either a scalar value or
        a list of scalar values. Stores annotations as an array of key value
        pairs.

        Parameters
        ----------
        annotations: dict, optional
            Dictionary of object annotations
        """
        with open(self.filename, 'w') as f:
            json.dump([{
                'key': key, 'value': annotations[key]
                } for key in annotations], f)


class PersistentAnnotationSet(DefaultAnnotationSet):
    """Object annotation set that is associated with a persistent annotation
    store. This class is a shortcut to instantiate an object annotation store
    with a persistent annotation store defined in this module.
    """
    def __init__(self, filename):
        """Initialize the file that maintains the annotations. Annotations are
        read from file (if it exists).

        Parameters
        ----------
        filename: string
            Path to annotation file
        """
        annotations = dict()
        # Read annotations from disk if the annotation file exists
        abs_path = os.path.abspath(filename)
        if os.path.isfile(abs_path):
            with open(abs_path, 'r') as f:
                obj = json.load(f)
                for anno in obj:
                    annotations[anno['key']] = anno['value']
        # Initialize the default object annotation set
        super(PersistentAnnotationSet, self).__init__(
            elements=annotations,
            writer=PersistentAnnotationStore(filename=abs_path)
        )
