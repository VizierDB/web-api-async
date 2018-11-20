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

"""Definition and implementation for different object stores that read and
write Json files.
"""

from abc import abstractmethod

import json


class ObjectStore(object):
    """Abstract object store class that defines the interface methods to read
    and write files.
    """

    @abstractmethod
    def read(self, filename):
        """Read Json document from given file.

        Parameters
        ----------
        filename: string
            Absolute file path

        Returns
        -------
        dict or list
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, filename, content):
        """Write content as Json document to given file.

        Parameters
        ----------
        filename: string
            Absolute file path
        content: dict or list
            Json object or array
        """
        raise NotImplementedError


class DefaultObjectStore(ObjectStore):
    """Default implementation of the object store."""

    def read(self, filename):
        """Read Json document from given file.

        Parameters
        ----------
        filename: string
            Absolute file path

        Returns
        -------
        dict or list
        """
        with open(filename, 'r') as f:
            return json.load(f)

    @abstractmethod
    def write(self, filename, content):
        """Write content as Json document to given file.

        Parameters
        ----------
        filename: string
            Absolute file path
        content: dict or list
            Json object or array
        """
        with open(filename, 'w') as f:
            json.dump(content, f)
