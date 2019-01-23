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

"""Factory object for filestore instances. Each project is associated with its
own filestore. These filestore instances are created by a filestore factory when
the project is instantiated.
"""

from abc import abstractmethod


class FilestoreFactory(object):
    """Create and delete filestore instances that are associated with vizier
    projects. Each filestore is associated with a project that is identified
    by a unique identifier.
    """
    @abstractmethod
    def delete_filestore(self, identifier):
        """Delete a filestore. This method is normally called when the project
        with which the filestore is associated is deleted.

        Parameters
        ----------
        identifier: string
            Unique identifier for filestore
        """
        raise NotImplementedError

    @abstractmethod
    def get_filestore(self, identifier):
        """Get the filestore instance for the project with the given identifier.

        Paramaters
        ----------
        identifier: string
            Unique identifier for filestore

        Returns
        -------
        vizier.filestore.base.Filestore
        """
        raise NotImplementedError


class DevNullFilestoreFactory(FilestoreFactory):
    """This dummy filestore factory is used in environments that requires a
    filestore factory but that never access the returned filestore object. A
    typical example is a remote worker that executes Python cells in a vizier
    workflow.
    """
    def delete_filestore(self, identifier):
        """Delete a filestore. This method is normally called when the project
        with which the filestore is associated is deleted.

        Parameters
        ----------
        identifier: string
            Unique identifier for filestore
        """
        return None

    def get_filestore(self, identifier):
        """Get the filestore instance for the project with the given identifier.
        The dummy implementation returns None in any case. It is assumed that
        the calling process will never try to access the returned object anyway.

        Paramaters
        ----------
        identifier: string
            Unique identifier for filestore

        Returns
        -------
        vizier.filestore.base.Filestore
        """
        return None
