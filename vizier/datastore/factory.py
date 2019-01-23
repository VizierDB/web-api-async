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

"""Factory object for datastore instances. Each project is associated with its
own datastore. These datastore instances are created by a datastore factory when
the project is instantiated.
"""

from abc import abstractmethod


class DatastoreFactory(object):
    """Create and delete datastore instances that are associated with vizier
    projects. Each datastore is associated with a project that is identified
    by a unique identifier.
    """
    @abstractmethod
    def delete_datastore(self, identifier):
        """Delete a datastore. This method is normally called when the project
        with which the datastore is associated is deleted.

        Parameters
        ----------
        identifier: string
            Unique identifier for datastore
        """
        raise NotImplementedError

    @abstractmethod
    def get_datastore(self, identifier):
        """Get the datastore instance for the project with the given identifier.

        Paramaters
        ----------
        identifier: string
            Unique identifier for datastore

        Returns
        -------
        vizier.datastore.base.Datastore
        """
        raise NotImplementedError
