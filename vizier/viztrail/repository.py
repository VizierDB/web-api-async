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

"""Abstract class for viztrail repositories. The repository maintains a set of
viztrails. It provides methods to create, delete, and access viztrails.
"""

from abc import abstractmethod


class ViztrailRepository(object):
    """Repository for viztrails. This is an abstract class that defines all
    necessary methods to maintain and manipulate viztrails.
    """
    @abstractmethod
    def create_viztrail(self, properties=None):
        """Create a new viztrail. The initial set of properties is an optional
        dictionary of (key,value)-pairs where all values are expected to either
        be scalar values or a list of scalar values.

        Parameters
        ----------
        properties: dict, optional
            Set of properties for the new viztrail

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def delete_viztrail(self, viztrail_id):
        """Delete the viztrail with given identifier. The result is True if a
        viztrail with the given identifier existed, False otherwise.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def get_viztrail(self, viztrail_id):
        """Retrieve the viztrail with the given identifier. The result is None
        if no viztrail with given identifier exists.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        vizier.viztrail.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def list_viztrails(self):
        """List handles for all viztrails in the repository.

        Returns
        -------
        list(vizier.viztrail.base.ViztrailHandle)
        """
        raise NotImplementedError
