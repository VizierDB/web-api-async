# Copyright (C) 2019 New York University,
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

"""Objects representing descriptors and handles for datasets."""

import vizier.api.serialize.deserialize as deserialize


class DatasetDescriptor(object):
    """A dataset descriptor is simply an identifier and a dictionary of HATEOAS
    references.
    """
    def __init__(self, identifier, links):
        self.identifier = identifier
        self.links = links

    @staticmethod
    def from_dict(obj):
        """Create the descriptor from a dictionary serialization.

        Parameters
        ----------
        obj: dict
            Dictionary serialization for dataset descriptor as returned by the
            server.

        Returns
        -------
        vizier.api.client.resources.dataset.DatasetDescriptor
        """
        return DatasetDescriptor(
            identifier=obj['id'],
            links=deserialize.HATEOAS(links=obj['links'])
        )
