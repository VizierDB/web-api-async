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

"""Resource object representing a project in a remote vizier instance."""

from vizier.core.timestamp import to_datetime


class ProjectResource(object):
    """A project in a remote vizier instance."""
    def __init__(self, identifier, name, created_at, last_modified_at):
        """Initialize the project attributes."""
        self.identifier = identifier
        self.name = name
        self.created_at = created_at
        self.last_modified_at = last_modified_at

    @staticmethod
    def from_dict(obj):
        """Get a project instance from the dictionary representation returned
        by the vizier web service.

        Parameters
        ----------
        obj: dict()
            Dictionary serialization of a project handle

        Returns
        -------
        vizier.client.api.resources.project.ProjectResource
        """
        # Get the name from the properties list
        name = None
        for prop in obj['properties']:
            if prop['key'] == 'name':
                name = prop['value']
                break
        return ProjectResource(
            identifier=obj['id'],
            name=name,
            created_at=to_datetime(obj['createdAt']),
            last_modified_at=to_datetime(obj['lastModifiedAt'])
        )
