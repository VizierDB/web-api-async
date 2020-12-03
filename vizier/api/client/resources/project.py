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

"""Resource object representing a project in a remote vizier instance."""

from vizier.core.timestamp import to_datetime
from datetime import datetime
from typing import Dict, Any, Optional

class ProjectResource(object):
    """A project in a remote vizier instance."""
    def __init__(self, 
            identifier: str, 
            name: str, 
            created_at: datetime, 
            last_modified_at: datetime, 
            default_branch: Optional[str] = None):
        """Initialize the project attributes."""
        self.identifier = identifier
        self.name = name
        self.created_at = created_at
        self.last_modified_at = last_modified_at
        self.default_branch = default_branch

    @staticmethod
    def from_dict(obj: Dict[str, Any]) -> "ProjectResource":
        """Get a project instance from the dictionary representation returned
        by the vizier web service.

        Parameters
        ----------
        obj: dict
            Dictionary serialization of a project handle

        Returns
        -------
        vizier.api.client.resources.project.ProjectResource
        """
        # Get the name from the properties list
        name_properties = [property for property in obj.get('properties', []) if property['key'] == 'name']
        name =  (name_properties[0] if name_properties else {}).get("value", None)
        default_branch = None
        if 'branches' in obj:
            for branch in obj['branches']:
                if branch['isDefault']:
                    default_branch = branch['id']
                    break
        return ProjectResource(
            identifier=obj['id'],
            name=name,
            created_at=to_datetime(obj['createdAt']),
            last_modified_at=to_datetime(obj['lastModifiedAt']),
            default_branch=default_branch
        )
