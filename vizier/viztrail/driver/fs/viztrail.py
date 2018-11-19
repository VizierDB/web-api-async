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

"""Implements the viztrail handle for the file system based repository."""

import shutil

from vizier.viztrail.base.ViztrailHandle


class FSViztrailHandle(ViztrailHandle):
    """
    """
    def __init__(
        self, identifier, exec_env_id, properties, base_dir, branches=None,
        created_at=None
    ):
        """Initialize the viztrail descriptor.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        exec_env_id: string
            Identifier of the execution environment that is used for the
            viztrail
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Handler for user-defined properties
        base_dir : string
            Path to base directory for viztrail resources
        branches: list(vizier.viztrail.branch.BranchHandle)
            List of branches in the viztrail
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        """
        super(FSViztrailHandle, self).__init__(
            identifier=identifier,
            exec_env_id=exec_env_id,
            properties=properties,
            branches=branches,
            created_at=created_at,
            last_modified_at=created_at
        )
        self.base_dir = base_dir

    @staticmethod
    def create(identifier, properties, exec_env_id, base_dir) :
        """
        """
        pass

    def delete(self) :
        """Deletes the directory that contains all resoures that are associated
        with this viztrail.
        """
        shutil.rmtree(self.base_dir)

    @staticmethod
    def load(base_dir):
        """
        """
        pass
