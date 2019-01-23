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

"""Project cache for container servers that maintain a single project only. All
methods except get projects remain unimplemented.
"""

from vizier.engine.project.cache.base import ProjectCache


class SingleProjectCache(ProjectCache):
    """The container project cache maintains a single project. The class is used
    by the container server API. All methods except get project remain
    unimplemented.
    """
    def __init__(self, project):
        """Initialize the project handle.

        Parameters
        ----------
        project: vizier.engine.project.base.ProjectHandle
            Handle for the container project
        """
        self.project = project

    def get_project(self, project_id):
        """Get the handle for project. Raises RuntimeError if the project
        identifier does not match the identifier of the container project.

        Returns
        -------
        vizier.engine.project.base.ProjectHandle
        """
        if project_id != self.project.identifier:
            raise RuntimeError('invalid project identifier \'' + str(project_id) + '\'')
        return self.project
