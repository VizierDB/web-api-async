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

"""A vizier data curation project is a wrapper for the workflows, datastore,
and filestore that are associated with a project.
"""


class ProjectHandle(object):
    """The project handle is a wrapper around the main components of a vizier
    data curation project.
    """
    def __init__(self, viztrail, datastore, filestore):
        """Initialize the project components.

        Parameters
        ----------
        viztrail: vizier.viztrail.base.ViztrailHandle
            The viztrail repository manager for the Vizier instance
        datastore: vizier.datastore.base.Datastore
            Associated datastore
        filestore: vizier.filestore.base.Filestore
            Associated filestore
        """
        self.viztrail = viztrail
        self.datastore = datastore
        self.filestore = filestore
