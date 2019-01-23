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

"""A vizier data curation project is a wrapper for the workflows in a viztrail.
The project handle may also contain references to the datastore and filestore
that are associated with a project.
"""


class ProjectHandle(object):
    """The project handle is a wrapper around the main components of a vizier
    data curation project. The datastore and filestore are optional to allow
    for projects that maintain their datastores and filestores in separate
    containers or virtual machines.
    """
    def __init__(self, viztrail, datastore=None, filestore=None):
        """Initialize the project components.

        Parameters
        ----------
        viztrail: vizier.viztrail.base.ViztrailHandle
            The viztrail handle for the project
        datastore: vizier.datastore.base.Datastore, optional
            Associated datastore
        filestore: vizier.filestore.base.Filestore, optional
            Associated filestore
        """
        self.viztrail = viztrail
        self.datastore = datastore
        self.filestore = filestore

    @property
    def created_at(self):
        """Shortcut to get the timestamp of creation from the associated
        viztrail.

        Returns
        -------
        datetime.datetime
        """
        return self.viztrail.created_at

    def get_default_branch(self):
        """Shortcut to access the handle for the default branch of the viztrail.

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        return self.viztrail.get_default_branch()

    @property
    def identifier(self):
        """Unique project identifier. The project identifier is the same
        identifier as the unique identifier for the associated viztrail.

        Returns
        -------
        string
        """
        return self.viztrail.identifier

    @property
    def last_modified_at(self):
        """Shortcut to get the last modified timestamp from the associated
        viztrail.

        Returns
        -------
        datetime.datetime
        """
        return self.viztrail.last_modified_at

    @property
    def name(self):
        """Shortcut to get the name of the associated viztrail.

        Returns
        -------
        string
        """
        return self.viztrail.name
