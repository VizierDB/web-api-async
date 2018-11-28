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

"""Vizier API - File Store - Implements the methods of the API that directly
interact with the file store component of the Vizier instance.
"""

class FileStoreApi(object):
    """API wrapper around the file store object that manages the file store of
    the vizier instance.

    At this point the only file store method that is visible to the outside is
    the file upload. All other file store methods are for internal use only and
    therefore not exposed via the API.
    """
    def __init__(self, filestore):
        """Initialize the object that manages the file store of the Vizier
        instance.

        Parameters
        ----------
        filestore: vizier.filestore.base.FileStore
            Backend store for uploaded files
        """
        self.filestore = filestore

    def upload_file(self, filename):
        """Upload a given file to the file store. Does not expect a file of a
        specific type. The base name of the given file will be the name of the
        resulting local file handle. When accessing uploaded files in the VizUAL
        load command the file type is determined by the file suffix.

        Parameters
        ----------
        filename : string
            Path to file on local disk

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        return self.filestore.upload_file(filename)
