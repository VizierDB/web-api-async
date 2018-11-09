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

"""Interface for file store to maintain files that are uploaded via the Web UI
and need to keep as a local copy because they are not accessible via an Url.
"""

from abc import abstractmethod
import gzip

from vizier.core.util import get_unique_identifier
from vizier.core.system import build_info, component_descriptor
from vizier.core.system import VizierSystemComponent


"""File format identifier."""
FORMAT_CSV = 'csv'
FORMAT_CSV_GZIP = 'csv:gzip'
FORMAT_TSV = 'tsv'
FORMAT_TSV_GZIP = 'tsv:gzip'

FILE_FORMATS = [FORMAT_CSV, FORMAT_CSV_GZIP, FORMAT_TSV, FORMAT_TSV_GZIP]


class FileHandle(object):
    """File handle for an uploaded file."""
    def __init__(self, identifier, filepath, file_format=None):
        """Initialize the file identifier, the (full) file path, the file
        format, and the file creation timestamp.

        If the format of the file is not one of the known formats it has to be
        None. Raises ValueError if an invalid file format is specified.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        filepath: string
            Absolute path to file on disk
        file_format: string, optional
            File format identifier or None if unknown
        """
        # Ensure that a valid file format has been given
        if not file_format is None:
            if not file_format in FILE_FORMATS:
                raise ValueError('invalid file format \'' + str(file_format) + '\'')
        # Initialize the class variables
        self.identifier = identifier
        self.filepath = filepath
        self.file_format = file_format

    @property
    def compressed(self):
        """Flag indicating whether the file is in gzip format. This is currently
        determined by the file format value. If the file format is unknown the
        result is False.

        Returns
        -------
        bool
        """
        if not self.file_format is None:
            return self.file_format in [FORMAT_CSV_GZIP, FORMAT_TSV_GZIP]
        return False

    @property
    def delimiter(self):
        """Determine the column delimiter for a CSV/TSV file based on the file
        format information. If the file format is unknown the result is None

        Returns
        -------
        string
        """
        if not self.file_format is None:
            if self.file_format in [FORMAT_CSV, FORMAT_CSV_GZIP]:
                return ','
            else:
                return '\t'
        return None

    def open(self):
        """Get open file object for associated file.

        Returns
        -------
        FileObject
        """
        # The open method depends on the compressed flag
        if self.compressed:
            return gzip.open(self.filepath, 'rb')
        else:
            return open(self.filepath, 'r')


class FileServer(VizierSystemComponent):
    """Abstract API to upload and retrieve files."""
    def __init__(self, build):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        build : dict()
            Build information
        """
        super(FileServer, self).__init__(build)

    def components(self):
        """List containing component descriptor.

        Returns
        -------
        list
        """
        return [component_descriptor('fileserver', self.system_build())]

    @abstractmethod
    def get_file(self, identifier):
        """Get handle for file with given identifier. Returns None if no file
        with given identifier exists.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, filename):
        """Create a new entry from a given local file. Will make a copy of the
        given file.

        Parameters
        ----------
        filename: string
            Path to file on disk

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        raise NotImplementedError
