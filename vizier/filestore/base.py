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


"""File format identifier."""
FORMAT_CSV = 'csv'
FORMAT_CSV_GZIP = 'csv:gzip'
FORMAT_TSV = 'tsv'
FORMAT_TSV_GZIP = 'tsv:gzip'

FILE_FORMATS = [FORMAT_CSV, FORMAT_CSV_GZIP, FORMAT_TSV, FORMAT_TSV_GZIP]


class FileHandle(object):
    """File handle for an uploaded file."""
    def __init__(self, identifier, filepath, file_name, file_format=None):
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
        file_name: string
            Base name of the original file
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
        self.file_name = file_name
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

    @property
    def is_tabular(self):
        """Returns True if the file suffix indicates that the associated file is
        in one of the supported tabular data formats (i.e., CSV or TSV).

        Returns
        -------
        bool
        """
        return not self.file_format is None

    @property
    def name(self):
        """Alternative to access the file name.

        Returns
        -------
        string
        """
        return self.file_name

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


class Filestore(object):
    """Abstract API to upload and retrieve files."""

    @abstractmethod
    def delete_file(self, identifier):
        """Delete file with given identifier. Returns True if file was deleted
        or False if no such file existed.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def download_file(self, uri, username=None, password=None):
        """Create a local copy of the identified web resource.

        Parameters
        ----------
        uri : string
            Unique resource identifier for external resource that is accessed
        username: string, optional
            Optional user name for authentication
        password: string, optional
            Optional password for authentication

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        raise NotImplementedError

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
    def list_files(self):
        """Get list of file handles for all uploaded files.

        Returns
        -------
        list(vizier.filestore.base.FileHandle)
        """
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, filename):
        """Create a new entry from a given local file. Will make a copy of the
        given file.

        Raises ValueError if the given file does not exist.

        Parameters
        ----------
        filename: string
            Path to file on disk

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        raise NotImplementedError

    @abstractmethod
    def upload_stream(self, file, file_name):
        """Create a new entry from a given file stream.

        Parameters
        ----------
        file: werkzeug.datastructures.FileStorage
            File object (e.g., uploaded via HTTP request)
        file_name: string
            Name of the file

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        raise NotImplementedError


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_download_filename(url, info):
    """Extract a file name from a given Url or request info header.

    Parameters
    ----------
    url: string
        Url that was opened using urllib2.urlopen
    info: dict
        Header information returned by urllib2.urlopen

    Returns
    -------
    string
    """
    # Try to extract the filename from the Url first
    filename = url[url.rfind('/') + 1:]
    if '.' in filename:
        return filename
    else:
        if 'Content-Disposition' in info:
            content = info['Content-Disposition']
            if 'filename="' in content:
                filename = content[content.rfind('filename="') + 11:]
                return filename[:filename.find('"')]
    return 'download'


def get_fileformat(filename):
    """Determine the file type based on the file name suffix. If the suffix
    is unknown the result is None.

    Parameters
    ----------
    filename: string
        File name

    Returns
    -------
    string
    """
    if filename.endswith('.csv'):
        return FORMAT_CSV
    elif filename.endswith('.csv.gz'):
        return FORMAT_CSV_GZIP
    elif filename.endswith('.tsv'):
        return FORMAT_TSV
    elif filename.endswith('.tsv.gz'):
        return FORMAT_TSV_GZIP
    return None
