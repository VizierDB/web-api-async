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

"""Interface for file store to maintain files that are uploaded via the Web UI
and need to keep as a local copy because they are not accessible via an Url.
"""
from typing import cast, Optional, IO, Dict, Any, List

import gzip
import mimetypes
import os.path

from abc import abstractmethod

import vizier.core.util as util


"""File format identifier."""
FORMAT_CSV = 'text/csv'
FORMAT_TSV = 'text/tab-separated-values'

ENCODING_GZIP = 'gzip'


class FileHandle(object):
    """File handle for an uploaded file."""
    def __init__(self, 
            identifier: str, 
            filepath: str, 
            file_name: str, 
            mimetype: Optional[str] = None, 
            encoding: Optional[str] = None):
        # quotechar='"', quoting=csv.QUOTE_MINIMAL
        """Initialize the file identifier, the (full) file path, the file
        format, and the file encoding (for compressed files).

        The file encoding may be None. If the mimetype is None an attempt is
        made to guess the mime type.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        filepath: string
            Absolute path to file on disk
        file_name: string
            Base name of the original file
        mimetype: string
            The (assumed) Mime type of the file
        encoding: string
            Name of the program used to encode the file (if compressed)> May be
            None
        """
        self.identifier = identifier
        self.filepath = filepath
        self.file_name = file_name
        if mimetype is None:
            type, encoding = mimetypes.guess_type(url=file_name)
            self.mimetype = type
            self.encoding = encoding
        else:
            self.mimetype = mimetype
            self.encoding = encoding
        # self.quotechar = quotechar
        # self.quoting = quoting

    @property
    def compressed(self):
        """Flag indicating whether the file is in gzip format. This is currently
        determined by the value of the file encoding.

        Returns
        -------
        bool
        """
        return self.encoding == ENCODING_GZIP

    @property
    def delimiter(self):
        """Determine the column delimiter for a CSV/TSV file based on the file
        format information. If the file format is unknown the result is None

        Returns
        -------
        string
        """
        if self.mimetype == FORMAT_CSV:
            return ','
        elif self.mimetype == FORMAT_TSV:
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
        return self.mimetype in [FORMAT_CSV, FORMAT_TSV]

    @property
    def name(self):
        """Alternative to access the file name.

        Returns
        -------
        string
        """
        return self.file_name

    def open(self, raw: bool = False) -> IO:
        """Get open file object for associated file.

        Returns
        -------
        FileObject
        """
        # The open method depends on the compressed flag
        if self.compressed and not raw:
            return cast(IO, gzip.open(self.filepath, 'rb'))
        else:
            return open(self.filepath, 'r'+('b' if raw else ''))

    def size(self, raw: bool = False) -> int:
        if self.compressed and not raw:
            size = 0
            with self.open() as f:
                while True:
                    c = f.read(1)
                    if c:
                        size += 1
                    else:
                        break
            return size
        else:
            return os.path.getsize(self.filepath)



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
        raise NotImplementedError()

    @abstractmethod
    def download_file(self, 
            url: str, 
            username: str = None, 
            password: str = None
        ) -> FileHandle:
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
        raise NotImplementedError()

    @abstractmethod
    def get_file(self, identifier: str) -> Optional[FileHandle]:
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
        raise NotImplementedError()

    @abstractmethod
    def replace_file(self, 
            identifier: str, 
            file_name: Optional[str] = None, 
            mimetype: Optional[str] = None,
            encoding: Optional[str] = None
        ) -> IO[bytes]:
        """Open an IO pipe to replace the file with the given 
           identifier"""
        raise NotImplementedError

    @abstractmethod
    def list_files(self) -> List[FileHandle]:
        """Get list of file handles for all uploaded files.

        Returns
        -------
        list(vizier.filestore.base.FileHandle)
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_download_filename(url: str, info: Dict[str,Any]) -> str:
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


def CSV(filename):
    """Return a file handle object for a CSV file on disk.

    Parameters
    ----------
    filename: string
        Path to file on the local file system.

    Returns
    -------
    vizier.filestore.base.Filehandle
    """
    return FileHandle(
        identifier=util.get_unique_identifier(),
        filepath=filename,
        file_name=os.path.basename(filename)
    )
