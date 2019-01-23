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

"""Default implementation of the file store. This implementation keeps all files
and their metadata on disk.
"""

import gzip
import json
import os
import shutil
import tempfile
import urllib2

from vizier.core.util import get_unique_identifier
from vizier.filestore.base import Filestore, FileHandle
from vizier.filestore.base import get_download_filename


"""File store constants."""
# Default suffix for all local files
DATA_FILENAME = 'file.dat'
# Name of the metadata file
METADATA_FILENAME = 'properties.json'


"""Configuration parameter."""
PARA_DIRECTORY = 'directory'


class FileSystemFilestore(Filestore):
    """Default file server implementation. Keeps all files subfolders of a
    given base directory. Each subfolder contains the uploaded file (named
    file.dat for convenience), and the metadata file properties.json.

    The metadata object is a dictionary with three elements: originalFilename
    and mimeType, and encoding.
    """
    def __init__(self, base_path):
        """Initialize the base directory that is used for file storage. The
        directory will be created if they do not exist.

        Parameters
        ---------
        base_path : string
            Path to the base directory.
        """
        # Create the base directory if it does not exist
        if not os.path.isdir(base_path):
            os.makedirs(base_path)
        self.base_path = base_path

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
        file_dir =self.get_file_dir(identifier)
        if os.path.isdir(file_dir):
            shutil.rmtree(file_dir, ignore_errors=True)
            return True
        return False

    def download_file(self, url, username=None, password=None):
        """Create a local copy of the identified web resource.

        Parameters
        ----------
        url : string
            Unique resource identifier for external resource that is accessed
        username: string, optional
            Optional user name for authentication
        password: string, optional
            Optional password for authentication

        Returns
        -------
        vizier.filestore.base.FileHandle
        """
        # Get unique identifier and output file
        identifier = get_unique_identifier()
        file_dir = self.get_file_dir(identifier, create=True)
        output_file = os.path.join(file_dir, DATA_FILENAME)
        # Write web resource to output file.
        response = urllib2.urlopen(url)
        filename = get_download_filename(url, response.info())
        mode = 'w'
        if filename.endswith('.gz'):
            mode += 'b'
        with open(output_file, mode) as f:
            f.write(response.read())
        # Add file to file index
        f_handle = FileHandle(
            identifier,
            filepath=output_file,
            file_name=filename
        )
        # Write metadata file
        write_metadata_file(file_dir, f_handle)
        return f_handle

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
        file_dir = self.get_file_dir(identifier)
        if os.path.isdir(file_dir):
            file_name, mimetype, encoding = read_metadata_file(file_dir)
            return FileHandle(
                identifier,
                filepath=os.path.join(file_dir, DATA_FILENAME),
                file_name=file_name,
                mimetype=mimetype,
                encoding=encoding
            )
        return None

    def get_file_dir(self, identifier, create=False):
        """Get path to the directory for the file with the given identifier. If
        the directory does not exist it will be created if the create flag is
        True.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        create: bool, optional
            File directory will be created if it does not exist and the flag
            is True

        Returns
        -------
        string
        """
        file_dir = os.path.join(os.path.abspath(self.base_path), identifier)
        if create and not os.path.isdir(file_dir):
            os.makedirs(file_dir)
        return file_dir

    def list_files(self):
        """Get list of file handles for all uploaded files.

        Returns
        -------
        list(vizier.filestore.base.FileHandle)
        """
        result = list()
        for f_name in os.listdir(self.base_path):
            dir_name = os.path.join(self.base_path, f_name)
            if os.path.isdir(dir_name):
                file_name, mimetype, encoding = read_metadata_file(dir_name)
                f_handle = FileHandle(
                    f_name,
                    filepath=os.path.join(dir_name, DATA_FILENAME),
                    file_name=file_name,
                    mimetype=mimetype,
                    encoding=encoding
                )
                result.append(f_handle)
        return result

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
        # Ensure that the given file exists
        if not os.path.isfile(filename):
            raise ValueError('invalid file path \'' + str(filename) + '\'')
        name = name = os.path.basename(filename)
        # Create a new unique identifier for the file.
        identifier = get_unique_identifier()
        file_dir = self.get_file_dir(identifier, create=True)
        output_file = os.path.join(file_dir, DATA_FILENAME)
        # Copy the uploaded file
        shutil.copyfile(filename, output_file)
        # Add file to file index
        f_handle = FileHandle(
            identifier,
            filepath=output_file,
            file_name=name
        )
        # Write metadata file
        write_metadata_file(file_dir, f_handle)
        return f_handle

    def upload_stream(self, file, file_name):
        """Create a new entry from a given file stream. Will copy the given
        file to a file in the base directory.

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
        # Create a new unique identifier for the file.
        identifier = get_unique_identifier()
        file_dir = self.get_file_dir(identifier, create=True)
        output_file = os.path.join(file_dir, DATA_FILENAME)
        # Save the file object to the new file path
        file.save(output_file)
        f_handle = FileHandle(
            identifier,
            filepath=output_file,
            file_name=file_name
        )
        # Write metadata file
        write_metadata_file(file_dir, f_handle)
        return f_handle


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def read_metadata_file(file_dir):
    """Read metadata information for the specified file. Returns the original
    file name, the mime type and encoding (the last two values may be None).

    Parameters
    ----------
    file_dir: string
        Base directory for the file

    Returns
    -------
    (string, string, string)
    """
    metadata_file = os.path.join(file_dir, METADATA_FILENAME)
    with open(metadata_file, 'r') as f:
        obj = json.load(f)
    return obj['originalFilename'], obj['mimeType'], obj['encoding']


def write_metadata_file(file_dir, f_handle):
    """Write the metadata file for the given file handle.

    Parameters
    ----------
    file_dir: string
        Base directory for the file
    f_handle: vizier.filestore.base.FileHandle
        File handle
    """
    metadata_file = os.path.join(file_dir, METADATA_FILENAME)
    with open(metadata_file, 'w') as f:
        json.dump({
            'originalFilename': f_handle.file_name,
            'mimeType': f_handle.mimetype,
            'encoding': f_handle.encoding
        }, f)
