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

"""Default implementation of the file store. This implementation keeps all files
and their metadata on disk.
"""

import gzip
import os
import shutil
import tempfile
import urllib2

from vizier.core.util import get_unique_identifier
from vizier.filestore.base import Filestore, FileHandle
from vizier.filestore.base import get_download_filename, get_fileformat


"""File store constants."""
# Default suffix for all local files
FILENAME_SUFFIX = '.dat'
# Name of the metadata file
METADATA_FILE_NAME = 'index.tsv'


"""Configuration parameter."""
PARA_DIRECTORY = 'directory'


class DefaultFilestore(Filestore):
    """Default file server implementation. Keeps all files in a given base
    directory on disk. Files are named by their unique identifier (with suffix
    .dat for convenience). File metadata is kept in a tab-delimited file
    index.tsv in the same folder.

    The index file has three columns: the file identifier, the original file
    name and an optional column with the file format. For files with unknown
    format the third column is omitted.
    """
    def __init__(self, base_path):
        """Initialize the base directory that is used for file storage. The base
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
        # Initialize the index file and read the content
        self.index_file = os.path.join(self.base_path, METADATA_FILE_NAME)

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
        files = self.list_files()
        for i in range(len(files)):
            fh = files[i]
            if fh.identifier == identifier:
                os.remove(fh.filepath)
                del files[i]
                write_index_file(self.index_file, files)
                return True
        return False

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
        # Get unique identifier and output file
        identifier = get_unique_identifier()
        output_file = get_filepath(self.base_path, identifier)
        # Write web resource to output file.
        response = urllib2.urlopen(uri)
        filename = get_download_filename(uri, response.info())
        mode = 'w'
        if filename.endswith('.gz'):
            mode += 'b'
        with open(output_file, mode) as f:
            f.write(response.read())
        # Add file to file index
        f_handle = FileHandle(
            identifier,
            filepath=output_file,
            file_name=filename,
            file_format=get_fileformat(filename)
        )
        write_index_file(self.index_file, self.list_files() + [f_handle])
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
        for fh in self.list_files():
            if fh.identifier == identifier:
                return fh
        return None

    @staticmethod
    def init(properties):
        """Create an instance of the viztrails repositorfile store from a given
        dictionary of configuration arguments. At this point only the base
        directory is expected as to be present in the properties dictionary.

        Parameter
        ---------
        properties: dict()
            Dictionary of configuration arguments

        Returns
        -------
        vizier.filestore.fs.base.DefaultFilestore
        """
        # Raise an exception if the pase directory argument is not given
        if not PARA_DIRECTORY in properties:
            raise ValueError('missing value for argument \'' + PARA_DIRECTORY + '\'')
        return DefaultFilestore(base_path=properties[PARA_DIRECTORY])

    def list_files(self):
        """Get list of file handles for all uploaded files.

        Returns
        -------
        list(vizier.filestore.base.FileHandle)
        """
        files = dict()
        if os.path.isfile(self.index_file):
            with open(self.index_file, 'r') as f:
                for line in f:
                    tokens = line.strip().split('\t')
                    file_id = tokens[0]
                    filepath = get_filepath(self.base_path, file_id)
                    file_name = tokens[1]
                    file_format = None
                    if len(tokens) == 3:
                        file_format = tokens[2]
                    files[file_id] = FileHandle(
                        file_id,
                        filepath=filepath,
                        file_name=file_name,
                        file_format=file_format
                    )
        return files.values()

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
        output_file = get_filepath(self.base_path, identifier)
        # Copy the uploaded file
        shutil.copyfile(filename, output_file)
        # Add file to file index
        f_handle = FileHandle(
            identifier,
            filepath=output_file,
            file_name=name,
            file_format=get_fileformat(filename)
        )
        write_index_file(self.index_file, self.list_files() + [f_handle])
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
        output_file = get_filepath(self.base_path, identifier)
        # Save the file object to the new file path
        file.save(output_file)
        f_handle = FileHandle(
            identifier,
            filepath=output_file,
            file_name=file_name,
            file_format=get_fileformat(file_name)
        )
        write_index_file(self.index_file, self.list_files() + [f_handle])
        return f_handle


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_filepath(base_dir, file_id):
    """Get the absolute path for the file with the given identifier.

    Parameters
    ----------
    base_dir: string
        Path of file store base directory
    file_id: string
        Unique file identifier
    """
    return os.path.join(base_dir, file_id + FILENAME_SUFFIX)


def write_index_file(filename, files):
    """Write content of the file index. The output is a tab-delimited file where
    each row has one or two columns. The first column is the file identifier.
    The optional second column is the file format. This information is omitted
    for files with unknown format.

    Parameters
    ----------
    filename: string
        Absolute path to the output file
    files: list(vizier.filestore.base.FileHandle)
        List of file handles
    """
    with open(filename, 'w') as f:
        for fh in files:
            line = fh.identifier + '\t' + fh.file_name
            if not fh.file_format is None:
                line += '\t' + fh.file_format
            f.write(line + '\n')
