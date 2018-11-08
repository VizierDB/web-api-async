# Copyright (C) 2018 New York University
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

"""Simple interface to upload and retieve files."""

from abc import abstractmethod
import csv
import gzip
import os
import shutil
import yaml

from vizier.core.properties import ObjectProperty
from vizier.core.util import get_unique_identifier
from vizier.core.system import build_info, component_descriptor
from vizier.core.system import VizierSystemComponent
from vizier.core.timestamp import get_current_time, to_datetime


"""File handle property keys."""
FH_COLUMNS = 'columns'
FH_COMPRESSED = 'compressed'
FH_DELIMITER = 'delimiter'
FH_FILENAME = 'filename'
FH_ROWS = 'rows'
FH_UPLOAD_NAME = 'uploadName'
FH_URL = 'url'


class FileHandle(object):
    """File handle containing statistics for an uploaded CSV file."""
    def __init__(self, identifier, name, filepath, created_at=None, last_modified_at=None, properties=None, active=True):
        """Initialize the file identifier, the (full) file path, and information
        about number of columns and rows in the CSV file.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        name: string
            File base name (with suffix)
        filepath: string
            Absolute path to file on disk
        created_at : datetime.datetime
            Timestamp of file upload (UTC)
        last_modified_at : datetime.datetime
            Timestamp of last modification (UTC)
        properties: dict, optional
            Dictionary of file properties
        active: bool
            Flag indicating whether the file is active
        """
        self.identifier = identifier
        self.name = name
        self.filepath = filepath
        self.created_at = created_at if not created_at is None else get_current_time()
        self.last_modified_at = last_modified_at if not last_modified_at is None else self.created_at
        self.properties = properties if not properties is None else dict()
        self.active = active

    @property
    def base_name(self):
        """Get the file base name without suffix identifying the file format.

        Returns
        -------
        string
        """
        if self.name.endswith('.csv') or self.name.endswith('.tsv'):
            return self.name[:-4]
        elif self.name.endswith('.csv.gz') or  self.name.endswith('.tsv.gz'):
            return self.name[:7]
        elif '.' in self.name:
            return self.name[:self.name.rfind('.')]
        else:
            return self.name

    @property
    def columns(self):
        """Return the number of columns in the file (if it was parsed
        successfully as a CSV/TSV file).

        Returns
        -------
        int
        """
        return self.get_property(FH_COLUMNS, default_value=-1)

    @property
    def compressed(self):
        """Flag indicating whether the file is in gzip format. This is currently
        determined by the file suffix at file upload time.

        Returns
        -------
        bool
        """
        return self.get_property(FH_COMPRESSED, default_value=None)

    @property
    def delimiter(self):
        """Determine the column delimiter for a CSV/TSV file based on the suffix
        of the name of the uploaded file.

        Returns
        -------
        string
        """
        return self.get_property(FH_DELIMITER, default_value=None)

    @property
    def filesize(self):
        """Return the size of the uploaded file on disk.

        Returns
        -------
        int
        """
        return os.stat(self.filepath).st_size

    @staticmethod
    def from_dict(doc, func_filepath):
        """Create a file handle instance from a dictionary representations.
        Requires a function that maps the file identifier to the actual file on
        disk.

        Parameters
        ----------
        doc: dict
            Dictionary representation of the file handle
        func_filepath: func
            Function that maps the file identifier to a file on disk.
        """
        # Transform properties list
        properties = dict()
        for p in [ObjectProperty.from_dict(obj) for obj in doc['properties']]:
            properties[p.key] = p.value
        # Return new file handle
        return FileHandle(
            doc['identifier'],
            doc['name'],
            func_filepath(doc['identifier']),
            to_datetime(doc['createdAt']),
            last_modified_at=to_datetime(doc['lastModifiedAt']),
            properties=properties,
            active=doc['active']
        )

    def get_property(self, key, default_value=None):
        """Return the property value for the given key or the default value if
        the key does not exist.

        Parameters
        ----------
        key: string
            Property key
        default_value: any
            Default value for property key

        Returns
        -------
        any
        """
        return self.properties[key] if key in self.properties else default_value

    @property
    def is_verified_csv(self):
        """Flag indicating whether the original file was parsed correctly by
        the CSV parser (i.e., column and row information is not a negative
        number).

        Returns
        -------
        bool
        """
        return self.columns >= 0 and self.rows >= 0

    def open(self):
        """Get open file object for associated file.

        Returns
        -------
        FileObject
        """
        if self.upload_name.endswith('.gz'):
            return gzip.open(self.filepath, 'rb')
        else:
            return open(self.filepath, 'r')

    @property
    def source(self):
        """Get provenance information about the file source. A file may either
        be uploaded from local disk or downloaded from an Url.

        Returns
        -------
        string
        """
        if FH_URL in self.properties:
            return self.properties[FH_URL]
        elif FH_FILENAME in self.properties:
            return self.properties[FH_FILENAME]
        else:
            return self.name
            
    @property
    def rows(self):
        """Return the number of rows in the file (if it was parsed
        successfully as a CSV/TSV file).

        Returns
        -------
        int
        """
        return self.get_property(FH_ROWS, default_value=-1)

    def to_dict(self):
        """Get dictionary serialization for the file object.

        Returns
        -------
        dict
        """
        return {
            'identifier': self.identifier,
            'name': self.name,
            'createdAt' : self.created_at.isoformat(),
            'lastModifiedAt' : self.last_modified_at.isoformat(),
            'properties': [
                ObjectProperty(key, self.properties[key]).to_dict()
                    for key in self.properties
            ],
            'active': self.active
        }

    @property
    def upload_name(self):
        """Name of the originally uploaded file. This remains invariant while
        the file.name property may be changed by the user.

        Returns
        -------
        string
        """
        return self.get_property(FH_UPLOAD_NAME, default_value=self.name)


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
    def get_file(self, identifier):
        """Get handle for file with given identifier. Returns None if no file
        with given identifier exists.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        FileHandle
        """
        raise NotImplementedError

    @abstractmethod
    def list_files(self):
        """Get list of file handles for all uploaded files.

        Returns
        -------
        list(FileHandle)
        """
        raise NotImplementedError

    @abstractmethod
    def rename_file(self, identifier, name):
        """Rename file with given identifier. Returns the file handle for the
        renamed file or None if no such file existed.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        name: string
            New file name

        Returns
        -------
        FileHandle
        """
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, filename, provenance=None):
        """Upload a new file.

        Parameters
        ----------
        filename: string
            Path to file on disk
        provenance: dict, optional
            Optional file provenance information

        Returns
        -------
        FileHandle
        """
        raise NotImplementedError


class DefaultFileServer(FileServer):
    """Default file server implementation. Keeps all files in a folder on disk.
    File metadata is kept in a separate Yaml file."""
    def __init__(self, base_directory):
        """Initialize the base directory that is used for file storage. The
        actual files are kept in a sub-folder (named 'files').

        The base directory and sub-folder will be created if they do not exist.

        Parameters
        ---------
        base_directory : string
            Path to the base directory.
        """
        super(DefaultFileServer, self).__init__(build_info('DefaultFileServer'))
        if not os.path.isdir(base_directory):
            os.makedirs(base_directory)
        self.base_directory = base_directory
        self.index_file = os.path.join(self.base_directory, 'index.yaml')
        self.file_directory = os.path.join(base_directory, 'files')
        if not os.path.isdir(self.file_directory):
            os.makedirs(self.file_directory)
        self.files = self.read_index()

    def delete_file(self, identifier):
        """Delete file with given identifier. Returns True if file was deleted
        or False if no such file existed. In the current implementation files
        will not be deleted from disk to maintain provenance information.
        Deleted files have their active flag set to False, instead.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        bool
        """
        if identifier in self.files:
            fh = self.files[identifier]
            if fh.active == True:
                fh.active = False
                self.write_index(self.files)
                return True
        return False

    def get_file(self, identifier):
        """Get handle for file with given identifier. Returns None if no file
        with given identifier exists.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        FileHandle
        """
        if identifier in self.files:
            fh = self.files[identifier]
            if fh.active:
                return fh
        return None

    def get_filepath(self, identifier):
        """Get absolute path for file with given identifier. Does not check if
        the file exists.

        Parameters
        ----------
        identifier: string
            Unique file identifier

        Returns
        -------
        string
        """
        return os.path.join(self.file_directory, identifier)

    def list_files(self):
        """Get list of file handles for all uploaded files.

        Returns
        -------
        list(FileHandle)
        """
        active_files = list()
        for fh in self.files.values():
            if fh.active:
                active_files.append(fh)
        return active_files

    def read_index(self):
        """Return content of the file index.

        Returns
        -------
        dict(vizier.filestore.base.FileHandle)
        """
        files = dict()
        if os.path.isfile(self.index_file):
            with open(self.index_file, 'r') as f:
                for f_desc in yaml.load(f.read())['files']:
                    fh = FileHandle.from_dict(f_desc, self.get_filepath)
                    files[fh.identifier] = fh
        return files

    def rename_file(self, identifier, name):
        """Rename file with given identifier. Returns the file handle for the
        renamed file or None if no such file existed.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        name: string
            New file name

        Returns
        -------
        FileHandle
        """
        f_handle = None
        for fh in self.files.values():
            if fh.identifier == identifier and fh.active:
                fh.name = name
                fh.last_modified_at = get_current_time()
                f_handle = fh
        if not f_handle is None:
            self.write_index(self.files)
        return f_handle

    def upload_file(self, filename, provenance=None):
        """Upload a new file.

        Parameters
        ----------
        filename: string
            Path to file on disk
        provenance: dict, optional
            Optional file provenance information

        Returns
        -------
        FileHandle
        """
        name = os.path.basename(filename).lower()
        # Determine the file type based on the file name suffix. If the file
        # type is unknoen reader will be None
        csvfile = None
        reader = None
        delimiter = None
        if name.endswith('.csv'):
            csvfile = open(filename, 'r')
            reader = csv.reader(
                csvfile,
                delimiter=',',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                skipinitialspace=True
            )
            delimiter = ','
        elif name.endswith('.csv.gz'):
            csvfile = gzip.open(filename, 'rb')
            reader = csv.reader(
                csvfile, delimiter=',',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                skipinitialspace=True
            )
            delimiter = ','
        elif name.endswith('.tsv'):
            csvfile = open(filename, 'r')
            reader = csv.reader(csvfile, delimiter='\t')
            delimiter = '\t'
        elif name.endswith('.tsv.gz'):
            csvfile = gzip.open(filename, 'rb')
            reader = csv.reader(csvfile, delimiter='\t')
            delimiter = '\t'
        # Parse csv file to get column and row statistics (and to ensure that
        # the file parses).
        if not provenance is None:
            properties = dict(provenance)
        else:
            properties = dict()
        properties[FH_UPLOAD_NAME] =  os.path.basename(filename)
        if not reader is None and not csvfile is None:
            columns = -1
            rows = 0
            try:
                for row in reader:
                    if columns == -1:
                        columns = len(row)
                    else:
                        rows += 1
                properties[FH_COLUMNS] = columns
                properties[FH_ROWS] = rows
                properties[FH_COMPRESSED] = filename.endswith('.gz')
                properties[FH_DELIMITER] = delimiter
            except Exception as ex:
                pass
            csvfile.close()
        # Create a new unique identifier for the file.
        identifier = get_unique_identifier()
        created_at = get_current_time()
        output_file = self.get_filepath(identifier)
        # Copy the uploaded file
        shutil.copyfile(filename, output_file)
        # Add file to file index
        f_handle = FileHandle(
            identifier,
            name,
            output_file,
            created_at,
            properties=properties
        )
        self.files[identifier] = f_handle
        self.write_index(self.files)
        return f_handle

    def write_index(self, files):
        """Write content of the file index.

        Parameters
        -------
        files: dict
            New context for file index
        """
        content = {'files' : [fh.to_dict() for fh in files.values()]}
        with open(self.index_file, 'w') as f:
            yaml.dump(content, f, default_flow_style=False)
