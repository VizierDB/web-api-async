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

"""Implementation of commands that interact with the Api's file store."""

from vizier.client.cli.command import Command


class FileStoreCommands(Command):
    """"Collection of commands that interact with the file store."""
    def __init__(self, api):
        """Initialize the file store reference from the Api object.

        Parameters
        ----------
        api: vizier.api.VizierApi
            Vizier Api instance
        """
        self.filestore = api.filestore

    def eval(self, tokens):
        """Currently supports the following commands:

        LIST FILES: Print a listing of all files that are currently managed by
        the file store. The output contains the file identifier, file name and
        file format.

        UPLOAD FILES: Uplod a file from disk to the file store.

        Parameters
        ----------
        tokans: list(string)
            List of tokens in the command line
        """
        if len(tokens) == 2:
            if tokens[0].lower() == 'list' and tokens[1].lower() == 'files':
                print 'File store (' + self.filestore.base_directory + ')'
                files = self.filestore.list_files()
                for fh in files:
                    output = [fh.name(), fh.uri()]
                    if not fh.file_format is None:
                        output.append(fh.file_format)
                    print '\t'.join(output)
                print str(len(files)) + ' file(s)'
                return True
        elif len(tokens) == 3:
            if tokens[0].lower() == 'upload' and tokens[1].lower() == 'file':
                fh = self.filestore.upload_file(tokens[2])
                print 'Uploaded file ' + fh.name() + ' as ' + fh.uri()
                return True
            elif tokens[0].lower() == 'cleanup' and tokens[1].lower() == 'file' and tokens[2].lower() == 'store':
                count = self.filestore.cleanup(active_files=[])
                print 'Removed ' + str(count) + ' unused file(s)'
                return True
        return False

    def help(self):
        """Print help statement."""
        print 'File store'
        print '  CLEANUP FILE STORE'
        print '  LIST FILES'
        print '  UPLOAD FILE <file-path>'
