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

"""Implementation of commands that interact with a notebook."""

from vizier.api.client.cli.command import Command
from vizier.engine.packages.base import FILE_ID, FILE_URI

import vizier.api.client.command.vizual as vizual


class NotebookCommands(Command):
    """"Collection of commands that interact with a notebook."""
    def __init__(self, api):
        """Initialize the viztrails API.

        Parameters
        ----------
        api: vizier.api.client.base.VizierApiClient
            Vizier API client
        """
        self.api = api

    def eval(self, tokens):
        """Currently supports the following commands:

        list projects: Print a listing of all viztrails in the repository

        Parameters
        ----------
        tokans: list(string)
            List of tokens in the command line
        """
        if len(tokens) == 3:
            # run python [<script> | <file>]
            if tokens[0] == 'run' and tokens[1] == 'script':
                return True
        elif len(tokens) == 5:
            # load <name> from file <file>
            if tokens[0] == 'load' and tokens[2] == 'from' and tokens[3] == 'file':
                return self.load_dataset_from_file(tokens[1], tokens[4])
            # load <name> from url <url>
            elif tokens[0] == 'load' and tokens[2] == 'from' and tokens[3] == 'url':
                return self.load_dataset_from_url(tokens[1], tokens[4])
        return False

    def help(self):
        """Print help statement."""
        print '\nNotebooks'
        print '  load <name> from file <file>'
        print '  load <name> from url <url>'
        print '  run python [<script> | <file>]'

    def load_dataset_from_file(self, name, file):
        """Create a new dataset from a given file."""
        # Ensure that the specified file exists
        file_id = self.api.upload_file(filename=file)
        notebook = self.api.get_notebook()
        modules = notebook.append_cell(
            command=vizual.load_dataset(
                dataset_name=name,
                file={FILE_ID: file_id}
            )
        )
        print modules
        return True

    def load_dataset_from_url(self, name, url):
        """Create a new dataset from a given file."""
        # Ensure that the specified file exists
        notebook = self.api.get_notebook()
        modules = notebook.append_cell(
            command=vizual.load_dataset(
                dataset_name=name,
                file={FILE_URI: url}
            )
        )
        print modules
        return True
