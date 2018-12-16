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
                return self.load_dataset_from_file(tokens[2], tokens[4])
            # load <name> from url <url>
            elif tokens[0] == 'load' and tokens[2] == 'from' and tokens[3] == 'url':
                return True
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
        self.api.load_from_file(
            project_id=self.api.get_default_project(),
            branch_id=self.api.get_default_branch(),
            name=name,
            file=file
        )
