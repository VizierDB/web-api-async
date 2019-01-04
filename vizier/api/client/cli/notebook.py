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

import os

from vizier.api.client.cli.command import Command
from vizier.engine.packages.base import FILE_ID, FILE_NAME, FILE_URI

import vizier.api.client.command.pycell as pycell
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

    def cancel_exec(self):
        """Cancel exection of tasks for the default notebook."""
        workflow = self.api.get_notebook().cancel_exec()
        for i in range(len(workflow.modules)):
            module = workflow.modules[i]
            cell_id = '[' + str(i+1) + '] '
            indent = ' ' * len(cell_id)
            print cell_id + '(' + module.state.upper() + ') ' + module.identifier
        return True

    def delete_cell(self, module_id):
        """Delete the module with given identifier."""
        modules = self.api.get_notebook().delete_module(module_id=module_id)
        if not modules is None:
            for i in range(len(modules)):
                module = modules[i]
                cell_id = '[' + str(i+1) + '] '
                indent = ' ' * len(cell_id)
                print cell_id + '(' + module.state.upper() + ') ' + module.identifier
        else:
            print 'unknown module ' + module_id
        return True

    def eval(self, tokens):
        """Currently supports the following commands:

        list projects: Print a listing of all viztrails in the repository

        Parameters
        ----------
        tokans: list(string)
            List of tokens in the command line
        """
        if len(tokens) == 2:
            if tokens[0] in ['nb', 'notebook'] and tokens[1] == 'cancel':
                return self.cancel_exec()
        elif len(tokens) == 3:
            # run python [<script> | <file>]
            if tokens[0] in ['nb', 'notebook'] and tokens[1] == 'delete':
                return self.delete_cell(module_id=tokens[2])
            elif tokens[0] == 'run' and tokens[1] == 'python':
                return self.run_python(tokens[2])
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
        print '  notebook cancel'
        print '  notebook delete <module-id>'
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
                file={
                    FILE_ID: file_id,
                    FILE_NAME: os.path.basename(file)
                }
            )
        )
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
        return True


    def run_python(self, script):
        """Append cell with python script."""
        # Ensure that the specified file exists
        if os.path.isfile(script):
            with open(script, 'r') as f:
                source = f.read()
        else:
            source = script
        notebook = self.api.get_notebook()
        modules = notebook.append_cell(pycell.python_cell(source))
        return True
