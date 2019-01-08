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
from vizier.api.client.cli.packages import parse_command, print_commands


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

    def append_module(self, command, notebook):
        """Append the given command as new cellt o the current notebook. The
        command may be None in which case no action is taken and False is
        returned.

        Parameters
        ----------
        command: vizier.engine.module.command.ModuleCommand
            Notebook cell command
        notebook: vizier.api.client.resources.notebook.Notebook
            Current notebook state

        Returns
        -------
        bool
        """
        if command is None:
            return False
        notebook.append_cell(command=command)
        return True

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
            # [notebook | nb] cancel
            if tokens[0] in ['nb', 'notebook'] and tokens[1] == 'cancel':
                return self.cancel_exec()
        elif len(tokens) == 3:
            # run python [<script> | <file>]
            if tokens[0] in ['nb', 'notebook'] and tokens[1] == 'delete':
                return self.delete_cell(module_id=tokens[2])
            elif tokens[0] == 'show' and tokens[1] == 'dataset':
                # show dataset <name>
                return self.show_dataset(name=tokens[2])
        elif len(tokens) == 5:
            # load <name> from file <file>
            if tokens[0] == 'show' and tokens[1] == 'dataset' and tokens[3] == 'in':
                # show dataset <name> {in <module-id>}
                return self.show_dataset(name=tokens[2], module_id=tokens[4])
        elif len(tokens) >= 3:
            if tokens[0] in ['nb', 'notebook'] and tokens[1] == 'append':
                # [notebook | nb] append <cmd>
                notebook = self.api.get_notebook()
                return self.append_module(
                    command=parse_command(tokens=tokens[2:], notebook=notebook),
                    notebook=notebook
                )
        elif len(tokens) >= 4:
            # [notebook | nb] replace <module-id> <cmd>
            if tokens[0] in ['nb', 'notebook'] and tokens[1] == 'replace':
                return True
        elif len(tokens) >= 5:
            # [notebook | nb] insert before <module-id> <cmd>
            if tokens[0] in ['nb', 'notebook'] and tokens[1] == 'insert' and tokens[2] == 'before':
                return True
        return False

    def help(self):
        """Print help statement."""
        print '\nNotebooks'
        print '  [notebook | nb] append <cmd>'
        print '  [notebook | nb] cancel'
        print '  [notebook | nb] delete <module-id>'
        print '  [notebook | nb] insert before <module-id> <cmd>'
        print '  [notebook | nb] replace <module-id> <cmd>'
        print '  show dataset <name> {in <module-id>}'
        print '\nCommands'
        print_commands()

    def show_dataset(self, name, module_id=None):
        """Create a new dataset from a given file."""
        # Ensure that the specified file exists
        notebook = self.api.get_notebook()
        module = None
        if module_id is None and len(notebook.workflow.modules) > 0:
            module = notebook.workflow.modules[-1]
        else:
            for m in notebook.workflow.modules:
                if m.identifier == module_id:
                    module = m
                    break
        if not module is None and name in module.datasets:
            ds = notebook.fetch_dataset(identifier=module.datasets[name])
            rows = [[col['name'] for col in ds['columns']]]
            for row in ds['rows']:
                values = [str(val) for val in row['values']]
                rows.append(values)
            self.output(rows)
        else:
            print 'unknown dataset \'' + name + '\''
        return True
