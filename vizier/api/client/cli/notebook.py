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
        wf = notebook.append_cell(command=command)
        print_modules(wf.modules)
        return True

    def cancel_exec(self):
        """Cancel exection of tasks for the default notebook."""
        wf = self.api.get_notebook().cancel_exec()
        print_modules(wf.modules)
        return True

    def delete_cell(self, module_id):
        """Delete the module with given identifier."""
        wf = self.api.get_notebook().delete_module(module_id=module_id)
        print_modules(wf.modules)
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
                # [notebook | nb] cancel
                return self.cancel_exec()
        elif len(tokens) == 3:
            if tokens[0] in ['nb', 'notebook'] and tokens[1] == 'delete':
                # [notebook | nb] delete <module-id>
                return self.delete_cell(module_id=tokens[2])
            elif tokens[0] == 'show' and tokens[1] == 'chart':
                # show chart <name>
                return self.show_chart(name=tokens[2])
            elif tokens[0] == 'show' and tokens[1] == 'dataset':
                # show dataset <name>
                return self.show_dataset(name=tokens[2])
        elif len(tokens) == 5 and tokens[0] not in ['nb', 'notebook']:
            # load <name> from file <file>
            if tokens[0] == 'show' and tokens[1] == 'chart' and tokens[3] == 'in':
                # show chart <name> {in <module-id>}
                return self.show_chart(name=tokens[2], module_id=tokens[4])
            elif tokens[0] == 'show' and tokens[1] == 'dataset' and tokens[3] == 'in':
                # show dataset <name> {in <module-id>}
                return self.show_dataset(name=tokens[2], module_id=tokens[4])
        elif len(tokens) >= 3 and tokens[0] in ['nb', 'notebook']:
            if tokens[1] == 'append':
                # [notebook | nb] append <cmd>
                notebook = self.api.get_notebook()
                datasets = None
                if len(notebook.workflow.modules) > 0:
                    datasets = notebook.workflow.modules[-1].datasets
                return self.append_module(
                    command=parse_command(
                        tokens=tokens[2:],
                        notebook=notebook,
                        datasets=datasets
                    ),
                    notebook=notebook
                )
            elif len(tokens) >= 4 and tokens[1] == 'replace':
                # [notebook | nb] replace <module-id> <cmd>
                notebook = self.api.get_notebook()
                module = notebook.get_module(tokens[2])
                if not module is None:
                    return self.replace_module(
                        command=parse_command(
                            tokens=tokens[3:],
                            notebook=notebook,
                            datasets=module.datasets
                        ),
                        notebook=notebook,
                        module_id=module.identifier
                    )
            elif len(tokens) >= 5 and tokens[1] == 'insert' and tokens[2] == 'before':
                # [notebook | nb] insert before <module-id> <cmd>
                notebook = self.api.get_notebook()
                module = notebook.get_module(tokens[3])
                if not module is None:
                    return self.insert_module(
                        command=parse_command(
                            tokens=tokens[4:],
                            notebook=notebook,
                            datasets=module.datasets
                        ),
                        notebook=notebook,
                        before_module=module.identifier
                    )
            else:
                print tokens
        return False

    def help(self):
        """Print help statement."""
        print '\nNotebooks'
        print '  [notebook | nb] append <cmd>'
        print '  [notebook | nb] cancel'
        print '  [notebook | nb] delete <module-id>'
        print '  [notebook | nb] insert before <module-id> <cmd>'
        print '  [notebook | nb] replace <module-id> <cmd>'
        print '  show chart <name> {in <module-id>}'
        print '  show dataset <name> {in <module-id>}'
        print '\nCommands'
        print_commands()

    def insert_module(self, command, notebook, before_module):
        """Insert the given command as new cell to the current notebook. The
        cell is inserted before the module with the given identifier.

        The command may be None in which case no action is taken and False is
        returned.

        Parameters
        ----------
        command: vizier.engine.module.command.ModuleCommand
            Notebook cell command
        notebook: vizier.api.client.resources.notebook.Notebook
            Current notebook state
        before_module: string
            Unique module identifier

        Returns
        -------
        bool
        """
        if command is None:
            return False
        wf = notebook.insert_cell(command=command, before_module=before_module)
        if wf is None:
            return False
        print_modules(wf.modules)
        return True

    def show_chart(self, name, module_id=None):
        """Print data series values for a chart in a given module. The chart is
        identified by the user-provided name. If the module is not given the
        last module in the notebook is used as default.
        """
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
        if not module is None and name in module.charts:
            chart = self.api.fetch_chart(module.charts[name].links['self'])
            rows = [[s.name for s in chart.data]]
            for i in range(chart.data[0].length):
                rows.append([str(s.values[i]) for s in chart.data])
            self.output(rows)
        else:
            print 'unknown dataset \'' + name + '\''
        return True

    def show_dataset(self, name, module_id=None):
        """List rows for a dataset in a given module. The dataset is identified
        by the user-provided name. If the module is not given the last module
        in the notebook is used as default.
        """
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
            ds = notebook.fetch_dataset(
                dataset=notebook.workflow.datasets[module.datasets[name]]
            )
            header = ['[ID]'] + [col.name for col in ds.columns]
            rows = [header]
            for row in ds.fetch_rows():
                values = [str(row.identifier)] + [str(val) for val in row.values]
                rows.append(values)
            self.output(rows)
        else:
            print 'unknown dataset \'' + name + '\''
        return True

    def replace_module(self, command, notebook, module_id):
        """Replace the current command in a notebook cell with the given
        command. The command may be None in which case no action is taken and
        False is returned.

        Parameters
        ----------
        command: vizier.engine.module.command.ModuleCommand
            Notebook cell command
        notebook: vizier.api.client.resources.notebook.Notebook
            Current notebook state
        before_module: string
            Unique module identifier

        Returns
        -------
        bool
        """
        if command is None:
            return False
        wf = notebook.replace_cell(command=command, module_id=module_id)
        if wf is None:
            return False
        print_modules(wf.modules)
        return True


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def print_modules(modules):
    """Print status information for a list of modules.

    Parameters
    ----------
    modules: list(vizier.api.client.resources.ModuleResource)
        List of workflow modules
    """
    for m in modules:
        print '[' + m.identifier + ']' '(' + m.state.upper() + ')'
        print m.external_form
