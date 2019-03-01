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

"""Implementation of commands that interact with a notebook."""

import os
import requests

from vizier.api.client.cli.command import Command
from vizier.api.client.cli.packages import parse_command, print_commands
from vizier.api.client.cli.util import ts

import vizier.api.serialize.hateoas as ref


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

    def download_dataset(self, name, target_file, module_id=None):
        """Download dataset with given name to target file. If the module
        identifier is given the version of the dataset in the resulting state
        of the module will be downloaded. Otherwise, the dataset in the current
        notebokk state is downloaded.

        Parameters
        ----------
        name: string
            Dataset name
        target_file: string
            Target path for storing the downloaded file
        module_id: string, optional
            Unique module identifier

        Returns
        -------
        bool
        """
        # Ensure that the specified file exists
        notebook = self.api.get_notebook()
        module = notebook.get_module(module_id)
        if not module is None and name in module.datasets:
            ds = notebook.get_dataset(module.datasets[name])
            notebook.download_dataset(dataset=ds, target_file=target_file)
        else:
            print 'unknown dataset \'' + name + '\''
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
            elif tokens[0] == 'show' and tokens[1] in ['nb', 'notebook']:
                # show [notebook | nb]
                return self.show_notebook()
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
            elif tokens[0] == 'show' and tokens[1] in ['nb', 'notebook']:
                # show [notebook | nb] <workflow-id>
                return self.show_notebook(workflow_id=tokens[2])
        elif len(tokens) == 5 and tokens[0] not in ['nb', 'notebook']:
            # load <name> from file <file>
            if tokens[0] == 'show' and tokens[1] == 'chart' and tokens[3] == 'in':
                # show chart <name> {in <module-id>}
                return self.show_chart(name=tokens[2], module_id=tokens[4])
            elif tokens[0] == 'show' and tokens[1] == 'dataset' and tokens[3] == 'in':
                # show dataset <name> {in <module-id>}
                return self.show_dataset(name=tokens[2], module_id=tokens[4])
            elif tokens[0:2] == ['download', 'dataset'] and tokens[3] == 'to':
                # download dataset <name> to <target-path>
                return self.download_dataset(
                    name=tokens[2],
                    target_file=tokens[4]
                )
        elif len(tokens) == 7 and tokens[0:2] == ['download', 'dataset']:
            if tokens[0:2] == ['download', 'dataset'] and tokens[3] == 'in' and tokens[5] == 'to':
                # download dataset <name> in <module-id> to <target-path>
                return self.download_dataset(
                    name=tokens[2],
                    module_id=tokens[4],
                    target_file=tokens[6]
                )
        elif len(tokens) >= 3 and tokens[0] in ['nb', 'notebook']:
            if tokens[1] == 'append':
                # [notebook | nb] append <command>
                notebook = self.api.get_notebook()
                datasets = None
                if len(notebook.workflow.modules) > 0:
                    datasets = dict()
                    for m in notebook.workflow.modules:
                        for name in m.datasets:
                            datasets[name] = m.datasets[name]
                return self.append_module(
                    command=parse_command(
                        tokens=tokens[2:],
                        notebook=notebook,
                        datasets=datasets
                    ),
                    notebook=notebook
                )
            elif len(tokens) >= 4 and tokens[1] == 'replace':
                # [notebook | nb] replace <module-id> <command>
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
                # [notebook | nb] insert before <module-id> <command>
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
        print '  [notebook | nb] append <command>'
        print '  [notebook | nb] cancel'
        print '  [notebook | nb] delete <module-id>'
        print '  [notebook | nb] insert before <module-id> <command>'
        print '  [notebook | nb] replace <module-id> <command>'
        print '  show chart <name> {in <module-id>}'
        print '  show [notebook | nb] {<workflow-id>}'
        print '\nDatasets'
        print '  download dataset <name> {in <module-id>} to <target-path>'
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
        module = notebook.get_module(module_id)
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
        module = notebook.get_module(module_id)
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

    def show_notebook(self, workflow_id=None):
        """List all modules for a given workflow. If the workflow identifier is
        not given the branch head is used as the workflow.
        """
        workflow = self.api.get_workflow(
            project_id=self.api.get_default_project(),
            branch_id=self.api.get_default_branch(),
            workflow_id=workflow_id
        )
        if workflow.is_empty:
            print 'Notebook is empty'
            return True
        print 'Workflow ' + workflow.identifier + ' (created at ' + ts(workflow.created_at) + ')'
        for i in range(len(workflow.modules)):
            module = workflow.modules[i]
            cell_id = '[' + str(i+1) + '] '
            indent = ' ' * len(cell_id)
            print '\n' + cell_id + '(' + module.state.upper() + ') ' + module.identifier
            timestamps = 'Created @ ' + ts(module.timestamp.created_at)
            if not module.timestamp.started_at is None:
                timestamps += ', Started @ ' + ts(module.timestamp.started_at)
            if not module.timestamp.finished_at is None:
                timestamps += ', Finished @ ' + ts(module.timestamp.finished_at)
            print indent + timestamps
            print indent + '--'
            for line in module.external_form.split('\n'):
                print indent + line
            if len(module.outputs) > 0:
                print indent + '--'
                for line in module.outputs:
                    if '\n' in line:
                        sublines = line.split('\n')
                        for l in sublines:
                            print indent + l
                    else:
                        print indent + line
            if len(module.datasets) > 0:
                print indent + '--'
                print indent + 'Datasets: ' + ', '.join(module.datasets)
            if len(module.charts) > 0:
                print indent + 'Charts: ' + ', '.join(module.charts.keys())
            print '.'
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
