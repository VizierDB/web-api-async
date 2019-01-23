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

"""Implementation of commands that interact with the Api's viztrails
repository.
"""

from vizier.api.client.base import KEY_DEFAULT_BRANCH, KEY_DEFAULT_PROJECT
from vizier.api.client.cli.command import Command
from vizier.api.client.cli.util import ts
from vizier.viztrail.base import PROPERTY_NAME


class ViztrailsCommands(Command):
    """"Collection of commands that interact with the viztrails repository."""
    def __init__(self, api):
        """Initialize the viztrails API.

        Parameters
        ----------
        api: vizier.api.client.base.VizierApiClient
            Vizier API client
        """
        self.api = api

    def create_branch(self, name, workflow_id=None, module_id=None):
        """Create a new branch in the default project."""
        project_id = self.api.get_default_project()
        branch_id = None
        if not workflow_id is None and not module_id is None:
            branch_id = self.api.get_default_branch()
        branch = self.api.create_branch(
            project_id=project_id,
            branch_id=branch_id,
            workflow_id=workflow_id,
            module_id=module_id,
            properties={PROPERTY_NAME: name}
        )
        print 'Branch ' + name + ' created (' + branch.identifier + ')'
        return True

    def create_project(self, name):
        """Create a new project with given name."""
        project = self.api.create_project(
            properties={PROPERTY_NAME: name}
        )
        print 'Project ' + name + ' created (' + project.identifier + ')'
        return True

    def delete_branch(self, branch_id):
        """Delete branch with given idnetifier from default project."""
        result = self.api.delete_branch(
            project_id=self.api.get_default_project(),
            branch_id=branch_id
        )
        if result:
            print 'Branch deleted'
        else:
            print 'Unknown branch ' + branch_id
        return True

    def delete_project(self, project_id):
        """Delete project with given idnetifier."""
        result = self.api.delete_project(project_id)
        if result:
            default_project = self.api.get_default_project()
            if not default_project is None:
                if default_project == project_id:
                    self.api.defaults.delete(KEY_DEFAULT_PROJECT)
            print 'Project deleted'
        else:
            print 'Unknown project ' + project_id
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
            # list branches
            if tokens[0] == 'list' and tokens[1] == 'branches':
                return self.list_branches()
            # list projects
            elif tokens[0] == 'list' and tokens[1] == 'projects':
                return self.list_projects()
            # show history
            elif tokens[0] == 'show' and tokens[1] in ['history', 'notebooks']:
                return self.list_workflows()
        elif len(tokens) == 3:
            # create branch <name>
            if tokens[0] == 'create' and tokens[1] == 'branch':
                return self.create_branch(tokens[2])
            # create project <name>
            elif tokens[0] == 'create' and tokens[1] == 'project':
                return self.create_project(tokens[2])
            # delete branch <branch-id>
            elif tokens[0] == 'delete' and tokens[1] == 'branch':
                return self.delete_branch(tokens[2])
            # delete project <project-id>
            elif tokens[0] == 'delete' and tokens[1] == 'project':
                return self.delete_project(tokens[2])
            # rename branch <name>
            elif tokens[0] == 'rename' and tokens[1] == 'branch':
                return self.rename_branch(tokens[2])
            # rename project <name>
            elif tokens[0] == 'rename' and tokens[1] == 'project':
                return self.rename_project(tokens[2])
        elif len(tokens) == 6:
            # create branch <name> from module <module-id>
            if tokens[0:2] == ['create', 'branch'] and tokens[3:5] == ['from', 'module']:
                notebook = self.api.get_notebook()
                return self.create_branch(
                    name=tokens[2],
                    workflow_id=notebook.workflow.identifier,
                    module_id=tokens[5]
                )
        elif len(tokens) == 8:
            # create branch <name> from workflow <workflow-id> module <module-id>
            if tokens[0:2] == ['create', 'branch'] and tokens[3:5] == ['from', 'workflow'] and tokens[6] == 'module':
                return self.create_branch(
                    name=tokens[2],
                    workflow_id=tokens[5],
                    module_id=tokens[7]
                )
        return False

    def help(self):
        """Print help statement."""
        print '\nProjects'
        print '  create branch <name>'
        print '  create branch <name> from module <module-id>'
        print '  create branch <name> from workflow <workflow-id> module <module-id>'
        print '  create branch <name>'
        print '  create project <name>'
        print '  delete branch <branch-id>'
        print '  delete project <project-id>'
        print '  list branches'
        print '  list projects'
        print '  rename branch <name>'
        print '  rename project <name>'
        print '  show [history | notebooks]'

    def list_branches(self):
        """Print listing of branches for default project in tabular format."""
        branches = self.api.list_branches(self.api.get_default_project())
        rows = list()
        rows.append(['Identifier', 'Name', 'Created at', 'Last modified'])
        for branch in branches:
            rows.append([
                branch.identifier,
                branch.name,
                ts(branch.created_at),
                ts(branch.last_modified_at)
            ])
        print
        self.output(rows)
        print '\n' + str(len(branches)) + ' branch(es)\n'
        return True

    def list_projects(self):
        """Print listing of active projects in tabular format."""
        projects = self.api.list_projects()
        rows = list()
        rows.append(['Identifier', 'Name', 'Created at', 'Last modified'])
        for project in projects:
            rows.append([
                project.identifier,
                project.name,
                ts(project.created_at),
                ts(project.last_modified_at)
            ])
        print
        self.output(rows)
        print '\n' + str(len(projects)) + ' project(s)\n'
        return True

    def list_workflows(self):
        """Print listing of workflows in the history of the default branch in
        tabular format.
        """
        branch = self.api.get_branch(
            project_id=self.api.get_default_project(),
            branch_id=self.api.get_default_branch()
        )
        rows = list()
        rows.append(['Identifier', 'Action', 'Command', 'Created at'])
        for workflow in branch.workflows:
            rows.append([
                workflow.identifier,
                workflow.action.upper(),
                workflow.command,
                ts(workflow.created_at)
            ])
        print
        self.output(rows)
        print '\n' + str(len(branch.workflows)) + ' workflow(s)\n'
        return True

    def rename_branch(self, name):
        """Rename the default branch."""
        self.api.update_branch(
            project_id=self.api.get_default_project(),
            branch_id=self.api.get_default_branch(),
            properties={PROPERTY_NAME: name}
        )
        print 'Branch renamed to ' + name
        return True

    def rename_project(self, name):
        """Rename the default project."""
        self.api.update_project(
            project_id=self.api.get_default_project(),
            properties={PROPERTY_NAME: name}
        )
        print 'Project renamed to ' + name
        return True
