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

"""Implementation of commands that interact with the Api's viztrails
repository.
"""

from vizier.client.cli.command import Command
from vizier.core.timestamp import utc_to_local
from vizier.viztrail.base import PROPERTY_NAME


"""Annotation keys for default values."""
KEY_DEFAULT_PROJECT = 'project'


"""Default error messages."""
MSG_NO_DEFAULT_PROJECT = 'Default project not set'


"""Default timestamp format."""
TIME_FORMAT = '%d-%m-%Y %H:%M'


class ViztrailsCommands(Command):
    """"Collection of commands that interact with the viztrails repository."""
    def __init__(self, api, defaults):
        """Initialize the viztrails repository manager from the API object.

        Parameters
        ----------
        api: vizier.api.base.VizierApi
            API for vizier instance
        defaults: vizier.core.annotation.base.ObjectAnnotationSet
            Annotation set for default values
        """
        self.viztrails = api.projects
        self.defaults = defaults
        # Set the default project
        project_id = self.defaults.find_one(KEY_DEFAULT_PROJECT)
        if not project_id is None:
            self.default_project = self.viztrails.get_project(project_id)
            self.default_branch = self.default_project.get_default_branch()
        else:
            self.default_project = None
            self.default_branch = None

    def create_branch(self, name):
        """Create a new branch in the default project."""
        if self.default_project is None:
            print MSG_NO_DEFAULT_PROJECT
            return True
        branch = self.viztrails.create_branch(
            project_id=self.default_project.identifier,
            properties={PROPERTY_NAME: name}
        )
        print 'Branch ' + name + ' created with identifier ' + branch.identifier
        return True

    def create_project(self, name):
        """Create a new project with given name."""
        vt = self.viztrails.create_project(
            properties={PROPERTY_NAME: name}
        )
        print 'Project ' + name + ' created with identifier ' + vt.identifier
        return True

    def delete_branch(self, branch_id):
        """Delete branch with given idnetifier from default project."""
        if self.default_project is None:
            print MSG_NO_DEFAULT_PROJECT
            return True
        result = self.viztrails.delete_branch(
            project_id=self.default_project.identifier,
            branch_id=branch_id
        )
        if result:
            print 'Branch deleted'
        else:
            print 'Unknown branch ' + branch_id
        return True

    def delete_project(self, project_id):
        """Delete project with given idnetifier."""
        result = self.viztrails.delete_project(project_id=project_id)
        if result:
            if not self.default_project is None:
                if self.default_project.identifier == project_id:
                    self.default_project = None
                    self.defaults.delete(KEY_DEFAULT_PROJECT)
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
        if len(tokens) == 1:
            # defaults
            if tokens[0] == 'defaults':
                return self.print_defaults()
        elif len(tokens) == 2:
            # list branches
            if tokens[0] == 'list' and tokens[1] == 'branches':
                return self.list_branches()
            # list projects
            elif tokens[0] == 'list' and tokens[1] == 'projects':
                return self.list_projects()
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
            # set project <project-id>
            elif tokens[0] == 'set' and tokens[1] == 'branch':
                return self.set_branch(tokens[2])
            # set project <project-id>
            elif tokens[0] == 'set' and tokens[1] == 'project':
                return self.set_project(tokens[2])
        return False

    def help(self):
        """Print help statement."""
        print '\nProjects'
        print '  create branch <name>'
        print '  create project <name>'
        print '  defaults'
        print '  delete branch <branch-id>'
        print '  delete project <project-id>'
        print '  list branches'
        print '  list projects'
        print '  rename branch <name>'
        print '  rename project <name>'
        print '  set project <project-id>'
        print '  set branch <branch-id>'

    def list_branches(self):
        """Print listing of branches for default project in tabular format."""
        if self.default_project is None:
            print MSG_NO_DEFAULT_PROJECT
            return True
        branches = self.viztrails.list_branches(
            project_id=self.default_project.identifier
        )
        rows = list()
        rows.append(['Identifier', 'Name', 'Created at', 'Last modified'])
        for branch in branches:
            rows.append([
                branch.identifier,
                branch.name,
                utc_to_local(branch.created_at).strftime(TIME_FORMAT),
                utc_to_local(branch.last_modified_at).strftime(TIME_FORMAT)
            ])
        print
        self.output(rows)
        print '\n' + str(len(branches)) + ' branch(es)\n'
        return True

    def list_projects(self):
        """Print listing of active projects in tabular format."""
        viztrails = self.viztrails.list_projects()
        rows = list()
        rows.append(['Identifier', 'Name', 'Created at', 'Last modified'])
        for vt in viztrails:
            rows.append([
                vt.identifier,
                vt.name,
                utc_to_local(vt.created_at).strftime(TIME_FORMAT),
                utc_to_local(vt.last_modified_at).strftime(TIME_FORMAT)
            ])
        print
        self.output(rows)
        print '\n' + str(len(viztrails)) + ' project(s)\n'
        return True

    def print_defaults(self):
        """Print the current defaults."""
        if not self.default_project is None:
            print 'Project ' + self.default_project.name + ' (' + self.default_project.identifier + ')'
            print 'On branch ' + self.default_branch.name + ' (' + self.default_branch.identifier + ')'
        else:
            print MSG_NO_DEFAULT_PROJECT
        return True

    def rename_branch(self, name):
        """Rename the default branch."""
        self.viztrails.update_branch_properties(
            project_id=self.default_project.identifier,
            branch_id=self.default_branch.identifier,
            properties={PROPERTY_NAME: name}
        )
        print 'Branch renamed to ' + name
        return True

    def rename_project(self, name):
        """Rename the default project."""
        if self.default_project is None:
            print MSG_NO_DEFAULT_PROJECT
            return True
        self.viztrails.update_project_properties(
            project_id=self.default_project.identifier,
            properties={PROPERTY_NAME: name}
        )
        print 'Project renamed to ' + name
        return True

    def set_branch(self, branch_id):
        """Set the default branch."""
        if self.default_project is None:
            print MSG_NO_DEFAULT_PROJECT
            return True
        branch = self.viztrails.set_default_branch(
            project_id=self.default_project.identifier,
            branch_id=branch_id
        )
        if not branch is None:
            print 'On branch ' + branch.name
        else:
            print 'Unknown branch ' + branch_id
        return True

    def set_project(self, project_id):
        """Set the default project."""
        vt = self.viztrails.get_project(project_id=project_id)
        if not vt is None:
            self.default_project = vt
            self.defaults.add(
                key=KEY_DEFAULT_PROJECT,
                value=project_id,
                replace=True
            )
            print 'Default project is now ' + vt.name
        else:
            print 'Unknown project: ' + project_id
        return True
