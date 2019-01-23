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

"""Implementation of commands that interact with the default settings."""

from vizier.api.client.base import KEY_DEFAULT_BRANCH, KEY_DEFAULT_PROJECT
from vizier.api.client.base import MSG_NO_DEFAULT_BRANCH, MSG_NO_DEFAULT_PROJECT
from vizier.api.client.cli.command import Command


class SettingCommands(Command):
    """"Collection of commands that interact with the default settings."""
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
        if len(tokens) == 1:
            # defaults
            if tokens[0] == 'defaults':
                return self.print_defaults()
        elif len(tokens) == 3:
            # set project <project-id>
            if tokens[0] == 'set' and tokens[1] == 'branch':
                return self.set_branch(tokens[2])
            # set project <project-id>
            elif tokens[0] == 'set' and tokens[1] == 'project':
                return self.set_project(tokens[2])
        return False

    def help(self):
        """Print help statement."""
        print '\nSettings'
        print '  defaults'
        print '  set project <project-id>'
        print '  set branch <branch-id>'

    def print_defaults(self):
        """Print the current defaults."""
        if not self.api.default_project is None:
            project = self.api.get_project(project_id=self.api.default_project)
            print 'Project \'' + project.name + '\' (' + project.identifier + ')'
            if not self.api.default_branch is None:
                branch = self.api.get_branch(
                    project_id=project.identifier,
                    branch_id=self.api.default_branch
                )
                print 'On branch \'' + branch.name + '\' (' + branch.identifier + ')'
            else:
                print MSG_NO_DEFAULT_BRANCH
        else:
            print MSG_NO_DEFAULT_PROJECT
        return True

    def set_branch(self, branch_id):
        """Set the default branch."""
        if self.api.default_project is None:
            print MSG_NO_DEFAULT_PROJECT
            return True
        branch = None
        for br in self.api.list_branches(project_id=self.api.default_project):
            if br.identifier == branch_id:
                branch = br
                break
        if not branch is None:
            self.api.defaults.add(
                key=KEY_DEFAULT_BRANCH,
                value=branch_id,
                replace=True
            )
            print 'On branch \'' + branch.name + '\''
        else:
            print 'Unknown branch ' + branch_id
        return True

    def set_project(self, project_id):
        """Set the default project."""
        project = self.api.get_project(project_id=project_id)
        if not project is None:
            self.api.default_project = project_id
            self.api.defaults.add(
                key=KEY_DEFAULT_PROJECT,
                value=project_id,
                replace=True
            )
            print 'Default project is now \'' + project.name + '\''
            if not project.default_branch is None:
                print 'Default branch is ' + project.default_branch
                self.api.defaults.add(
                    key=KEY_DEFAULT_BRANCH,
                    value=project.default_branch,
                    replace=True
                )
            else:
                self.api.defaults.delete(key=KEY_DEFAULT_BRANCH)
        else:
            print 'Unknown project: ' + project_id
        return True
