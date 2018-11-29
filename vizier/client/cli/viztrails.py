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
        self.viztrails = api.viztrails
        self.defaults = defaults
        project_id = self.defaults.find_one('projects')
        if not project_id is None:
            self.default_project = self.viztrails.get_viztrail(project_id)
        else:
            self.default_project = None

    def eval(self, tokens):
        """Currently supports the following commands:

        list projects: Print a listing of all viztrails in the repository

        Parameters
        ----------
        tokans: list(string)
            List of tokens in the command line
        """
        if len(tokens) == 2:
            if tokens[0] == 'list' and tokens[1] == 'projects':
                viztrails = self.viztrails.list_projects()
                rows = list()
                rows.append(['Name', 'Identifier', 'Created at', 'Last modified'])
                for vt in viztrails:
                    rows.append([
                        vt.name,
                        vt.identifier,
                        utc_to_local(vt.created_at).strftime('%d-%m-%y %H:%M'),
                        utc_to_local(vt.last_modified_at).strftime('%d-%m-%y %H:%M')
                    ])
                print
                self.output(rows)
                print '\n' + str(len(viztrails)) + ' project(s)\n'
                return True
            elif tokens[0] == 'list' and tokens[1] == 'branches':
                return True
        elif len(tokens) == 3:
            if tokens[0] == 'create' and tokens[1] == 'project':
                vt = self.viztrails.create_project(
                    properties={PROPERTY_NAME: tokens[2]}
                )
                print 'Project created with identifier ' + vt.identifier
                return True
            elif tokens[0] == 'delete' and tokens[1] == 'project':
                project_id = tokens[2]
                result = self.viztrails.delete_project(project_id=project_id)
                if result:
                    if not self.default_project is None:
                        if self.default_project.identifier == project_id:
                            self.default_project = None
                            self.defaults.delete('project')
                    print 'Project deleted'
                else:
                    print 'Unknown project: ' + project_id
                return True
            elif tokens[0] == 'rename' and tokens[1] == 'to':
                if self.default_project is None:
                    print 'No default project set'
                    return True
                self.default_project.update_project_properties(
                    project_id=tokens[2],
                    properties={PROPERTY_NAME: tokens[4]}
                )
                print 'Project renamed to ' + tokens[4]
                return True
            elif tokens[0] == 'use' and tokens[1] == 'project':
                project_id = tokens[2]
                vt = self.viztrails.get_project(project_id=project_id)
                if not vt is None:
                    self.default_project = vt
                    self.defaults.add(
                        key='project',
                        value=project_id,
                        replace=True
                    )
                    print 'Default project is now ' + vt.name
                else:
                    print 'Unknown project: ' + project_id
                return True
        return False

    def help(self):
        """Print help statement."""
        print '\nProjects'
        print '  checkout branch <branch-id>'
        print '  create branch <name> from ???'
        print '  create project <name>'
        print '  delete branch <branch-id>'
        print '  delete project <project-id>'
        print '  list branches'
        print '  list projects'
        print '  rename to <name>'
        print '  use project <project-id>'
