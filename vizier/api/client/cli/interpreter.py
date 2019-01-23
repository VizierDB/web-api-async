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

"""Simple command line interpreter to test functionality of the vizier Api."""

from vizier.api.client.base import VizierApiClient
from vizier.api.client.cli import print_header
from vizier.api.client.cli.notebook import NotebookCommands
from vizier.api.client.cli.setting import SettingCommands
from vizier.api.client.cli.viztrail import ViztrailsCommands


class CommandInterpreter(object):
    """The command interpreter allows to run simple commands against an instance
    of the vizier Api.
    """
    def __init__(self, urls, defaults):
        """Initialize the Url factory for requests and the object with the
        default values.

        Parameters
        ----------
        urls: vizier.api.routes.base.UrlFactory
            Factory for request urls
        defaults: vizier.core.annotation.base.ObjectAnnotationSet
            Annotation set for default values
        """
        self.api = VizierApiClient(urls=urls, defaults=defaults)
        self.commands = [
            SettingCommands(api=self.api),
            ViztrailsCommands(api=self.api),
            NotebookCommands(api=self.api)
        ]

    def eval(self, tokens):
        """Evaluate a command line. Runs through the set of configured commands
        until the first command evaluates to True.

        Parameters
        ----------
        line: string
            Command line
        """
        # Check if the line equals 'help'.
        if len(tokens) == 1:
            if tokens[0] == 'help':
                print_header()
                for cmd in self.commands:
                    cmd.help()
                return
            elif tokens[0] == 'api':
                print_header()
                self.api.info()
                return
        #try:
        for cmd in self.commands:
            if cmd.eval(tokens):
                return
        print 'Unknown command ' + ' '.join(tokens)
        #except Exception as ex:
        #    print str(ex)

    def prompt(self):
        """The current interpreter prompt.

        Returns
        -------
        string
        """
        return 'vizier -> '
