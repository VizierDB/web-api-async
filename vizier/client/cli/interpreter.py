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

"""Simple command line interpreter to test functionality of the vizier Api."""

from vizier.client.cli.filestore import FileStoreCommands
from vizier.client.cli.viztrails import ViztrailsCommands


class CommandInterpreter(object):
    """The command interpreter allows to run simple commands against an instance
    of the vizier Api.
    """
    def __init__(self, api, defaults):
        """Initialize the vizier Api instance.

        Parameters
        ----------
        api: vizier.api.base.VizierApi
            API for vizier instance
        defaults: vizier.core.annotation.base.ObjectAnnotationSet
            Annotation set for default values
        """
        self.api = api
        self.commands = [
            FileStoreCommands(api),
            ViztrailsCommands(api=api, defaults=defaults)
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
        if len(tokens) == 1 and tokens[0] == 'help':
            print 'Vizier Command Line Interface'
            for cmd in self.commands:
                cmd.help()
        else:
            for cmd in self.commands:
                if cmd.eval(tokens):
                    return
            print 'unknown command: ' + ' '.join(tokens)

    def prompt(self):
        """The current interpreter prompt.

        Returns
        -------
        string
        """
        return 'vizier -> '
