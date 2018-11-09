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

"""Run the vizier command line interpreter."""

import sys

from vizier.api import VizierApi
from vizier.client.cli.interpreter import CommandInterpreter
from vizier.config import AppConfig
from vizier.filestore.fs import DefaultFileStore


def main(args):
    """Read user input from stdin until either quit, exit or CTRL-D is entered.
    """
    # Initialize the vizier Api. If the optional configuration file is not given
    # as an argument the default configuration file will be used
    if len(args) == 0:
        config = AppConfig()
    elif len(args) == 1:
        config = AppConfig(configuration_file=args[0])
    else:
        print 'Usage: {<config-file>}'
        return
    api = VizierApi(DefaultFileStore(config.settings.fileserver.directory))
    # Initialize the command interpreter
    cli = CommandInterpreter(api)
    # Read user input until quit, exit or Cnrtl-D is read.
    done = False
    while not done:
        try:
            line = raw_input(cli.prompt())
            if line.lower() in ['quit', 'exit']:
                done = True
            else:
                cli.eval(line)
        except EOFError as eof:
            print
            done = True
        except Exception as ex:
            print str(ex)
    print 'bye'

if __name__ == '__main__':
    main(args=sys.argv[1:])
