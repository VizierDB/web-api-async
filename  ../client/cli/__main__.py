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

import os
import sys

from vizier.core.annotation.persistent import PersistentAnnotationSet
from vizier.api.base import VizierApi
from vizier.client.cli.interpreter import CommandInterpreter
from vizier.config import AppConfig


def get_base_directory():
    """Get the directory that contains the vizier configuration files.

    Returns
    -------
    string
    """
    return os.path.abspath('.vizierdb')


def main(args):
    """Read user input from stdin until either quit, exit or CTRL-D is entered.
    """
    # Initialize the vizier Api.
    app_dir = get_base_directory()
    config = AppConfig(configuration_file=os.path.join(app_dir, 'config.yaml'))
    api = VizierApi(
        filestore=config.filestore.create_instance(),
        viztrails_repository=config.viztrails.create_instance()
    )
    # Run the command interpreter on the given arguments
    defaults_file = os.path.join(app_dir, 'defaults.json')
    defaults = PersistentAnnotationSet(object_path=defaults_file)
    CommandInterpreter(api=api, defaults=defaults).eval(args)

if __name__ == '__main__':
    main(args=sys.argv[1:])
