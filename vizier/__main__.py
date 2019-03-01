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

"""Run the vizier command line interpreter."""

import json
import os
import sys

from vizier.api.client.cli import print_header
from vizier.api.client.cli.interpreter import CommandInterpreter
from vizier.api.routes.base import UrlFactory
from vizier.core.annotation.persistent import PersistentAnnotationSet
from vizier.core.io.base import read_object_from_file
from vizier.core.util import load_json

"""Configuration files."""
CONFIG_DIR = '.vizierdb'
CONFIG_FILE = 'cli.json'


def get_base_directory():
    """Get the directory that contains the vizier configuration files.

    Returns
    -------
    string
    """
    return os.path.abspath(CONFIG_DIR)


def main(args):
    """Read user input from stdin until either quit, exit or CTRL-D is entered.
    """
    # Initialize the url factory and read default values.
    app_dir = get_base_directory()
    config_file = os.path.join(app_dir, CONFIG_FILE)
    if len(args) in [1,2] and args[0] == 'init':
        if len(args) == 2:
            url = args[1]
        elif not os.path.isfile(config_file):
            url = 'http://localhost:5000/vizier-db/api/v1'
        else:
            # Print the URL of the API in the configuration file
            config = read_object_from_file(config_file)
            print_header()
            print '\nConnected to API at ' + config['url']
            return
        if not os.path.isdir(app_dir):
            os.makedirs(app_dir)
        with open(config_file, 'w') as f:
            json.dump({'url': url}, f)
    elif not os.path.isfile(config_file):
        raise ValueError('vizier client is not initialized')
    else:
        config = read_object_from_file(config_file)
        defaults_file = os.path.join(app_dir, 'defaults.json')
        defaults = PersistentAnnotationSet(object_path=defaults_file)
        CommandInterpreter(
            urls=UrlFactory(base_url=config['url']),
            defaults=defaults
        ).eval(args)

if __name__ == '__main__':
    #try:
    main(args=sys.argv[1:])
    #except Exception as ex:
    #    print str(ex)
