# Copyright (C) 2018 New York University
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

"""Write package definitions to file. These files are part of the server
configuration. A current version of the file for each available package should
be in the config/ folder.

Usage: <package-name> [  MIMIR | PLOT | PYTHON | VIZUAL ] <file-name> {<format> [YAML | JSON]}

If the format argument is omited the output format is determined based on the
file name. If the name ends with .json the output format is JSON otherwise YAML
will be used.
"""

import sys

from vizier.engine.packages.mimir.base import export_package as export_mimir
from vizier.engine.packages.plot.base import export_package as export_plot
from vizier.engine.packages.pycell.base import export_package as export_python
from vizier.engine.packages.scala.base import export_package as export_scala
from vizier.engine.packages.r.base import export_package as export_r
from vizier.engine.packages.sql.base import export_package as export_sql
from vizier.engine.packages.vizual.base import export_package as export_vizual


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) < 1 or len(args) > 3:
        print("""Usage:
  <package-name> [ MIMIR | PLOT | PYTHON | SCALA | R | SQL | VIZUAL ]
  <file-name>
  {<format> [YAML | JSON]}""")
        sys.exit(-1)
    name = args[0]
    filename = args[1]
    if len(args) == 3:
        format = args[2]
    elif filename.endswith('.json'):
        format = 'JSON'
    else:
        format = 'YAML'
    if name.upper() == 'MIMIR':
        export_mimir(filename, format=format)
    elif name.upper() == 'PLOT':
        export_plot(filename, format=format)
    elif name.upper() == 'PYTHON':
        export_python(filename, format=format)
    elif name.upper() == 'SCALA':
        export_scala(filename, format=format)
    elif name.upper() == 'R':
        export_r(filename, format=format)
    elif name.upper() == 'SQL':
        export_sql(filename, format=format)
    elif name.upper() == 'VIZUAL':
        export_vizual(filename, format=format)
    else:
        print('Unknown package name: ' + str(name))
        sys.exit(-1)
