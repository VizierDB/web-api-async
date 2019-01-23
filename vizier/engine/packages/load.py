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

"""Helper methods to initialize the set of supported packages."""

import os

from vizier.core.io.base import read_object_from_file
from vizier.engine.packages.base import PackageIndex


def load_packages(path):
    """Load package declarations from directories in the given path. The
    packages path may contain multiple directories separated by ':'. The
    directories in the path are processed in reverse order to ensure that
    loaded packages are not overwritten by declarations that occur in
    directories later in the path.

    Returns
    -------
    dict(vizier.engine.package.base.PackageIndex)
    """
    packages = dict()
    for dir_name in path.split(':')[::-1]:
        for filename in os.listdir(dir_name):
            filename = os.path.join(dir_name, filename)
            if os.path.isfile(filename):
                pckg = read_object_from_file(filename)
                for key in pckg:
                    packages[key] = PackageIndex(package=pckg[key])
    return packages
