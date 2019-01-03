# Copyright (C) 2019 New York University,
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

"""Collection of common helper methods for the engine factory."""

from vizier.config.base import read_object_from_file
from vizier.core.loader import ClassLoader
from vizier.engine.packages.base import PackageIndex


"""Parameters for package declarations."""
PARA_PACKAGE_DECLARATION = 'declaration'
PARA_PACKAGE_ENGINE = 'engine'


def load_packages(elements):
    """Load package declaration and processors using a given dictionary. Each
    dictionary is the is expected to contain the following elements:

    - declaration: Path to package declaration file
      engine: Class loader for package processor

    Returns a dictionary of loaded packages and a dictionary of task processors.
    In both dictionaries the unique package identifier is the key.

    Parameters
    ----------
    elements: list(dict)
        List of dictionaries that contain references to the package declaration
        file and the package processor.

    Returns
    -------
    dict(vizier.engine.package.base.PackageIndex),
    dict(vizier.engine.packages.task.processor.TaskProcessor)
    """
    packages = dict()
    processors = dict()
    for el in elements:
        if not PARA_PACKAGE_DECLARATION in el:
            raise ValueError('missing package element \'' + PARA_PACKAGE_DECLARATION + '\'')
        pckg = read_object_from_file(el[PARA_PACKAGE_DECLARATION])
        if PARA_PACKAGE_ENGINE in el:
            engine = ClassLoader(values=el[PARA_PACKAGE_ENGINE]).get_instance()
        else:
            engine = None
        for key in pckg:
            packages[key] = PackageIndex(package=pckg[key])
            if not engine is None:
                processors[key] = engine
    return packages, processors
