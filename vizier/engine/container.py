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

"""The vizier container engine is used by web services that maintain a single
project. the engine is simply a wrapper around the project datastore and
filestore as well as the used execution backend.
"""


class VizierContainerEngine(object):
    """The vizier engine container is a wrapper around the datastore and
    filestore for a project, and the execution backend that is used by the
    container web service.
    """
    def __init__(self, datastore, filestore, backend):
        """Initialize the engine components.

        Parameters
        ----------
        datastore: vizier.datastore.base.Datastore
            Project datastore
        filestore: vizier.filestore.base.FIlestore
            Project filestore
        backend: vizier.engine.backend.base.VizierBackend
            Execution backend
        """
        self.datastore = datastore
        self.filestore = filestore
        self.backend = backend
