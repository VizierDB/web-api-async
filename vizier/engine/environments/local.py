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

"""Implements engine for local installation of vizier. Assumes single user with
full controll over machine. No external access. Work normally on a single
project. Can also be useful for system demonstrations.
"""

from vizier.engine.base import VizierEngine

class LocalVizierEngine(VizierEngine):
    """
    """
    def __init__(self, properties):
        super(PersonalEditionEngine, self).__init__(
            name='Personal Edition',
            version='0.1.0'
        )
