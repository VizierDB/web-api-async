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

"""Factory for vizier engine configurations. Each configuration has a unique
identifier. Configuration-specific parameters are passed via a dictionary.

Modify this class to add new engine configurations.
"""

import vizier.config.engine.dev as dev


class VizierEngineFactory(object):
    """Factory for vizier engines that are used by various applications."""
    @staticmethod
    def get_engine(identifier, config):
        """Create an instance of the vizier engine for a given configuration.
        Each configuration is identified by a unique key.

        Parameters
        ----------
        identifier: string
            Unique configuration identifier
        config: vizier.config.base.ServerConfig
            Server configuration object

        Returns
        -------
        vizier.engine.base.VizierEngine
        """
        # For each new configuration add a clause the the if-statement
        if identifier in dev.ENGINES:
            return dev.DevEngineFactory.get_engine(
                identifier=identifier,
                config=config
            )
        else:
            raise ValueError('unknown configuration identifer \'' + str(identifier) + '\'')
