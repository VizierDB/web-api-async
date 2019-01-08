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

from vizier.config.engine.dev import DevEngineFactory, DEV_ENGINE


class VizierEngineFactory(object):
    """Factory for vizier engines that are used by various applications."""
    @staticmethod
    def get_container_engine(identifier, properties=None):
        """Create an instance of the vizier container engine that is used by a
        single- project container. The engine is created from a given dictionary
        containing configuration parameters. Each configuration is identified by
        a unique key.

        Parameters
        ----------
        identifier: string
            Unique configuration identifier
        properties: dict, optional
            Configuration-specific parameters

        Returns
        -------
        vizier.engine.container.VizierContainerEngine
        """
        # For each new configuration add a clause the the if-statement
        if identifier == DEV_ENGINE:
            return DevEngineFactory.get_container_engine(properties)
        else:
            raise ValueError('unknown configuration identifer \'' + str(identifier) + '\'')

    @staticmethod
    def get_engine(identifier, properties=None):
        """Create an instance of the vizier engine for a given configuration.
        Each configuration is identified by a unique key. The properties
        dictionary contains configuration-specific parameters.

        Parameters
        ----------
        identifier: string
            Unique configuration identifier
        properties: dict, optional
            Configuration-specific parameters

        Returns
        -------
        vizier.engine.base.VizierEngine
        """
        # For each new configuration add a clause the the if-statement
        if identifier == DEV_ENGINE:
            return DevEngineFactory.get_engine(properties)
        else:
            raise ValueError('unknown configuration identifer \'' + str(identifier) + '\'')
