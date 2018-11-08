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

"""System commands package."""

import vizier.workflow.packages.base as pckg


# Package name
PACKAGE_SYS = '_sys'

# Unique package-specific command identifiers
SYS_CREATE_BRANCH = 'createBranch'

# Declaration of system command package
SYSTEM_COMMANDS = pckg.package_declaration(
    identifier=PACKAGE_SYS,
    commands=[
        pckg.command_declaration(
            identifier=SYS_CREATE_BRANCH,
            name='Create Branch'
        )
    ]
)
