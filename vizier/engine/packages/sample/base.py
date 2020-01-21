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

"""Specification of parameters for SQL cell."""

import vizier.engine.packages.base as pckg


"""Global constants."""

# Package name
PACKAGE_SAMPLE = 'sample'

# Command identifier (unique within the package)
BASIC_SAMPLE = "basic_sample"

# SQL command parameters
PARA_INPUT_DATASET = 'input_dataset'
PARA_OUTPUT_DATASET = 'output_dataset'
PARA_SAMPLING_RATE = 'rate'
