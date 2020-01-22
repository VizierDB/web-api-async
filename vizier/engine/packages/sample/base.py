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

# Sampling modes (must correspond w/ Mimir's mimir.algebra.sampling._)
SAMPLING_MODE_UNIFORM_PROBABILITY = 'uniform_probability'
SAMPLING_MODE_STRATIFIED_ON = 'stratified_on'

# Command identifier (unique within the package)
BASIC_SAMPLE = "basic_sample"
MANUAL_STRATIFIED_SAMPLE = "manual_stratified_sample"
AUTOMATIC_STRATIFIED_SAMPLE = "automatic_stratified_sample"

# SQL command parameters
PARA_INPUT_DATASET = 'input_dataset'
PARA_OUTPUT_DATASET = 'output_dataset'
PARA_SAMPLING_RATE = 'sample_rate'
PARA_STRATIFICATION_COLUMN = 'stratification_column'
PARA_STRATA = 'strata'
PARA_STRATUM_VALUE = 'stratum_value'

