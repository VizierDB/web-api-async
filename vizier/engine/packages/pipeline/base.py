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

import vizier.engine.packages.base as pckg

PARA_INPUT_DATASET = "input_dataset"
PARA_SAMPLING_RATE = "sample_rate"
PARA_MODEL = "model"
PARA_COLUMNS = "columns"
PARA_LABEL_COLUMN = "label_column"

SAMPLING_MODE_UNIFORM_PROBABILITY = 'uniform_probability'

SELECT_TRAINING = "select_training"
SELECT_TESTING = "select_testing"
SELECT_MODEL = "select_model"
SELECT_PREDICTION_COLUMNS = "select_prediction_columns"
SELECT_LABEL_COLUMN = "select_label_column"
SELECT_ACCURACY_METRIC = "select_metric"
CONTEXT_DATABASE_NAME = "context"