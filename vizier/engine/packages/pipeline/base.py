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
PARA_LOSS_FUNCTION = "loss_function"
PARA_COLUMNS = "columns"
PARA_LABEL_COLUMN = "label_column"
PARA_CONFUSION_METRIC = "confusion_metric"
PARA_CONFUSION_ATTRIBUTE = "confusion_attribute"

SAMPLING_MODE_UNIFORM_PROBABILITY = 'uniform_probability'

SELECT_TRAINING = "select_training"
SELECT_TESTING = "select_testing"
SELECT_MODEL = "select_model"
SELECT_PREDICTION_COLUMNS = "select_prediction_columns"
SELECT_LABEL_COLUMN = "select_label_column"
COMPUTE_CONFUSION = "compute_confusion"
SELECT_ACCURACY_METRIC = "select_metric"
CREATE_CONTEXT = "create_context"
CONTEXT_DATABASE_NAME = "context"

TRAINING_SUFFIX = "_training"
TESTING_SUFFIX = "_testing"

CATEGORICAL_THRESHOLD = 50

TP = "tp"
FP = "fp"
TN = "tn"
FN = "fn"

from sklearn.linear_model import SGDClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.tree import DecisionTreeClassifier

LinearClassifier = SGDClassifier

model_mapping = {
    "Linear Classifier": LinearClassifier,
    "Random Forest Classifier": RandomForestClassifier,
    "Neural Network": MLPClassifier,
    "Decision Tree Classifier": DecisionTreeClassifier
}

loss_function_mapping = {
    "Hinge": "hinge",
    "Log Loss": "log",
    "Perceptron": "perceptron",
    "Squared Loss": "squared_loss",
    "Gini Impurity": "gini",
    "Entropy": "entropy",
    "Stochastic Gradient Descent": "sgd",
    "L-BFGS": "lbfgs",
    "Adam": "adam"
}