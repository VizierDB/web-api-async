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

"""
Implementation of the task processor that executes commands that
are defined in the `pipeline` package.
"""

import math
import sys
from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.api.webservice import server
from vizier.viztrail.module.output import ModuleOutputs, DatasetOutput, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.datastore.dataset import DATATYPE_VARCHAR, DatasetDescriptor, DatasetColumn, DatasetRow
from vizier.datastore.mimir.dataset import MimirDatasetColumn
import vizier.engine.packages.pipeline.base as cmd
import vizier.mimir as mimir
from sklearn.metrics import accuracy_score, confusion_matrix, multilabel_confusion_matrix
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import numpy as np


class PipelineProcessor(TaskProcessor):
    """
    Implementation of the task processor for the pipeline package.
    """

    def __init__(self):
        pass

    def save_context(self, context, notebook_context, keys, values):
        """
        Save a list of keys and corresponding values in a database for carrying context across cells.
        TODO: Lists and strings as values seem to throw vizier off, using keys to represent both keys 
        and values for now. This should be fixed as soon as lists and strings are supported, the current
        handling is just ugly haha. 
        """

        def save_pair(rows, key: str, value: int):
            """
            Save a specific key-value pair
            """

            is_key_present = False
            for i, row in enumerate(rows):
                if row.values[0] == key:
                    is_key_present = True
                    row[i] = DatasetRow(
                        identifier=notebook_context.max_row_id()+1,
                        values=[key, value]
                    )
                    break
            if not is_key_present:
                rows.append(
                    DatasetRow(
                        identifier=notebook_context.max_row_id()+1,
                        values=[key, value]
                    )
                )

        rows = notebook_context.fetch_rows()

        seen = set()
        for key, value in zip(keys, values):
            if key not in seen:
                save_pair(rows, key, value)
            seen.add(key)

        ds = context.datastore.create_dataset(
            columns=notebook_context.columns,
            rows=rows,
            annotations=notebook_context.annotations
        )

        return ds

    def get_notebook_context(self, context):
        notebook_context = None
        if cmd.CONTEXT_DATABASE_NAME in context.datasets:
            notebook_context = context.get_dataset(cmd.CONTEXT_DATABASE_NAME)
        else:
            notebook_context = context.datastore.create_dataset(
                columns=[
                    MimirDatasetColumn(
                        identifier=0,
                        name_in_dataset='k',
                        data_type=DATATYPE_VARCHAR
                    ),
                    MimirDatasetColumn(
                        identifier=1,
                        name_in_dataset='v',
                        data_type=DATATYPE_VARCHAR
                    )
                ],
                rows=[],
                human_readable_name=cmd.CONTEXT_DATABASE_NAME
            )
        return notebook_context

    def compute(self, command_id, arguments, context):

        outputs = ModuleOutputs()
        provenance = ModuleProvenance(
            read={},
            write={}
        )
        output_ds_name = ""

        notebook_context = self.get_notebook_context(context)

        label_encoder_map = {}

        print(command_id, flush=True)

        if command_id == cmd.SELECT_TRAINING or command_id == cmd.SELECT_TESTING:

            input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()

            input_dataset = context.get_dataset(input_ds_name)
            sample_mode = None

            if input_dataset is None:
                raise ValueError('unknown dataset \'' + input_ds_name + '\'')

            def get_sample_mode(mode, sampling_rate):
                if sampling_rate > 1.0 or sampling_rate < 0.0:
                    raise Exception(
                        "Sampling rate must be between 0.0 and 1.0")
                return {
                    "mode": mode,
                    "probability": sampling_rate
                }

            if command_id == cmd.SELECT_TRAINING:
                output_ds_name = (input_ds_name + cmd.TRAINING_SUFFIX).lower()

            elif command_id == cmd.SELECT_TESTING:
                output_ds_name = (input_ds_name + cmd.TESTING_SUFFIX).lower()

            sample_mode = get_sample_mode(cmd.SAMPLING_MODE_UNIFORM_PROBABILITY, float(
                arguments.get_value(cmd.PARA_SAMPLING_RATE)))

            mimir_table_names = dict()
            for ds_name_o in context.datasets:
                dataset_id = context.datasets[ds_name_o]
                dataset = context.datastore.get_dataset(dataset_id)
                if dataset is None:
                    raise ValueError('unknown dataset \'' + ds_name_o + '\'')
                mimir_table_names[ds_name_o] = dataset.table_name

            view_name, dependencies = mimir.createView(
                mimir_table_names,
                'SELECT * FROM ' + str(input_ds_name)
            )
            sample_view_id = mimir.createSample(
                view_name,
                sample_mode
            )

            row_count = mimir.countRows(sample_view_id)

            # Register the view with Vizier
            ds = context.datastore.register_dataset(
                table_name=sample_view_id,
                columns=input_dataset.columns,
                row_counter=row_count
            )

            ds_output = server.api.datasets.get_dataset(
                project_id=context.project_id,
                dataset_id=ds.identifier,
                offset=0,
                limit=10
            )

            ds_output['name'] = output_ds_name
            outputs.stdout.append(DatasetOutput(ds_output))

            # Record Reads and writes
            provenance.read[input_ds_name] = input_dataset.identifier

            provenance.write[output_ds_name] = DatasetDescriptor(
                identifier=ds.identifier,
                columns=ds.columns,
                row_count=ds.row_count
            )

        elif command_id == cmd.SELECT_MODEL:

            def parse_loss_function(loss_function_argument):
                _, loss_function = loss_function_argument.split(": ")
                return loss_function

            classifier = arguments.get_value(cmd.PARA_MODEL)
            loss_function_argument = arguments.get_value(
                cmd.PARA_LOSS_FUNCTION)
            loss_function = parse_loss_function(loss_function_argument)

            outputs.stdout.append(TextOutput(
                "{} selected for classification. Loss function selected: {}".format(classifier, loss_function)))

            # TODO: change as soon as Vizier supports strings/integers
            # The context is currently saved using prefixes to denote the key followed by
            # the value separated by an underscore :'(

            ds = self.save_context(context, notebook_context, [
                                   "model_" + classifier, "loss_" + loss_function], [0, 0])

            provenance.write[cmd.CONTEXT_DATABASE_NAME] = DatasetDescriptor(
                identifier=ds.identifier,
                columns=ds.columns,
                row_count=ds.row_count
            )

        elif command_id == cmd.SELECT_PREDICTION_COLUMNS:

            input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()
            input_dataset = context.get_dataset(input_ds_name)

            columns = arguments.get_value(cmd.PARA_COLUMNS)
            outputs.stdout.append(TextOutput(
                "Input columns selected for prediction. "))

            # since saving lists was a problem and keys need to be unique,
            # input columns are saved as multiple keys with the prefix 'column_'
            # TODO: fix as soon as lists/strings are supported :'(
            ds = self.save_context(context, notebook_context, [
                                   "column_" + str(column.arguments['columns_column']) for column in columns], [0 for column in columns])

            provenance.read[input_ds_name] = input_dataset.identifier
            provenance.write[cmd.CONTEXT_DATABASE_NAME] = DatasetDescriptor(
                identifier=ds.identifier,
                columns=ds.columns,
                row_count=ds.row_count
            )

        elif command_id == cmd.SELECT_LABEL_COLUMN:

            input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()
            input_dataset = context.get_dataset(input_ds_name)
            label_column = arguments.get_value(cmd.PARA_LABEL_COLUMN)

            outputs.stdout.append(TextOutput(
                "Column {} selected as the label column. ".format(label_column)))

            ds = self.save_context(context, notebook_context, [
                                   "label"], [label_column])

            provenance.read[input_ds_name] = input_dataset.identifier
            provenance.write[cmd.CONTEXT_DATABASE_NAME] = DatasetDescriptor(
                identifier=ds.identifier,
                columns=ds.columns,
                row_count=ds.row_count
            )
        elif command_id == cmd.COMPUTE_CONFUSION:

            input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET)
            input_dataset = context.get_dataset(input_ds_name)
            confusion_metric = arguments.get_value(cmd.PARA_CONFUSION_METRIC)
            confusion_attribute = arguments.get_value(
                cmd.PARA_CONFUSION_ATTRIBUTE)
            notebook_context = self.get_notebook_context(context)

            # Just converting human readable names to shorter strings for saving in the context
            outputs.stdout.append(TextOutput(
                "Confusion Metric selected: " + confusion_metric))

            if confusion_metric == "True Positive":
                confusion_metric = cmd.TP
            elif confusion_metric == "False Positive":
                confusion_metric = cmd.FP
            elif confusion_metric == "True Negative":
                confusion_metric = cmd.TN
            elif confusion_metric == "False Negative":
                confusion_metric = cmd.FN

            ds = self.save_context(context, notebook_context, [
                                   "confusion_metric_" + confusion_metric, "confusion_attribute_" + input_dataset.columns[int(confusion_attribute)].name], [0, 0])

            provenance.write[cmd.CONTEXT_DATABASE_NAME] = DatasetDescriptor(
                identifier=ds.identifier,
                columns=ds.columns,
                row_count=ds.row_count
            )

        elif command_id == cmd.SELECT_ACCURACY_METRIC:

            input_ds_name = arguments.get_value(cmd.PARA_INPUT_DATASET).lower()
            input_dataset = context.get_dataset(input_ds_name)

            training_sample = context.get_dataset(
                input_ds_name + cmd.TRAINING_SUFFIX)
            testing_sample = context.get_dataset(
                input_ds_name + cmd.TESTING_SUFFIX)

            testing_label_pair = {}

            def parse_context(saved_context):
                model, loss_function, input_columns, output_column, confusion_metric, confusion_attribute = '', '', [], '', '', ''
                for k, v in saved_context:
                    if not k:
                        continue
                    elif "model_" in k:
                        model = k.split("model_")[1]
                    elif "loss_" in k:
                        loss_function = k.split("loss_")[1]
                    elif "column_" in k:
                        colnum = k.split("column_")[1]
                        input_columns.append(int(colnum))
                    elif "label" in k:
                        output_column = v
                    elif "confusion_metric_" in k:
                        confusion_metric = k.split("confusion_metric_")[1]
                    elif "confusion_attribute_" in k:
                        confusion_attribute = k.split(
                            "confusion_attribute_")[1]

                return model, loss_function, input_columns, output_column, confusion_metric, confusion_attribute

            saved_context = [
                row.values for row in notebook_context.fetch_rows()]

            model, loss_function, input_columns, output_column, confusion_metric, confusion_attribute = parse_context(
                saved_context)

            input_columns = [input_dataset.column_by_id(
                column).name.lower() for column in input_columns]
            output_column = input_dataset.column_by_id(
                output_column).name.lower()

            try:
                # Convert the user friendly loss function name to the one used in sklearn
                loss_function = cmd.loss_function_mapping[loss_function]

                # Construct the model using the loss function
                if model == "Neural Network":
                    model = cmd.model_mapping[model](solver=loss_function)
                elif model == "Random Forest Classifier" or model == "Decision Tree Classifier":
                    model = cmd.model_mapping[model](criterion=loss_function)
                else:
                    model = cmd.model_mapping[model](loss=loss_function)

            except:
                outputs.stdout.append(TextOutput(
                    "Incorrect model/loss function combination chosen. "))

            training_values = []
            testing_values = []

            for row in training_sample.fetch_rows():
                values = [value for index, value in enumerate(row.values)]
                training_values.append(values)

            for row in testing_sample.fetch_rows():
                values = [value for index, value in enumerate(row.values)]
                testing_values.append(row.values)

            def choose_columns(df, columns, label_column):
                return df[columns], df[label_column]

            def process(df, columns):

                le = LabelEncoder()

                for column in columns:

                    num_unique_columns = df[column].nunique()
                    if num_unique_columns < cmd.CATEGORICAL_THRESHOLD:
                        # Categorize variables
                        df[column] = le.fit_transform(df[column])

                        if confusion_attribute.lower() == column:
                            nonlocal label_encoder_map
                            label_encoder_map = dict(
                                zip(le.transform(le.classes_), le.classes_))

                return df

            df_training = pd.DataFrame(np.array(training_values), columns=[
                                       col.name.lower() for col in training_sample.columns])

            df_training = process(
                df_training, input_columns + [output_column])

            df_training, training_labels = choose_columns(
                df_training, input_columns, output_column)

            df_training = df_training.to_numpy()
            training_labels = training_labels.to_numpy()

            df_testing = pd.DataFrame(np.array(testing_values), columns=[
                                      col.name.lower() for col in testing_sample.columns])
            df_testing = process(
                df_testing, input_columns + [output_column])
            df_testing, testing_labels = choose_columns(
                df_testing, input_columns, output_column)

            if confusion_metric and confusion_attribute:

                attributes = np.unique(df_testing[confusion_attribute.lower()])

                if len(attributes) > 2:
                    raise Exception("Please choose a binary column")

                testing_label_pair = {
                    attribute: [np.array([]), []] for attribute in attributes
                }

                for index, row in df_testing.iterrows():

                    attribute = df_testing[confusion_attribute.lower(
                    )].iloc[index]

                    testing_label_pair[attribute][0] = np.vstack((testing_label_pair[attribute][0], np.array(
                        row))) if testing_label_pair[attribute][0].size > 0 else np.array(row)

            testing_labels = testing_labels.to_numpy()

            if confusion_metric and confusion_attribute:

                for index, row in df_testing.iterrows():

                    attribute = df_testing[confusion_attribute.lower(
                    )].iloc[index]

                    testing_label_pair[attribute][1].append(
                        testing_labels[index])

            df_testing = df_testing.to_numpy()

            try:

                # Train the model using the label column selected on the training dataset without the label column
                model.fit(df_training, training_labels)

                # Predict labels using the testing dataset without the label column
                predictions = model.predict(df_testing)

                confusion_scores = {}

                if confusion_metric and confusion_attribute:

                    confusion_predictions = {
                        label_encoder_map[attribute]: model.predict(testing_label_pair[attribute][0]) for attribute in testing_label_pair.keys()
                    }

                    confusion_testing_labels = {
                        label_encoder_map[attribute]: testing_label_pair[attribute][1] for attribute in attributes
                    }

                confusion_scores = {
                    label_encoder_map[attribute]: confusion_matrix(confusion_testing_labels[label_encoder_map[attribute]], confusion_predictions[label_encoder_map[attribute]]).ravel()[:4] for attribute in attributes
                }

                def get_confusion_stats(original_stats):
                    _sum = sum(original_stats)
                    return [stat/_sum for stat in original_stats]

                confusion_scores = {
                    label_encoder_map[attribute]: get_confusion_stats(confusion_scores[label_encoder_map[attribute]]) for attribute in attributes
                }

                # Use the number of mismatched labels as a measure of the accuracy for the classification task
                score = accuracy_score(testing_labels, predictions)

                outputs.stdout.append(TextOutput(
                    "Accuracy score: {}%\n".format(str(round(score * 100, 2)))))
                for attribute in attributes:
                    stats = "Stats for {}:\n\n".format(
                        label_encoder_map[attribute])
                    for index, metric in enumerate(["True Negatives", "False Negatives", "True Positives", "False Positives"]):
                        stats += (metric + ": " +
                                  str(round(confusion_scores[label_encoder_map[attribute]][index]*100, 2)) + "%\n")
                    stats += "\n"
                    outputs.stdout.append(TextOutput(stats))

            except ValueError as e:
                outputs.stdout.append(TextOutput(
                    "ERROR: Please choose numerical or categorical columns only"))

            finally:
                provenance.read[cmd.CONTEXT_DATABASE_NAME] = self.get_notebook_context(
                    context).identifier
                provenance.read[input_ds_name] = input_dataset.identifier
                provenance.read[input_ds_name +
                                cmd.TRAINING_SUFFIX] = training_sample.identifier
                provenance.read[input_ds_name +
                                cmd.TESTING_SUFFIX] = testing_sample.identifier

        else:
            raise Exception("Unknown pipeline command: {}".format(command_id))

        return ExecResult(
            outputs=outputs,
            provenance=provenance
        )
