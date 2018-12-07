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

"""Task processor for commands in the plot package."""

from vizier.core.util import is_valid_name
from vizier.engine.packages.plot.query import ChartQuery
from vizier.engine.packages.plot.view import ChartViewHandle
from vizier.engine.packages.processor import ExecResult, TaskProcessor
from vizier.viztrail.module import ModuleOutputs, ModuleProvenance, ChartOutput

import vizier.engine.packages.base as pckg
import vizier.engine.packages.plot.base as cmd


class PlotProcessor(TaskProcessor):
    """Implmentation of the task processor for the plot package."""
    def compute(self, command_id, arguments, context):
        """Compute results for the given plot command using the set of user-
        provided arguments and the current database state. Return an execution
        result is case of success or error.

        At this point there is only one plot command in the package.

        Parameters
        ----------
        command_id: string
            Unique identifier for a command in a package declaration
        arguments: vizier.viztrail.command.ModuleArguments
            User-provided command arguments
        context: vizier.engine.packages.processor.TaskContext
            Context in which a task is being executed

        Returns
        -------
        vizier.engine.packages.processor.ExecResult
        """
        if command_id == cmd.PLOT_SIMPLE_CHART:
            return self.compute_simple_chart(
                args=arguments,
                context=context
            )
        else:
            raise ValueError('unknown plot command \'' + str(command_id) + '\'')

    def compute_simple_chart(self, args, context):
        """Execute simple chart command.

        Parameters
        ----------
        args: vizier.viztrail.command.ModuleArguments
            User-provided command arguments
        context: vizier.engine.packages.processor.TaskContext
            Context in which a task is being executed

        Returns
        -------
        vizier.engine.packages.processor.ExecResult
        """
        # Get dataset name and the associated dataset. This will raise an
        # exception if the dataset name is unknown.
        ds_name = args.get_value(pckg.PARA_DATASET)
        ds = context.get_dataset(ds_name)
        # Get user-provided name for the new chart and verify that it is a
        # valid name
        chart_name = args.get_value(pckg.PARA_NAME)
        if not is_valid_name(chart_name):
            raise ValueError('invalid chart name \'' + str(chart_name) + '\'')
        chart_args = args.get_value(cmd.PARA_CHART)
        chart_type = chart_args.get_value(cmd.PARA_CHART_TYPE)
        grouped_chart = chart_args.get_value(cmd.PARA_CHART_GROUPED)
        # Create a new chart view handle and add the series definitions
        view = ChartViewHandle(
            dataset_name=ds_name,
            chart_name=chart_name,
            chart_type=chart_type,
            grouped_chart=grouped_chart
        )
        # The data series index for x-axis values is optional
        if args.has(cmd.PARA_XAXIS):
            x_axis = args.get_value(cmd.PARA_XAXIS)
            # X-Axis column may be empty. In that case, we ignore the
            # x-axis spec
            col_id = x_axis.get_value(cmd.PARA_XAXIS_COLUMN)
            add_data_series(
                args=x_axis,
                view=view,
                dataset=ds
            )
            view.x_axis = 0
        # Definition of data series. Each series is a pair of column
        # identifier and a printable label.
        for data_series in args.get_value(cmd.PARA_SERIES):
            add_data_series(
                args=data_series,
                view=view,
                dataset=ds,
                default_label='Series ' + str(len(view.data) + 1)
            )
        # Execute the query and get the result
        rows = ChartQuery().exec_query(ds, view)
        # Add chart view handle as module output
        return ExecResult(
            datasets=context.datasets,
            outputs=ModuleOutputs(stdout=[ChartOutput(view=view, rows=rows)]),
            provenance=ModuleProvenance(
                read={ds_name: ds.identifier},
                write=dict()
            )
        )


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def add_data_series(args, view, dataset, default_label=None):
    """Add a data series handle to a given chart view handle. Expects a data
    series specification and a dataset descriptor.

    Parameters
    ----------
    args: vizier.viztrail.command.ModuleArguments
        User-provided command line arguments for the series object
    view: vizier.plot.view.ChartViewHandle
        Chart view handle
    dataset: vizier.datastore.base.DatasetHandle
        Dataset handle
    default_label: string, optional
        Default label for dataseries if not given in the specification.
    """
    col_id = args.get_value(cmd.PARA_SERIES_COLUMN)
    # Get column index to ensure that the column exists. Will raise
    # an exception if c_name does not specify a valid column.
    c_name = dataset.column_by_id(col_id).name
    if args.has(cmd.PARA_SERIES_LABEL):
        s_label = args.get_value(cmd.PARA_SERIES_LABEL)
        if s_label.strip() == '':
            s_label = default_label if not default_label is None else c_name
    else:
        s_label = default_label if not default_label is None else c_name
    # Check for range specifications. Expect string of format int or
    # int:int with the second value being greater or equal than
    # the first.
    range_start = None
    range_end = None
    if args.has(cmd.PARA_SERIES_RANGE):
        s_range = args.get_value(cmd.PARA_SERIES_RANGE).strip()
        if s_range != '':
            pos = s_range.find(':')
            if pos > 0:
                range_start = int(s_range[:pos])
                range_end = int(s_range[pos+1:])
                if range_start > range_end:
                    raise ValueError('invalid range \'' + s_range + '\'')
            else:
                range_start = int(s_range)
                range_end = range_start
            if range_start < 0 or range_end < 0:
                raise ValueError('invalid range \'' + s_range + '\'')
    view.add_series(
        column=col_id,
        label=s_label,
        range_start=range_start,
        range_end=range_end
    )
