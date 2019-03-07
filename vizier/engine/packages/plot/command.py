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

"""Create instances of plot package commands."""

import vizier.engine.packages.base as pckg
import vizier.engine.packages.plot.base as plot
import vizier.viztrail.command as md


def create_plot(
    dataset_name, chart_name, series, chart_type='Bar Chart', chart_grouped=False,
    xaxis_range=None, xaxis_column=None, validate=False
):
    """Create an instance of a create plot command.

    Parameters
    ----------
    dataset_name: string
        Dataset name
    chart_name: string
        Name of the chart
    series: list
        Specification of data series. Each data series is specified by a
        dictionary that contains the mandadtory element 'column' and
        the optional elements 'range' and 'label'
    chart_type: string
        Identifier for chart type
    chart_grouped: bool
        Group multiple series into a single chart
    xaxis_range: string, optional
        Column value range definition
    xaxis_column: int, optional
        Column identifier
    validate: bool, optional
        If true, the command is validated

    Returns
    -------
    vizier.engine.viztrail.command.ModuleCommand
    """
    # If the validate flag is true create a package index that contains the
    # plot package declaration
    if validate:
        packages = {plot.PACKAGE_PLOT: pckg.PackageIndex(plot.PLOT_COMMANDS)}
    else:
        packages = None
    # Create a record for each series specification
    series_elements = list()
    for s in series:
        items = list()
        items.append(md.ARG(id=plot.PARA_SERIES_COLUMN, value=s['column']))
        if 'label' in s:
            items.append(md.ARG(id=plot.PARA_SERIES_LABEL, value=s['label']))
        if 'range' in s:
            items.append(md.ARG(id=plot.PARA_SERIES_RANGE, value=s['range']))
        series_elements.append(items)
    # Create list of arguments
    arguments= [
        md.ARG(id=pckg.PARA_DATASET, value=dataset_name),
        md.ARG(id=pckg.PARA_NAME, value=chart_name),
        md.ARG(id=plot.PARA_SERIES, value=series_elements),
        md.ARG(
            id=plot.PARA_CHART,
            value=[
                md.ARG(plot.PARA_CHART_TYPE, value=chart_type),
                md.ARG(id=plot.PARA_CHART_GROUPED, value=chart_grouped)
            ]
        )
    ]
    # Only add xaxis record if at least one of the two arguments are given
    if not xaxis_range is None or not xaxis_column is None:
        items = list()
        if not xaxis_column is None:
            items.append(md.ARG(id=plot.PARA_XAXIS_COLUMN, value=xaxis_column))
        if not xaxis_range is None:
            items.append(md.ARG(id=plot.PARA_XAXIS_RANGE, value=xaxis_range))
        arguments.append(md.ARG(id=plot.PARA_XAXIS, value=items))
    return md.ModuleCommand(
        package_id=plot.PACKAGE_PLOT,
        command_id=plot.PLOT_SIMPLE_CHART,
        arguments=arguments,
        packages=packages
    )
