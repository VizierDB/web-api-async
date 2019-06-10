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

"""Specification of parameters for plot cells."""

import vizier.engine.packages.base as pckg


"""Global constants."""

# Package name
PACKAGE_PLOT = 'plot'

# Package-specific command identifier
PLOT_SIMPLE_CHART = 'chart'

# Chart command arguments
PARA_CHART = 'chart'
PARA_CHART_TYPE = 'chartType'
PARA_CHART_GROUPED = 'chartGrouped'
PARA_RANGE = 'range'
PARA_SERIES = 'series'
PARA_SERIES_COLUMN = PARA_SERIES + '_' + pckg.PARA_COLUMN
PARA_SERIES_LABEL = PARA_SERIES + '_label'
PARA_SERIES_RANGE = PARA_SERIES + '_' + PARA_RANGE
PARA_XAXIS = 'xaxis'
PARA_XAXIS_COLUMN = PARA_XAXIS + '_' + pckg.PARA_COLUMN
PARA_XAXIS_RANGE = PARA_XAXIS + '_' + PARA_RANGE


# Declaration of plot package commands
PLOT_COMMANDS = pckg.package_declaration(
    identifier=PACKAGE_PLOT,
    commands=[
        pckg.command_declaration(
            identifier=PLOT_SIMPLE_CHART,
            name='Simple Chart',
            parameters=[
                pckg.para_dataset(0),
                pckg.parameter_declaration(
                    identifier=pckg.PARA_NAME,
                    name='Chart Name',
                    data_type=pckg.DT_STRING,
                    index=1,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_SERIES,
                    name='Data Series',
                    data_type=pckg.DT_LIST,
                    index=2
                ),
                pckg.parameter_declaration(
                    identifier=PARA_SERIES_COLUMN,
                    name='Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=3,
                    parent=PARA_SERIES
                ),
                pckg.parameter_declaration(
                    identifier=PARA_SERIES_RANGE,
                    name='Range',
                    data_type=pckg.DT_STRING,
                    index=4,
                    parent=PARA_SERIES,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_SERIES_LABEL,
                    name='Label',
                    data_type=pckg.DT_STRING,
                    index=5,
                    parent=PARA_SERIES,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_XAXIS,
                    name='X-Axis',
                    data_type=pckg.DT_RECORD,
                    index=6,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_XAXIS_COLUMN,
                    name='Column',
                    data_type=pckg.DT_COLUMN_ID,
                    index=7,
                    parent=PARA_XAXIS,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_XAXIS_RANGE,
                    name='Range',
                    data_type=pckg.DT_STRING,
                    index=8,
                    parent=PARA_XAXIS,
                    required=False
                ),
                pckg.parameter_declaration(
                    identifier=PARA_CHART,
                    name='Chart',
                    data_type=pckg.DT_RECORD,
                    index=9
                ),
                pckg.parameter_declaration(
                    identifier=PARA_CHART_TYPE,
                    name='Type',
                    data_type=pckg.DT_STRING,
                    index=10,
                    values=[
                        pckg.enum_value(value='Area Chart'),
                        pckg.enum_value(value='Bar Chart', is_default=True),
                        pckg.enum_value(value='Line Chart'),
                        pckg.enum_value(value='Scatter Plot')
                    ],
                    parent=PARA_CHART
                ),
                pckg.parameter_declaration(
                    identifier=PARA_CHART_GROUPED,
                    name='Grouped',
                    data_type=pckg.DT_BOOL,
                    index=11,
                    parent=PARA_CHART
                )
            ],
            format=[
                pckg.constant_format('CREATE'),
                pckg.constant_format('PLOT'),
                pckg.variable_format(pckg.PARA_NAME),
                pckg.constant_format('FOR'),
                pckg.variable_format(pckg.PARA_DATASET)
            ]
        )
    ]
)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def export_package(filename, format='YAML'):
    """Write package specification to the given file.

    Parameters
    ----------
    filename: string
        Name of the output file
    format: string, optional
        One of 'YAML' or 'JSON'
    """
    pckg.export_package(
        filename,
        PACKAGE_PLOT,
        PLOT_COMMANDS,
        format=format
    )
