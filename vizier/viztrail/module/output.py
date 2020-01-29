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

"""The module output consist of two streams of output object: one for the
standard output and one for error messages.
"""

import sys
import os
import traceback
import vizier.config.base as config
from vizier.mimir import MimirError
from requests.exceptions import ConnectionError
import itertools

"""Predefined output types."""
OUTPUT_CHART = 'chart/view'
OUTPUT_TEXT = 'text/plain'
OUTPUT_HTML = 'text/html'
OUTPUT_MARKDOWN = 'text/markdown'
OUTPUT_DATASET = 'dataset/view'

def debug_is_on():
    return str(os.environ.get('VIZIERSERVER_DEBUG', "False")) == "True"

def format_stack_trace(ex):
    trace = traceback.extract_tb(ex.__traceback__, limit = 30)
    # print("{}".format(trace))
    if not debug_is_on():
        trace = itertools.dropwhile(lambda x: x[0] != "<string>", trace)
    trace = list([
        "{} {} line {}{}".format(
            # Function Name
            "<Python Cell>" if element[2] == "<module>" else element[2]+"(...)", 

            # File
            "on" if element[0] == "<string>" else "in "+element[0]+", ", 

            # Line #
            element[1],

            # Line Content
            "\n    "+element[3] if element[3] != "" else ""  
        )
        for element in trace
    ])
    if len(trace) > 0:
        trace.reverse()
        trace = (
            ["  ... caused by "+trace[0]]+
            ["  ... called by "+line for line in trace[1:]]
        )
    else:
        return "INTERNAL ERROR\n{}".format(ex)
    return "{}".format("\n".join(trace))


class ModuleOutputs(object):
    """Wrapper for module outputs. Contains the standard output and to standard
    error streams. Each stream is a list of output objects.

    Attributes
    ----------
    stderr: list(vizier.viztrail.module.OutputObject)
        Standard error output stream
    stdout: list(vizier.viztrail.module.OutputObject)
        Standard output stream
    """
    def __init__(self, stdout=None, stderr=None):
        """Initialize the standard output and error stream.

        Parameters
        ----------
        stderr: list(vizier.viztrail.module.OutputObject)
            Standard error output stream
        stdout: list(vizier.viztrail.module.OutputObject)
            Standard output stream
        """
        self.stdout = stdout if not stdout is None else list()
        self.stderr = stderr if not stderr is None else list()


    def error(self, ex):
        """Add stack trace for execution error to STDERR stream of the output
        object.

        Parameters
        ----------
        ex: Exception
            Exception that was raised during mudule execution

        Returns
        -------
        vizier.viztrail.module.output.ModuleOutputs
        """
        message = "ERROR: NO ERROR MESSAGE"
        template = "{0}:{1!r}"
        try:
            if type(ex) is MimirError:
                # message = "MIMIR ERROR"
                err_data = ex.args[0]
                message = err_data.get('errorMessage', "An unknown internal error")
                # if "errorType" in err_data:
                #     message = err_data["errorType"] + ": " + message
                if debug_is_on():
                    message = message + "\nDEBUG IS ON"
                    if "stackTrace" in err_data:
                        message = "{}\n{}\n--------".format(message, err_data["stackTrace"])
                    message = "{}\n{}".format(message, format_stack_trace(ex)) 
            elif type(ex) is ConnectionError:
                message = "Couldn't connect to Mimir (Vizier's dataflow layer).  Make sure it's running."
            elif type(ex) is SyntaxError:
                context, line, pos, content = ex.args[1]
                message = "Syntax error (line {}:{})\n{}{}^-- {}".format(
                              line, pos, 
                              content,
                              " " * pos,
                              ex.args[0]
                            )
            elif type(ex) is NameError:
                message = "{}\n{}".format(
                                ex.args[0],
                                format_stack_trace(ex)
                            )
            else:
                message = template.format(type(ex).__name__, ex.args)
                if debug_is_on():
                    message = message + ":\n " + str(traceback.format_exc())
        except Exception as e:
            message = template.format(type(e).__name__, e.args)
            if debug_is_on():
                message += "\n"+format_stack_trace(e)

        self.stderr.append(TextOutput(message))
        return self


class OutputObject(object):
    """An object in an output stream has two components: an object type and a
    type-specific value.

    Attributes
    ----------
    type: string
        Unique object type identifier
    value: any
        Type-specific value
    """
    def __init__(self, type, value):
        """Initialize the object components.

        Parameters
        ----------
        type: string
            Unique object type identifier
        value: any
            Type-specific value
        """
        self.type = type
        self.value = value

    @property
    def is_text(self):
        """True if the type of the output object is text.

        Returns
        -------
        bool
        """
        return self.type == OUTPUT_TEXT


class DatasetOutput(OutputObject):
    """Output object where the value is a dataset handle."""
    def __init__(self, value):
        """Initialize the output dataset.

        Parameters
        ----------
        value, DatasetHandle
            Output dataset
        """
        super(DatasetOutput, self).__init__(type=OUTPUT_DATASET, value=value)


class ChartOutput(OutputObject):
    """Output object where the value is a string."""
    def __init__(self, view, rows):
        """Initialize the output object.

        Parameters
        ----------
        view: vizier.view.chart.ChartViewHandle
            Handle defining the dataset chart view
        rows: list
            List of rows in the query result
        """
        super(ChartOutput, self).__init__(
            type=OUTPUT_CHART,
            value={
                'data': view.to_dict(),
                'result': CHART_VIEW_DATA(view=view, rows=rows)
            }
        )


class HtmlOutput(OutputObject):
    """Output object where the value is a Html string."""
    def __init__(self, value):
        """Initialize the output string.

        Parameters
        ----------
        value, string
            Output string
        """
        super(HtmlOutput, self).__init__(type=OUTPUT_HTML, value=value)


class MarkdownOutput(OutputObject):
    """Output object where the value is a Markdown string."""
    def __init__(self, value):
        """Initialize the output string.

        Parameters
        ----------
        value, string
            Output string
        """
        super(MarkdownOutput, self).__init__(type=OUTPUT_MARKDOWN, value=value)


class TextOutput(OutputObject):
    """Output object where the value is a string."""
    def __init__(self, value):
        """Initialize the output string.

        Parameters
        ----------
        value, string
            Output string
        """
        super(TextOutput, self).__init__(type=OUTPUT_TEXT, value=value)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def CHART_VIEW_DATA(view, rows):
    """Create a dictionary serialization of daraset chart view results. The
    output is a dictionary with the following format (the xAxis element is
    optional):

    {
        "series": [{
            "label": "string",
            "data": [0]
        }],
        "xAxis": {
            "data": [0]
        }
    }

    Parameters
    ----------
    view: vizier.plot.view.ChartViewHandle
        Handle defining the dataset chart view
    rows: list
        List of rows in the query result

    Returns
    -------
    dict
    """
    obj = dict()
    # Add chart type information
    obj['chart'] = {
        'type': view.chart_type,
        'grouped': view.grouped_chart
    }
    # Create a list of series indexes. Then remove the series that contains the
    # x-axis labels (if given). Keep x-axis data in a separate list
    series = list(range(len(view.data)))
    if not view.x_axis is None:
        obj['xAxis'] = {'data': [row[view.x_axis] for row in rows]}
        del series[view.x_axis]
    obj['series'] = list()
    for s_idx in series:
        obj['series'].append({
            'label': view.data[s_idx].label,
            'data': [row[s_idx] for row in rows]
        })
    return obj
