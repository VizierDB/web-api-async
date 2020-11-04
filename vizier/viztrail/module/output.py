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

from typing import List, Any, Iterable, Dict, Optional
from vizier.view.chart import ChartViewHandle

import traceback
from vizier.mimir import MimirError
from requests.exceptions import ConnectionError
import itertools
from vizier import debug_is_on
from vizier.datastore.dataset import DatasetDescriptor

"""Predefined output types."""
OUTPUT_CHART = 'chart/view'
OUTPUT_TEXT = 'text/plain'
OUTPUT_HTML = 'text/html'
OUTPUT_MARKDOWN = 'text/markdown'
OUTPUT_DATASET = 'dataset/view'

def format_stack_trace(ex: Exception, offset_lines: int = 0) -> str:
    trace_frames: Iterable[traceback.FrameSummary] = traceback.extract_tb(ex.__traceback__, limit = 30)
    # print("{}".format(trace))
    if not debug_is_on():
        trace_frames = itertools.dropwhile(lambda x: x[0] != "<string>", trace_frames)
    trace_text = list([
        "{} {} line {}{}".format(
            # Function Name
            "<Python Cell>" if element[2] == "<module>" else element[2]+"(...)",

            # File
            "on" if element[0] == "<string>" else "in "+element[0]+", ",

            # Line #
            element[1] + (offset_lines if element[0] == "<string>" else 0),

            # Line Content
            "\n    "+element[3] if element[3] != "" else ""
        )
        for element in trace_frames
    ])
    if len(trace_text) > 0:
        trace_text.reverse()
        trace_text = (
            ["  ... caused by "+trace_text[0]]+
            ["  ... called by "+line for line in trace_text[1:]]
        )
    else:
        return "INTERNAL ERROR\n{}".format(ex)
    return "{}".format("\n".join(trace_text))




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
    def __init__(self, type: str, value: Any):
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

    def __repr__(self):
        if self.is_text:
            return self.value
        else:
            return "< {} >"

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
    def __init__(self, value: Dict[str, Any]):
        """Initialize the output dataset.

        Parameters
        ----------
        value, DatasetHandle
            Output dataset
        """
        super(DatasetOutput, self).__init__(type=OUTPUT_DATASET, value=value)

    @staticmethod
    def from_handle(
            ds: DatasetDescriptor, 
            project_id: str,
            name: Optional[str] = None, 
            raise_error_on_missing: bool = False
        ) -> Optional[OutputObject]:
        from vizier.api.webservice import server
        ds_output = server.api.datasets.get_dataset(
            project_id=project_id,
            dataset_id=ds.identifier,
            offset=0,
            limit=10
        )
        if ds_output is None:
            if raise_error_on_missing:
                raise ValueError("Dataset {} does not exist.".format(ds.identifier if name is None else name))
            else:
                return None
        if name is not None:
            ds_output['name'] = name 
        return DatasetOutput(ds_output)





class ChartOutput(OutputObject):
    """Output object where the value is a string."""
    def __init__(self, 
            view: ChartViewHandle, 
            rows: List[List[Any]], 
            caveats: List[List[bool]]
        ):
        """Initialize the output object.

        Parameters
        ----------
        view: vizier.view.chart.ChartViewHandle
            Handle defining the dataset chart view
        rows: list
            List of rows in the query result
        caveats: list
            one to one of rows.  does each row 
            value have caveats or not
        """
        super(ChartOutput, self).__init__(
            type=OUTPUT_CHART,
            value={
                'data': view.to_dict(), # type: ignore[no-untyped-call]
                'result': CHART_VIEW_DATA(view=view, rows=rows, caveats=caveats) # type: ignore[no-untyped-call]
            }
        )


class HtmlOutput(OutputObject):
    """Output object where the value is a Html string."""
    def __init__(self, value: str):
        """Initialize the output string.

        Parameters
        ----------
        value, string
            Output string
        """
        super(HtmlOutput, self).__init__(type=OUTPUT_HTML, value=value)


class MarkdownOutput(OutputObject):
    """Output object where the value is a Markdown string."""
    def __init__(self, value: str):
        """Initialize the output string.

        Parameters
        ----------
        value, string
            Output string
        """
        super(MarkdownOutput, self).__init__(type=OUTPUT_MARKDOWN, value=value)


class TextOutput(OutputObject):
    """Output object where the value is a string."""
    def __init__(self, value: str):
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

def CHART_VIEW_DATA(view, rows, caveats):
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
    if view.x_axis is not None:
        obj['xAxis'] = {'data': [row[view.x_axis] for row in rows]}
        del series[view.x_axis]
    obj['series'] = list()
    for s_idx in series:
        obj['series'].append({
            'label': view.data[s_idx].label,
            'data': [row[s_idx] for row in rows],
            'caveats': [row_caveats[s_idx] for row_caveats in caveats]
        })
    return obj


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
    def __init__(self, 
            stdout: Optional[List[OutputObject]]=None, 
            stderr: Optional[List[OutputObject]]=None):
        """Initialize the standard output and error stream.

        Parameters
        ----------
        stderr: list(vizier.viztrail.module.OutputObject)
            Standard error output stream
        stdout: list(vizier.viztrail.module.OutputObject)
            Standard output stream
        """
        self.stdout = stdout if stdout is not None else list()
        self.stderr = stderr if stderr is not None else list()

    def __repr__(self) -> str:
        ret: List[str] = []
        if self.stdout is not None and len(self.stdout) > 0:
            ret = ret + ["----- STDOUT ------"] + [ str(output) for output in self.stdout ]
        if self.stderr is not None and len(self.stderr) > 0:
            ret = ret + ["----- STDERR ------"] + [ str(output) for output in self.stderr ]
        return "\n".join(ret)

    def error(self, ex: Exception, offset_lines: int = 0) -> "ModuleOutputs":
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
                    message = "{}\n{}".format(message, format_stack_trace(ex, offset_lines = offset_lines)) 
            elif type(ex) is ConnectionError:
                message = "Couldn't connect to Mimir (Vizier's dataflow layer).  Make sure it's running."
            elif type(ex) is SyntaxError:
                context, line, pos, content = ex.args[1]
                message = "Syntax error (line {}:{})\n{}{}^-- {}".format(
                              line + offset_lines, pos, 
                              content,
                              " " * pos,
                              ex.args[0]
                            )
            elif type(ex) is NameError:
                message = "{}\n{}".format(
                                ex.args[0],
                                format_stack_trace(ex, offset_lines = offset_lines)
                            )
            else:
                message = "{}{}\n{}".format(
                    type(ex).__name__, 
                    ( (": " + "; ".join(str(arg) for arg in ex.args)) if ex.args is not None else "" ), 
                    format_stack_trace(ex, offset_lines = offset_lines)
                )
        except Exception as e:
            message = "{0}:{1!r}".format(type(e).__name__, e.args)
            if debug_is_on():
                message += "\n"+format_stack_trace(e, offset_lines = offset_lines)

        self.stderr.append(TextOutput(message))
        return self