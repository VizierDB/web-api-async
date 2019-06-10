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

"""Implementation of the task processor for the Python cell package."""

import sys

from vizier.datastore.dataset import DatasetDescriptor
from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.engine.packages.pycell.client.base import VizierDBClient
from vizier.engine.packages.pycell.plugins import python_cell_preload
from vizier.engine.packages.stream import OutputStream
from vizier.viztrail.module.output import ModuleOutputs, HtmlOutput, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance

import vizier.engine.packages.base as pckg
import vizier.engine.packages.pycell.base as cmd

"""Context variable name for Vizier DB Client."""
VARS_DBCLIENT = 'vizierdb'


class PyCellTaskProcessor(TaskProcessor):
    """Implementation of the task processor for the Python cell package."""
    def compute(self, command_id, arguments, context):
        """Execute the Python script that is contained in the given arguments.

        Parameters
        ----------
        command_id: string
            Unique identifier for a command in a package declaration
        arguments: vizier.viztrail.command.ModuleArguments
            User-provided command arguments
        context: vizier.engine.task.base.TaskContext
            Context in which a task is being executed

        Returns
        -------
        vizier.engine.task.processor.ExecResult
        """
        if command_id == cmd.PYTHON_CODE:
            return self.execute_script(
                args=arguments,
                context=context
            )
        else:
            raise ValueError('unknown pycell command \'' + str(command_id) + '\'')

    def execute_script(self, args, context):
        """Execute a Python script in the given context.

        Parameters
        ----------
        args: vizier.viztrail.command.ModuleArguments
            User-provided command arguments
        context: vizier.engine.task.base.TaskContext
            Context in which a task is being executed

        Returns
        -------
        vizier.engine.task.processor.ExecResult
        """
        # Get Python script from user arguments
        source = args.get_value(cmd.PYTHON_SOURCE)
        # Initialize the scope variables that are available to the executed
        # Python script. At this point this includes only the client to access
        # and manipulate datasets in the undelying datastore
        client = VizierDBClient(
            datastore=context.datastore,
            datasets=context.datasets
        )
        variables = {VARS_DBCLIENT: client}
        # Redirect standard output and standard error streams
        out = sys.stdout
        err = sys.stderr
        stream = list()
        sys.stdout = OutputStream(tag='out', stream=stream)
        sys.stderr = OutputStream(tag='err', stream=stream)
        # Keep track of exception that is thrown by the code
        exception = None
        # Run the Python code
        try:
            python_cell_preload(variables)
            exec source in variables, variables
        except Exception as ex:
            exception = ex
        finally:
            # Make sure to reverse redirection of output streams
            sys.stdout = out
            sys.stderr = err
        # Set module outputs
        outputs = ModuleOutputs()
        is_success = (exception is None)
        for tag, text in stream:
            text = ''.join(text).strip()
            if tag == 'out':
                outputs.stdout.append(HtmlOutput(text))
            else:
                outputs.stderr.append(TextOutput(text))
                is_success = False
        if is_success:
            # Create provenance information. Ensure that all dictionaries
            # contain elements of expected types, i.e, ensure that the user did
            # not attempt anything tricky.
            read = dict()
            for name in client.read:
                if not isinstance(name, basestring):
                    raise RuntimeError('invalid key for mapping dictionary')
                if name in context.datasets:
                    read[name] = context.datasets[name]
                    if not isinstance(read[name], basestring):
                        raise RuntimeError('invalid element in mapping dictionary')
                else:
                    read[name] = None
            write = dict()
            for name in client.write:
                if not isinstance(name, basestring):
                    raise RuntimeError('invalid key for mapping dictionary')
                ds_id = client.datasets[name]
                if not ds_id is None:
                    if not isinstance(ds_id, basestring):
                        raise RuntimeError('invalid value in mapping dictionary')
                    elif ds_id in client.descriptors:
                        write[name] = client.descriptors[ds_id]
                    else:
                        write[name] = client.datastore.get_descriptor(ds_id)
                else:
                    write[name] = None
            provenance = ModuleProvenance(
                read=read,
                write=write,
                delete=client.delete
            )
        else:
            outputs.error(exception)
            provenance = ModuleProvenance()
        # Return execution result
        return ExecResult(
            is_success=is_success,
            outputs=outputs,
            provenance=provenance
        )
