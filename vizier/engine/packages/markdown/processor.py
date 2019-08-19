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

"""Implementation of the task processor for the Markdown package."""

import sys

from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.engine.packages.stream import OutputStream
from vizier.viztrail.module.output import ModuleOutputs, HtmlOutput, TextOutput, MarkdownOutput
from vizier.viztrail.module.provenance import ModuleProvenance

import vizier.engine.packages.markdown.base as cmd



class MarkdownTaskProcessor(TaskProcessor):
    """Implementation of the task processor for the markdown package."""
    def compute(self, command_id, arguments, context):
        """Execute the Markdown script that is contained in the given arguments.

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
        if command_id == cmd.MARKDOWN_CODE:
            return self.execute_script(
                args=arguments,
                context=context
            )
        else:
            raise ValueError('unknown markdown command \'' + str(command_id) + '\'')

    def execute_script(self, args, context):
        """Execute a Markdown script in the given context.

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
        # Get Markdown script from user arguments
        source = args.get_value(cmd.PARA_MARKDOWN_SOURCE)
        # Redirect standard output and standard error streams
        out = sys.stdout
        err = sys.stderr
        stream = list()
        sys.stdout = OutputStream(tag='out', stream=stream)
        sys.stderr = OutputStream(tag='err', stream=stream)
        outputs = ModuleOutputs()
        # Run the markdown code
        try:
            #we should validate the markdown here
            ostd = source
            oerr = ''
            if not ostd == '':
                outputs.stdout.append(MarkdownOutput(ostd))
            if not oerr == '':
                outputs.stderr.append(TextOutput(oerr))
        except Exception as ex:
            outputs.error(ex)
        finally:
            # Make sure to reverse redirection of output streams
            sys.stdout = out
            sys.stderr = err
        # Set module outputs
        for tag, text in stream:
            text = ''.join(text).strip()
            if tag == 'out':
                outputs.stdout.append(MarkdownOutput(text))
            else:
                outputs.stderr.append(TextOutput(text))
        provenance = ModuleProvenance()
        # Return execution result
        return ExecResult(
            is_success=(len(outputs.stderr) == 0),
            outputs=outputs,
            provenance=provenance
        )
