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

"""Implements the executor for commands in the pycell package."""


class OutputStream(object):
    """Output stream for standard output and standard error streams when
    executing Python code in a pycell.
    """
    def __init__(self, tag, stream):
        self.closed = False
        self._tag = tag
        self._stream = stream

    def close(self):
        self.closed = True

    def flush(self):
        pass

    def writelines(self, iterable):
        for text in iterable:
            self.write(text)

    def write(self, text):
        if self._stream and self._stream[-1][0] == self._tag:
            self._stream[-1][1].append(text)
        else:
            self._stream.append((self._tag, [text]))


class PythonCell(object):
    _input_ports = [
        ('source', 'basic:String'),
        ('context', 'basic:Dictionary')
    ]
    _output_ports = [
        ('context', 'basic:Dictionary'),
        ('command', 'basic:String'),
        ('output', 'basic:Dictionary')
    ]

    def compute(self):
        # Get Python source code that is execyted in this cell and the global
        # variables
        source = urllib.unquote(self.get_input('source'))
        context = self.get_input('context')
        # Get module identifier and VizierDB client for current workflow state
        module_id = self.moduleInfo['moduleId']
        vizierdb = get_env(module_id, context)
        # Get Python variables from context and set the current vizier client
        variables = context[ctx.VZRENV_VARS]
        variables[ctx.VZRENV_VARS_DBCLIENT] = vizierdb
        # Redirect standard output and standard error
        out = sys.stdout
        err = sys.stderr
        stream = []
        sys.stdout = OutputStream(tag='out', stream=stream)
        sys.stderr = OutputStream(tag='err', stream=stream)
        # Run the Pyhton code
        try:
            exec source in variables, variables
        except Exception as ex:
            template = "{0}:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            sys.stderr.write(str(message) + '\n')
        finally:
            # Make sure to reverse redirection of output streams
            sys.stdout = out
            sys.stderr = err
        # Propagate potential changes to the dataset mappings
        propagate_changes(module_id, vizierdb.datasets, context)
        # Set module outputs
        outputs = ModuleOutputs()
        for tag, text in stream:
            text = ''.join(text).strip()
            if tag == 'out':
                outputs.stdout(content=PLAIN_TEXT(text))
            else:
                outputs.stderr(content=PLAIN_TEXT(text))
        self.set_output('context', context)
        self.set_output('command', source)
        self.set_output('output', outputs)
