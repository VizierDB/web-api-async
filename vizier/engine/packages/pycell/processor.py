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
import requests
import os

from vizier.datastore.dataset import DatasetDescriptor
from vizier.engine.task.processor import ExecResult, TaskProcessor
from vizier.engine.packages.pycell.client.base import VizierDBClient
from vizier.engine.packages.pycell.plugins import python_cell_preload
from vizier.engine.packages.stream import OutputStream
from vizier.viztrail.module.output import ModuleOutputs, OutputObject, HtmlOutput, TextOutput, OUTPUT_TEXT
from vizier.viztrail.module.provenance import ModuleProvenance
from os.path import normpath, basename
from os import path
from vizier.datastore.artifact import ArtifactDescriptor, ARTIFACT_TYPE_PYTHON

import vizier.engine.packages.base as pckg
import vizier.engine.packages.pycell.base as cmd

"""Context variable name for Vizier DB Client."""
VARS_DBCLIENT = 'vizierdb'

SANDBOX_PYTHON_EXECUTION = os.environ.get('SANDBOX_PYTHON_EXECUTION', "False")
SANDBOX_PYTHON_URL = os.environ.get('SANDBOX_PYTHON_URL', 'http://127.0.0.1:5005/')

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
        

        # Get Python script from user arguments.  It is the source for VizierDBClient
        cell_src = args.get_value(cmd.PYTHON_SOURCE)

        # prepend python objects exported in previous cells to the source
        injected_source = "\n".join(
            context.datastore.get_object(descriptor.identifier).decode()
            for name, descriptor in context.dataobjects.items()
            if descriptor.artifact_type == ARTIFACT_TYPE_PYTHON
        )
        source = injected_source + '\n' + cell_src

        # Initialize the scope variables that are available to the executed
        # Python script. At this point this includes only the client to access
        # and manipulate datasets in the undelying datastore
        client = VizierDBClient(
            datastore=context.datastore,
            datasets=context.datasets,
            source=cell_src,
            dataobjects=context.dataobjects,
            project_id=context.project_id,
            output_format=args.get_value(cmd.OUTPUT_FORMAT, default_value = OUTPUT_TEXT)
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
        resdata = None
        # Run the Python code
        try:
            python_cell_preload(variables, client = client)
            if SANDBOX_PYTHON_EXECUTION == "True":
                json_data = {'source':source, 
                             'datasets':context.datasets, 
                             'dataobjects':context.dataobjects, 
                             'datastore':context.datastore.__class__.__name__, 
                             'basepath':context.datastore.base_path,
                             'project_id':context.project_id,
                             'output_format':client.output_format}
                res = requests.post(SANDBOX_PYTHON_URL,json=json_data)
                resdata = res.json()
                client = DotDict()
                for key, value in resdata['provenance'].items():
                    client.setattr(key,value)
                client.setattr('descriptors',{})
                client.setattr('datastore',context.datastore)
                client.setattr('datasets',resdata['datasets'])
                client.setattr('dataobjects',resdata['dataobjects'])
                client.setattr('output_format',resdata['output_format'])
                client.setattr('stdout',
                    [
                        OutputObject(type = item['type'], value = item['value'])
                        for item in resdata.fetch('explicit_stdout', [])
                    ])
                
            else:
                exec(source, variables, variables)
        except Exception as ex:
            exception = ex
        finally:
            # Make sure to reverse redirection of output streams
            sys.stdout = out
            sys.stderr = err
        # Set module outputs
        outputs = ModuleOutputs()
        is_success = (exception is None)
        if SANDBOX_PYTHON_EXECUTION == "True":
            for text in resdata['stdout']:
                outputs.stdout.append(OutputObject(value = text, type = client.output_format))
            for text in resdata['stderr']:
                outputs.stderr.append(TextOutput(text))
                is_success = False        
        else:
            for tag, text in stream:
                text = ''.join(text).strip()
                if tag == 'out':
                    outputs.stdout.append(OutputObject(value = text, type = client.output_format))
                else:
                    outputs.stderr.append(TextOutput(text))
                    is_success = False
        for output in client.stdout:
            outputs.stdout.append(output)

        if is_success:
            # Create provenance information. Ensure that all dictionaries
            # contain elements of expected types, i.e, ensure that the user did
            # not attempt anything tricky.
            read = dict()
            for name in client.read:
                if not isinstance(name, str):
                    raise RuntimeError('invalid key for mapping dictionary')
                if name in context.datasets:
                    read[name] = context.datasets[name].identifier
                    if not isinstance(read[name], str):
                        raise RuntimeError('invalid element in read mapping dictionary: {} (expecting str)'.format(read[name]))
                elif name in context.dataobjects:
                    read[name] = context.dataobjects[name].identifier
                    if not isinstance(read[name], str):
                        raise RuntimeError('invalid element in read mapping dictionary: {} (expecting str)'.format(read[name]))
                else:
                    read[name] = None
            write = dict()
            for name in client.write:
                if not isinstance(name, str):
                    raise RuntimeError('invalid key for mapping dictionary')
                
                if name in client.datasets:
                    write_descriptor = client.datasets[name]
                    if not isinstance(write_descriptor, ArtifactDescriptor):
                        raise RuntimeError('invalid element in write mapping dictionary: {} (expecting str)'.format(name))
                    else:
                        write[name] = write_descriptor
                elif name in client.dataobjects:
                    #wr_id = client.dataobjects[name]
                    write_descriptor = client.dataobjects[name]
                    #write_descriptor = client.datastore.get_object(identifier=wr_id)
                    if not isinstance(write_descriptor, ArtifactDescriptor):
                        raise RuntimeError('invalid element in write mapping dictionary: {} (expecting str)'.format(name))
                    else:
                        write[name] = write_descriptor
                else:
                    write[name] = None
            print("Pycell Execution Finished")
            print("     read: {}".format(read))
            print("     write: {}".format(write))
            provenance = ModuleProvenance(
                read=read,
                write=write,
                delete=client.delete
            )
        else:
            print("ERROR: {}".format(exception))
            outputs.error(exception)
            provenance = ModuleProvenance()
        # Return execution result
        return ExecResult(
            is_success=is_success,
            outputs=outputs,
            provenance=provenance
        )
        
class DotDict(dict):
    def __getattr__(self,val):
        return self[val]
    
    def setattr(self, attr_name, val):
        self[attr_name] = val
