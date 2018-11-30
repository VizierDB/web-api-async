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

"""Implementation for module handles that are maintained as an objects in an
object store.
"""

from vizier.core.io.base import DefaultObjectStore
from vizier.core.timestamp import get_current_time, to_datetime
from vizier.viztrail.command import ModuleCommand
from vizier.viztrail.module import ModuleHandle, ModuleOutputs, ModuleProvenance
from vizier.viztrail.module import ModuleTimestamp, OutputObject, TextOutput
from vizier.viztrail.module import MODULE_CANCELED, MODULE_ERROR, MODULE_PENDING
from vizier.viztrail.module import MODULE_RUNNING, MODULE_SUCCESS


"""Json labels for serialized object."""
KEY_ARGUMENTS = 'args'
KEY_COMMAND = 'command'
KEY_COMMAND_ID = 'commandId'
KEY_CREATED_AT = 'createdAt'
KEY_DATASETS = 'datasets'
KEY_DATASET_ID = 'id'
KEY_DATASET_NAME = 'name'
KEY_EXTERNAL_FORM = 'externalForm'
KEY_FINISHED_AT = 'finishedAt'
KEY_IDENTIFIER = 'id'
KEY_STARTED_AT = 'startedAt'
KEY_OUTPUTS = 'output'
KEY_OUTPUT_TYPE = 'type'
KEY_OUTPUT_VALUE = 'value'
KEY_PACKAGE_ID = 'packageId'
KEY_PROVENANCE = 'prov'
KEY_PROVENANCE_READ = 'read'
KEY_PROVENANCE_WRITE = 'write'
KEY_STATE = 'state'
KEY_STDERR = 'stderr'
KEY_STDOUT = 'stdout'
KEY_TIMESTAMP = 'timestamp'


class OSModuleHandle(ModuleHandle):
    """Handle for workflow modules that are maintained as objects in an object
    store.

    Modules are maintained as Json objects with the following structure:

    - identifier: ...
    - externalForm: ...
    - state: ...
    - command:
      - packageId: ...
      - commandId: ...
      - arguments: [
          - key: ...
          - value: ...
        ]
    - timestamps:
      - createdAt
      - startedAt
      - finishedAt
    - datasets: [
        - id: ...
        - name: ...
      ]
    - outputs
      - stderr: [
          - type: ...
          - value: ...
        ]
      - stdout: [
          - type: ...
          - value: ...
        ]
    - provenance
      - read: [
          - id: ...
          - name: ...
        ]
      - write: [
          - id: ...
          - name: ...
        ]
    """
    def __init__(
        self, identifier, command, external_form, module_path,
        state=None, timestamp=None, datasets=None, outputs=None,
        provenance=None, object_store=None
    ):
        """Initialize the module handle. For new modules, datasets and outputs
        are initially empty.

        Parameters
        ----------
        identifier : string
            Unique module identifier
        command : vizier.viztrail.command.ModuleCommand
            Specification of the module (i.e., package, name, and arguments)
        external_form: string
            Printable representation of module command
        module_path: string
            Path to module resource in object store
        state: int
            Module state (one of PENDING, RUNNING, CANCELED, ERROR, SUCCESS)
        timestamp: vizier.viztrail.module.ModuleTimestamp, optional
            Module timestamp
        datasets : dict(string:string), optional
            Dictionary of resulting datasets. The user-specified name is the key
            and the unique dataset identifier the value.
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Module output streams STDOUT and STDERR
        provenance: vizier.viztrail.module.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources
        """
        super(OSModuleHandle, self).__init__(
            identifier=identifier,
            command=command,
            external_form=external_form,
            state=state if not state is None else MODULE_PENDING,
            timestamp=timestamp,
            datasets=datasets,
            outputs= outputs,
            provenance=provenance
        )
        self.module_path = module_path
        self.object_store = object_store if not object_store is None else DefaultObjectStore()

    @staticmethod
    def create_module(
        command, external_form, state, datasets, outputs, provenance, timestamp,
        modules_folder, object_store=None
    ):
        """
        """
        pass

    @staticmethod
    def load_module(module_path, object_store=None):
        """Load module from given object store.

        Parameters
        ----------
        module_path: string
            Resource path for module object
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources

        Returns
        -------
        vizier.viztrail.driver.objectstore.module.OSModuleHandle
        """
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # Read object from store
        obj = object_store.read_object(object_path=module_path)
        # Create module command
        command = ModuleCommand(
            package_id=obj[KEY_COMMAND][KEY_PACKAGE_ID],
            command_id=obj[KEY_COMMAND][KEY_COMMAND_ID],
            arguments=obj[KEY_COMMAND][KEY_ARGUMENTS]
        )
        # Create module timestamps
        created_at = to_datetime(obj[KEY_TIMESTAMP][KEY_CREATED_AT])
        if KEY_STARTED_AT in obj[KEY_TIMESTAMP]:
            started_at = to_datetime(obj[KEY_TIMESTAMP][KEY_STARTED_AT])
        else:
            started_at = None
        if KEY_FINISHED_AT in obj[KEY_TIMESTAMP]:
            finished_at = to_datetime(obj[KEY_TIMESTAMP][KEY_FINISHED_AT])
        else:
            finished_at = None
        timestamp = ModuleTimestamp(
            created_at=created_at,
            started_at=started_at,
            finished_at=finished_at
        )
        # Create dataset index. In the resulting dictionary datasets are
        # keyed by their name
        datasets = get_dataset_index(obj[KEY_DATASETS])
        # Create module output streams.
        outputs = ModuleOutputs(
            stdout=get_output_stream(obj[KEY_OUTPUTS][KEY_STDOUT]),
            stderr=get_output_stream(obj[KEY_OUTPUTS][KEY_STDERR])
        )
        # Create module provenance information
        read_prov = None
        if KEY_PROVENANCE_READ in obj[KEY_PROVENANCE]:
            read_prov = get_dataset_index(obj[KEY_PROVENANCE][KEY_PROVENANCE_READ])
        write_prov = None
        if KEY_PROVENANCE_WRITE in obj[KEY_PROVENANCE]:
            write_prov = get_dataset_index(obj[KEY_PROVENANCE][KEY_PROVENANCE_WRITE])
        provenance = ModuleProvenance(read=read_prov, write=write_prov)
        # Return module handle
        return OSModuleHandle(
            identifier=obj[KEY_IDENTIFIER],
            command=command,
            external_form=obj[KEY_EXTERNAL_FORM],
            module_path=module_path,
            state=obj[KEY_STATE],
            timestamp=timestamp,
            datasets=datasets,
            outputs=outputs,
            provenance=provenance,
            object_store=object_store
        )

    def set_canceled(self, finished_at=None, outputs=None):
        """Set status of the module to canceled. The finished_at property of the
        timestamp is set to the given value or the current time (if None). The
        module outputs are set to the given value. If no outputs are given the
        module output streams will be empty.

        Parameters
        ----------
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module
        """
        # Update state, timestamp and output information. Clear database state
        # and provenance infotmation.
        self.state = MODULE_CANCELED
        self.timestamp.finished_at = finished_at if not finished_at is None else get_current_time()
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.datasets = dict()
        self.provenance = ModuleProvenance()
        # Materialize module state
        self.write_safe()

    def set_error(self, finished_at=None, outputs=None):
        """Set status of the module to error. The finished_at property of the
        timestamp is set to the given value or the current time (if None). The
        module outputs are adjusted to the given value. the output streams are
        empty if no value is given for the outputs parameter.

        Parameters
        ----------
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module
        """
        # Update state, timestamp and output information. Clear database state
        # and provenance infotmation.
        self.state = MODULE_ERROR
        self.timestamp.finished_at = finished_at if not finished_at is None else get_current_time()
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.datasets = dict()
        self.provenance = ModuleProvenance()
        # Materialize module state
        self.write_safe()

    def set_running(self, started_at=None, external_form=None):
        """Set status of the module to running. The started_at property of the
        timestamp is set to the given value or the current time (if None).

        Parameters
        ----------
        started_at: datetime.datetime, optional
            Timestamp when module started running
        external_form: string, optional
            Adjusted external representation for the module command.
        """
        # Update state and timestamp information. Clear outputs, database state,
        # and provenance infotmation.
        self.state = MODULE_RUNNING
        self.timestamp.started_at = started_at if not started_at is None else get_current_time()
        self.outputs = ModuleOutputs()
        self.datasets = dict()
        self.provenance = ModuleProvenance()
        # Update external command representation if given
        if not external_form is None:
            self.external_form = external_form
        # Materialize module state
        self.write_safe()

    def set_success(self, finished_at=None, datasets=None, outputs=None, provenance=None):
        """Set status of the module to success. The finished_at property of the
        timestamp is set to the given value or the current time (if None).

        If case of a successful module execution the database state and module
        provenance information are also adjusted together with the module
        output streams.

        Parameters
        ----------
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        datasets : dict(string:string), optional
            Dictionary of resulting datasets. The user-specified name is the key
            and the unique dataset identifier the value.
        outputs: vizier.viztrail.module.ModuleOutputs, optional
            Output streams for module
        provenance: vizier.viztrail.module.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.
        """
        # Update state, timestamp, database state, outputs and provenance
        # information.
        self.state = MODULE_SUCCESS
        self.timestamp.finished_at = finished_at if not finished_at is None else get_current_time()
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.datasets = datasets if not datasets is None else dict()
        self.provenance = provenance if not provenance is None else ModuleProvenance()
        # Materialize module state
        self.write_safe()

    def write_module(self):
        """Write current module state to object store.

        Returns
        -------
        vizier.viztrail.driver.objectstore.module.OSModuleHandle
        """
        # Create dictionary serialization for module timestamps
        timestamp = {KEY_CREATED_AT: self.timestamp.created_at.isoformat()}
        if not self.timestamp.started_at is None:
            timestamp[KEY_STARTED_AT] = self.timestamp.started_at.isoformat()
        if not self.timestamp.finished_at is None:
            timestamp[KEY_FINISHED_AT] = self.timestamp.finished_at.isoformat()
        # Create dictionary serialization for module provenance
        prov = {}
        if not self.provenance.read is None:
            prov[KEY_PROVENANCE_READ] = [{
                    KEY_DATASET_NAME: name,
                    KEY_DATASET_ID: self.provenance.read[name]
                } for name in self.provenance.read
            ]
        if not self.provenance.write is None:
            prov[KEY_PROVENANCE_WRITE] = [{
                    KEY_DATASET_NAME: name,
                    KEY_DATASET_ID: self.provenance.write[name]
                } for name in self.provenance.write
            ]
        # Create dictionary serialization for the module handle
        obj = {
            KEY_IDENTIFIER: self.identifier,
            KEY_EXTERNAL_FORM: self.external_form,
            KEY_COMMAND: {
                KEY_PACKAGE_ID: self.command.package_id,
                KEY_COMMAND_ID: self.command.command_id,
                KEY_ARGUMENTS: self.command.arguments.to_list()
            },
            KEY_STATE: self.state,
            KEY_OUTPUTS: {
                KEY_STDERR: [{
                        KEY_OUTPUT_TYPE: obj.type,
                        KEY_OUTPUT_VALUE: obj.value
                    } for obj in self.outputs.stderr],
                KEY_STDOUT: [{
                        KEY_OUTPUT_TYPE: obj.type,
                        KEY_OUTPUT_VALUE: obj.value
                    } for obj in self.outputs.stdout]
            },
            KEY_TIMESTAMP: timestamp,
            KEY_DATASETS: [{
                    KEY_DATASET_NAME: name,
                    KEY_DATASET_ID: self.datasets[name]
                } for name in self.datasets],
            KEY_PROVENANCE: prov
        }
        self.object_store.write_object(
            object_path=self.module_path,
            content=obj
        )
        return self

    def write_safe(self):
        """The write safe method writes the current module state to the object
        store. It catches any occuring exception and sets the module into error
        state if an exception occurs. This method is used to ensure that the
        state of the module is in error (i.e., the workflow cannot further be
        executed) if a state change fails.
        """
        try:
            self.write_module()
        except Exception as ex:
            self.state = MODULE_ERROR
            self.outputs = ModuleOutputs(stderr=[TextOutput(str(ex))])
            self.datasets = dict()


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def get_dataset_index(datasets):
    """Convert a list of dataset object in default serialization format into a
    dictionary where dataset identifier are keyed by dataset names.

    Parameters
    ----------
    datasets: list(dict)
        List of datasets in default serialization format

    Returns
    -------
    dict
    """
    result = dict()
    for ds in datasets:
        result[ds[KEY_DATASET_NAME]] = ds[KEY_DATASET_ID]
    return result


def get_module_path(modules_folder, module_id, object_store):
    """Use a single method to get the module path. This should make it easier to
    change the directory structure for maintaining modules.

    Parameters
    ----------
    modules_folder: string
        path to base folder for module objects
    module_id: string
        Unique module identifier
        object_store: vizier.core.io.base.ObjectStore
            Object store implementation to access and maintain resources


    Returns
    -------
    string
    """
    # At the moment we maintain all module objects as files in a single folder
    return object_store.join(modules_folder, module_id)


def get_output_stream(items):
    """Convert a list of items in an output stream into a list of output
    objects. The element in list items are expected to be in default
    serialization format for output objects.

    Paramaters
    ----------
    items: list(dict)
        Items in the output stream in default serialization format

    Returns
    -------
    list(vizier.viztrail.module.OutputObject)
    """
    result = list()
    for item in items:
        result.append(
            OutputObject(
                type=item[KEY_OUTPUT_TYPE],
                value=item[KEY_OUTPUT_VALUE]
            )
        )
    return result
