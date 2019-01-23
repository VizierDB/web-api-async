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

"""Implementation for module handles that are maintained as an objects in an
object store.
"""

from vizier.core.io.base import DefaultObjectStore
from vizier.core.timestamp import get_current_time, to_datetime
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor
from vizier.view.chart import ChartViewHandle
from vizier.viztrail.command import ModuleCommand, UNKNOWN_ID
from vizier.viztrail.module.base import ModuleHandle
from vizier.viztrail.module.output import ModuleOutputs, OutputObject, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp

import vizier.viztrail.module.base as mstate


"""Json labels for serialized object."""
KEY_ARGUMENTS = 'args'
KEY_COLUMN_ID = 'id'
KEY_COLUMN_NAME = 'name'
KEY_COLUMN_TYPE = 'type'
KEY_COMMAND = 'command'
KEY_COMMAND_ID = 'commandId'
KEY_CREATED_AT = 'createdAt'
KEY_DATASETS = 'datasets'
KEY_DATASET_COLUMNS = 'columns'
KEY_DATASET_ID = 'id'
KEY_DATASET_NAME = 'name'
KEY_DATASET_ROWCOUNT = 'rowCount'
KEY_EXTERNAL_FORM = 'externalForm'
KEY_FINISHED_AT = 'finishedAt'
KEY_STARTED_AT = 'startedAt'
KEY_OUTPUTS = 'output'
KEY_OUTPUT_TYPE = 'type'
KEY_OUTPUT_VALUE = 'value'
KEY_PACKAGE_ID = 'packageId'
KEY_PROVENANCE = 'prov'
KEY_PROVENANCE_CHARTS = 'charts'
KEY_PROVENANCE_DELETE = 'delete'
KEY_PROVENANCE_READ = 'read'
KEY_PROVENANCE_RESOURCES = 'resources'
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
        - columns: [
          - id: ...
          - name: ...
        ]
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
      - delete: [],
      - resources: {}
      - charts: []
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
        timestamp: vizier.viztrail.module.timestamp.ModuleTimestamp, optional
            Module timestamp
        datasets : dict(vizier.datastore.dataset.DatasetDescriptor), optional
            Dictionary of resulting datasets. Dataset descriptors are keyed by
            the user-specified dataset name.
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Module output streams STDOUT and STDERR
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources
        """
        super(OSModuleHandle, self).__init__(
            identifier=identifier,
            command=command,
            external_form=external_form,
            state=state if not state is None else mstate.MODULE_PENDING,
            timestamp=timestamp,
            datasets=datasets,
            outputs= outputs,
            provenance=provenance
        )
        self.module_path = module_path
        self.object_store = object_store if not object_store is None else DefaultObjectStore()

    @staticmethod
    def create_module(
        command, external_form, state, timestamp, outputs, provenance,
        module_folder, datasets=None, object_store=None
    ):
        """Create a new materialized module instance for the given values.

        Parameters
        ----------
        command : vizier.viztrail.command.ModuleCommand
            Specification of the module (i.e., package, name, and arguments)
        external_form: string
            Printable representation of module command
        state: int
            Module state (one of PENDING, RUNNING, CANCELED, ERROR, SUCCESS)
        timestamp: vizier.viztrail.module.timestamp.ModuleTimestamp
            Module timestamp
        datasets : dict(vizier.datastore.dataset.DatasetDescriptor)
            Dictionary of resulting datasets. Dataset descriptors are keyed by
            the user-specified dataset name.
        outputs: vizier.viztrail.module.output.ModuleOutputs
            Module output streams STDOUT and STDERR
        provenance: vizier.viztrail.module.provenance.ModuleProvenance
            Provenance information about datasets that were read and writen by
            previous execution of the module.
        module_folder: string
            Object store folder containing module resources
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources

        Returns
        -------
        vizier.viztrail.objectstore.module.OSModuleHandle
        """
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # Serialize module components and materialize
        obj = serialize_module(
            command=command,
            external_form=external_form,
            state=state,
            timestamp=timestamp,
            outputs=outputs,
            provenance=provenance
        )
        identifier = object_store.create_object(
            parent_folder=module_folder,
            content=obj
        )
        # Return handle for created module
        return OSModuleHandle(
            identifier=identifier,
            command=command,
            external_form=external_form,
            module_path=object_store.join(module_folder, identifier),
            state=state,
            timestamp=timestamp,
            datasets=datasets if not datasets is None else dict(),
            outputs=outputs,
            provenance=provenance,
            object_store=object_store
        )

    @staticmethod
    def load_module(identifier, module_path, prev_state=None, object_store=None):
        """Load module from given object store.

        Parameters
        ----------
        identifier: string
            Unique module identifier
        module_path: string
            Resource path for module object
        prev_state: dict(string: vizier.datastore.dataset.DatasetDescriptor)
            Dataset descriptors keyed by the user-provided name that exist in
            the database state of the previous moudle (in sequence of occurrence
            in the workflow)
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources

        Returns
        -------
        vizier.viztrail.objectstore.module.OSModuleHandle
        """
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # Read object from store. This may raise a ValueError to indicate that
        # the module does not exists (in a system error condtion). In this
        # case we return a new module that is in error state.
        try:
            obj = object_store.read_object(object_path=module_path)
        except ValueError:
            return OSModuleHandle(
                identifier=identifier,
                command=ModuleCommand(
                    package_id=UNKNOWN_ID,
                    command_id=UNKNOWN_ID
                ),
                external_form='fatal error: object not found',
                module_path=module_path,
                state=mstate.MODULE_ERROR,
                object_store=object_store
            )
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
        # Create module output streams.
        outputs = ModuleOutputs(
            stdout=get_output_stream(obj[KEY_OUTPUTS][KEY_STDOUT]),
            stderr=get_output_stream(obj[KEY_OUTPUTS][KEY_STDERR])
        )
        # Create module provenance information
        read_prov = None
        if KEY_PROVENANCE_READ in obj[KEY_PROVENANCE]:
            read_prov = dict()
            for ds in obj[KEY_PROVENANCE][KEY_PROVENANCE_READ]:
                read_prov[ds[KEY_DATASET_NAME]] = ds[KEY_DATASET_ID]
        write_prov = None
        if KEY_PROVENANCE_WRITE in obj[KEY_PROVENANCE]:
            write_prov = dict()
            for ds in obj[KEY_PROVENANCE][KEY_PROVENANCE_WRITE]:
                descriptor = DatasetDescriptor(
                    identifier=ds[KEY_DATASET_ID],
                    columns=[
                        DatasetColumn(
                            identifier=col[KEY_COLUMN_ID],
                            name=col[KEY_COLUMN_NAME],
                            data_type=col[KEY_COLUMN_TYPE]
                        ) for col in ds[KEY_DATASET_COLUMNS]
                    ],
                    row_count=ds[KEY_DATASET_ROWCOUNT]
                )
                write_prov[ds[KEY_DATASET_NAME]] = descriptor
        delete_prov = None
        if KEY_PROVENANCE_DELETE in obj[KEY_PROVENANCE]:
            delete_prov = obj[KEY_PROVENANCE][KEY_PROVENANCE_DELETE]
        res_prov = None
        if KEY_PROVENANCE_RESOURCES in obj[KEY_PROVENANCE]:
            res_prov = obj[KEY_PROVENANCE][KEY_PROVENANCE_RESOURCES]
        charts_prov = None
        if KEY_PROVENANCE_CHARTS in obj[KEY_PROVENANCE]:
            charts_prov = [
                ChartViewHandle.from_dict(c)
                    for c in obj[KEY_PROVENANCE][KEY_PROVENANCE_CHARTS]
            ]
        provenance = ModuleProvenance(
            read=read_prov,
            write=write_prov,
            delete=delete_prov,
            resources=res_prov,
            charts=charts_prov
        )
        # Create dictionary of dataset descriptors only if previous state is
        # given and the module is in SUCCESS state. Otherwise, the database
        # state is empty.
        if obj[KEY_STATE] == mstate.MODULE_SUCCESS and not prev_state is None:
            datasets = provenance.get_database_state(prev_state)
        else:
            datasets = dict()
        # Return module handle
        return OSModuleHandle(
            identifier=identifier,
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
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module
        """
        # Update state, timestamp and output information. Clear database state.
        self.state = mstate.MODULE_CANCELED
        self.timestamp.finished_at = finished_at if not finished_at is None else get_current_time()
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.datasets = dict()
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
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module
        """
        # Update state, timestamp and output information. Clear database state.
        self.state = mstate.MODULE_ERROR
        self.timestamp.finished_at = finished_at if not finished_at is None else get_current_time()
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.datasets = dict()
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
        # Update state and timestamp information. Clear outputs and, database
        # state,
        if not external_form is None:
            self.external_form = external_form
        self.state = mstate.MODULE_RUNNING
        self.timestamp.started_at = started_at if not started_at is None else get_current_time()
        self.outputs = ModuleOutputs()
        self.datasets = dict()
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
        datasets : dict(vizier.datastore.dataset.DatasetDescriptor), optional
            Dictionary of resulting datasets. The user-specified name is the key
            for the dataset descriptor.
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.
        """
        # Update state, timestamp, database state, outputs and provenance
        # information.
        self.state = mstate.MODULE_SUCCESS
        self.timestamp.finished_at = finished_at if not finished_at is None else get_current_time()
        # If the module is set to success straight from pending state the
        # started_at timestamp may not have been set.
        if self.timestamp.started_at is None:
            self.timestamp.started_at = self.timestamp.finished_at
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.datasets = datasets if not datasets is None else dict()
        self.provenance = provenance if not provenance is None else ModuleProvenance()
        # Materialize module state
        self.write_safe()

    def update_property(self, external_form):
        """Update the value for the external command representation

        Parameters
        ----------
        external_form: string, optional
            Adjusted external representation for the module command.
        """
        self.external_form = external_form
        self.write_safe()

    def write_module(self):
        """Write current module state to object store."""
        obj = serialize_module(
            command=self.command,
            external_form=self.external_form,
            state=self.state,
            timestamp=self.timestamp,
            outputs=self.outputs,
            provenance=self.provenance
        )
        self.object_store.write_object(
            object_path=self.module_path,
            content=obj
        )

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
            self.state = mstate.MODULE_ERROR
            self.outputs = ModuleOutputs(stderr=[TextOutput(str(ex))])
            self.datasets = dict()


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

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


def serialize_module(command, external_form, state, timestamp, outputs, provenance):
    """Get dictionary serialization of a module.

    Parameters
    ----------
    command : vizier.viztrail.command.ModuleCommand
        Specification of the module (i.e., package, name, and arguments)
    external_form: string
        Printable representation of module command
    state: int
        Module state (one of PENDING, RUNNING, CANCELED, ERROR, SUCCESS)
    timestamp: vizier.viztrail.module.timestamp.ModuleTimestamp
        Module timestamp
    outputs: vizier.viztrail.module.output.ModuleOutputs
        Module output streams STDOUT and STDERR
    provenance: vizier.viztrail.module.provenance.ModuleProvenance
        Provenance information about datasets that were read and writen by
        previous execution of the module.

    Returns
    -------
    dict
    """
    # Create dictionary serialization for module timestamps
    ts = {KEY_CREATED_AT: timestamp.created_at.isoformat()}
    if not timestamp.started_at is None:
        ts[KEY_STARTED_AT] = timestamp.started_at.isoformat()
    if not timestamp.finished_at is None:
        ts[KEY_FINISHED_AT] = timestamp.finished_at.isoformat()
    # Create dictionary serialization for module provenance
    prov = dict()
    if not provenance.read is None:
        prov[KEY_PROVENANCE_READ] = [{
                KEY_DATASET_NAME: name,
                KEY_DATASET_ID: provenance.read[name]
            } for name in provenance.read
        ]
    if not provenance.write is None:
        prov[KEY_PROVENANCE_WRITE] = list()
        for ds_name in provenance.write:
            ds = provenance.write[ds_name]
            prov[KEY_PROVENANCE_WRITE].append({
                KEY_DATASET_NAME: ds_name,
                KEY_DATASET_ID: ds.identifier,
                KEY_DATASET_COLUMNS: [{
                    KEY_COLUMN_ID: col.identifier,
                    KEY_COLUMN_NAME: col.name,
                    KEY_COLUMN_TYPE: col.data_type
                } for col in ds.columns],
                KEY_DATASET_ROWCOUNT: ds.row_count
            })
    if not provenance.delete is None:
        prov[KEY_PROVENANCE_DELETE] = list(provenance.delete)
    if not provenance.resources is None:
        prov[KEY_PROVENANCE_RESOURCES] = provenance.resources
    if not provenance.charts is None:
        prov[KEY_PROVENANCE_CHARTS] = [c.to_dict() for c in provenance.charts]
    # Create dictionary serialization for the module handle
    return {
        KEY_EXTERNAL_FORM: external_form,
        KEY_COMMAND: {
            KEY_PACKAGE_ID: command.package_id,
            KEY_COMMAND_ID: command.command_id,
            KEY_ARGUMENTS: command.arguments.to_list()
        },
        KEY_STATE: state,
        KEY_OUTPUTS: {
            KEY_STDERR: [{
                    KEY_OUTPUT_TYPE: obj.type,
                    KEY_OUTPUT_VALUE: obj.value
                } for obj in outputs.stderr],
            KEY_STDOUT: [{
                    KEY_OUTPUT_TYPE: obj.type,
                    KEY_OUTPUT_VALUE: obj.value
                } for obj in outputs.stdout]
        },
        KEY_TIMESTAMP: ts,
        KEY_PROVENANCE: prov
    }
