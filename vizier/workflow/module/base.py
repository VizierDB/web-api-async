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

"""A workflow is a sequence of modules. The module handles that are defined in
the file contain information for each module in a workflow. The handle maintains
information about the module command, status and the module outputs.
"""

"""Constants for possible module states."""
MODULE_PENDING = 0
MODULE_RUNNING = 1
MODULE_CANCELED = 2
MODULE_ERROR = 3
MODULE_SUCCESS = 4
# List of valid module states
MODULE_STATE = [
    MODULE_CANCELED,
    MODULE_ERROR,
    MODULE_PENDING,
    MODULE_RUNNING,
    MODULE_SUCCESS
]


"""Predefined output types."""
OUTPUT_TEXT = 'txt'


class ModuleHandle(object):
    """Handle for a module in a curation workflow. Each module has a unique
    identifier, a specification of the executed command, a module state, a
    timestamp, a list of generated outputs to STDOUT and STDERR, a dictionary of
    resulting datasets and provenance information about datasets that were read
    and written (only for modules that have previously been executed
    successfully).

    If a module is in PENDING state it may contain datasets, outputs, and
    provenance information from a previous run of the module.

    Attributes
    ----------
    identifier : int
        Unique module number (within a worktrail)
    command : vizier.workflow.module.ModuleSpecification
        Specification of the module (i.e., package, name, and arguments)
    datasets : dict(string)
        Dictionary of resulting datasets. the user-specified name is the key
        and the unique dataset identifier the value.
    external_form: string
        Printable representation of the module command
    outputs : vizier.workflow.module.ModuleOutputs
        Module output streams STDOUT and STDERR
    prov: vizier.workflow.module.ModuleProvenance
        Provenance information about datasets that were read and writen by
        previous execution of the module.
    state: int
        Module state (one of PENDING, RUNNING, CANCELED, ERROR, SUCCESS)
    timestamp: vizier.workflow.module.ModuleTimestamp
        Module timestamp
    """
    def __init__(
        self, identifier, command, external_form, state=MODULE_PENDING,
        timestamp=None, datasets=None, outputs=None, prov=None
    ):
        """Initialize the module handle. For new modules, datasets and outputs
        are initially empty.

        Parameters
        ----------
        identifier : int
            Unique module number (within a worktrail)
        command : ModuleSpecification
            Specification of the module (i.e., package, name, and arguments)
        state: int
            Module state (one of PENDING, RUNNING, CANCELED, ERROR, SUCCESS)
        timestamp: vizier.workflow.module.ModuleTimestamp
            Module timestamp
        external_form: string, optional
            Printable representation of module command
        datasets : dict(string:string), optional
            Dictionary of resulting datasets. The user-specified name is the key
            and the unique dataset identifier the value.
        outputs: vizier.workflow.module.ModuleOutputs, optional
            Module output streams STDOUT and STDERR
        prov: vizier.workflow.module.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.
        """
        # Raise ValueError if state is not valid
        if not state in MODULE_STATE:
            raise ValueError('invalid module state \'' + str(state) + '\'')
        self.identifier = identifier
        self.command = command
        self.external_form = external_form
        self.state = state
        self.timestamp = timestamp if not timestamp is None else ModuleTimestamp()
        self.datasets = datasets if not datasets is None else dict()
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.prov = prov if not prov is None else ModuleProvenance()

    def copy_pending(self):
        """Return a copy of the module handle that is in PENDING state.

        Returns
        -------
        vizier.workflow.module.ModuleHandle
        """
        return ModuleHandle(
            identifier=self.identifier,
            command=self.command,
            state=MODULE_PENDING,
            timestamp=ModuleTimestamp(),
            external_form=self.external_form,
            datasets=self.datasets,
            outputs=self.outputs,
            prov=self.prov
        )

    @property
    def is_canceled(self):
        """True, if module is in CANCELED state.

        Returns
        -------
        bool
        """
        return self.state == MODULE_CANCELED

    @property
    def is_error(self):
        """True, if module is in ERROR state.

        Returns
        -------
        bool
        """
        return self.state == MODULE_ERROR

    @property
    def is_pending(self):
        """True, if module is in PENDING state.

        Returns
        -------
        bool
        """
        return self.state == MODULE_PENDING

    @property
    def is_running(self):
        """True, if module is in RUNNING state.

        Returns
        -------
        bool
        """
        return self.state == MODULE_RUNNING

    @property
    def is_success(self):
        """True, if module is in SUCCESS state.

        Returns
        -------
        bool
        """
        return self.state == MODULE_SUCCESS

    def set_canceled(self, finished_at):
        """Update the internal state to canceled. This will also update the
        imestamp.

        Parameters
        ----------
        finished_at: datetime.datetime
            Time when execution was canceled
        """
        self.state = MODULE_CANCELED
        self.timestamp = ModuleTimestamp(
            started_at=self.timestamp.started_at,
            finished_at=finished_at
        )

    def set_error(self, finished_at):
        """Update the internal state to error. This will also update the
        imestamp.

        Parameters
        ----------
        finished_at: datetime.datetime
            Time when execution was stopped
        """
        self.state = MODULE_ERROR
        self.timestamp = ModuleTimestamp(
            started_at=self.timestamp.started_at,
            finished_at=finished_at
        )

    def set_running(self, started_at):
        """Update the internal state to running. This will clear the module
        output streams and update the timestamp.

        Parameters
        ----------
        started_at: datetime.datetime
            Time when execution started
        """
        self.state = MODULE_RUNNING
        self.outputs = ModuleOutputs()
        self.timestamp = ModuleTimestamp(started_at=started_at)

    def set_success(self, datasets, prov, finished_at):
        """Update the internal state to success. This will set the mapping of
         datasets, the provenance information, and update the timestamp.

        Parameters
        ----------
        datasets: dict()
            Dictionary of available dataset identifier after module execution
            (keyed by the dataset name).
        prov: vizier.workflow.module.ModuleProvenance
            Provenance information containing the datasets that were read and
            written during module execution
        finished_at: datetime.datetime
            Time when execution was stopped
        """
        self.state = MODULE_SUCCESS
        self.datasets = datasets
        self.prov = prov
        self.timestamp = ModuleTimestamp(
            started_at=self.timestamp.started_at,
            finished_at=finished_at
        )


class ModuleOutputs(object):
    """Wrapper for module outputs. Contains the standard output and to standard
    error streams. Each stream is a list of output objects.
    """
    def __init__(self, std_out=None, std_err=None):
        """Initialize the standard output and error stream."""
        self.std_out = std_out if not std_out is None else list()
        self.std_err = std_err if not std_err is None else list()

    def stderr(self, content=None):
        """Add content to the error output stream for a workflow module. This
        method is used to update the output stream as well as to retrieve the
        output stream (when called without content parameter).

        Parameters
        ----------
        content: vizier.workflow.module.OutputObject, optional
            Output stream object.

        Returns
        -------
        list
        """
        # Validate content if given. Will raise ValueError if content is invalid
        if not content is None:
            self.std_err.append(content)
        return self.std_err

    def stdout(self, content=None):
        """Add content to the regular output stream for a workflow module.  This
        method is used to update the output stream as well as to retrieve the
        output stream (when called without content parameter).

        Parameters
        ----------
        content: vizier.workflow.module.OutputObject, optional
            Output stream object.

        Returns
        -------
        list
        """
        # Validate content if given. Will raise ValueError if content is invalid
        if not content is None:
            self.std_out.append(content)
        return self.std_out


class ModuleProvenance(object):
    """The module provenance object maintains information about the datasets
    that a module has read and written in previous executions. Provenance
    information is maintained in two dictionaries where the key is the dataset
    name and the value the dataset identifier.

    If provenance information is unknown (e.g., because the module has not been
    executed yet or it is treated as a black box) both dictionaries are None.

    Use method .requires_exec() if a module needs to be executed for a given
    database state.
    """
    def __init__(self, read=None, write=None):
        """
        """
        self.read = read
        self.write = write

    def requires_exec(self, datasets):
        """Test if a module requires execution based on the provenance
        information and a given database state. If True, the module needs to be
        re-executes. Otherwise, the write dependencies can be copied to the new
        database state.

        If either of the read/write dependencies is None we always excute the
        module. Execution can be skipped if all previous inputs are present in
        the database state and if the module at most modifies the datasets that
        are in the set of read dependencies.

        Parameters
        ----------
        datasets: dict()
            Dictionary of identifier for datasets in the current state. The key
            is the dataset name.

        Returns
        -------
        bool
        """
        # Always execute if any of the provenance information for the module is
        # unknown (i.e., None)
        if self.read is None or self.write is None:
            return True
        # Always execute if the module creates or changes a dataset that is not
        # in the read dependencies but that exists in the current state
        for name in self.write:
            if not name in self.read and name in datasets:
                return True
        # Check if all read dependencies are present and have not been modified
        for name in self.read:
            if not name in datasets:
                return True
            elif self.read[name] != datasets[name]:
                return True
        # The database state is the same as for the previous execution of the
        # module (with respect to the input dependencies). Thus, the module
        # does not need to be re-executed.
        return False


class ModuleTimestamp(object):
    """Each module contains two timestamp components: started_at and
    finished_at. The timestamp does not distinguish between the canceled, error,
    and success state. In either case the finished_at timestamp is set when the
    state change occurs.

    Attributes
    ----------
    started_at: datatime.datetime
        Time when module execution started
    finished_at: datatime.datetime
        Time when module execution finished (either due to cancel, error or
        success state change)
    """
    def __init__(self, started_at=None, finished_at=None):
        """Initialize the timestamp components.

        Parameters
        ----------
        started_at: datatime.datetime
            Time when module execution started
        finished_at: datatime.datetime
            Time when module execution finished
        """
        self.started_at = started_at
        self.finished_at = finished_at


# ------------------------------------------------------------------------------
# Output stream objects
# ------------------------------------------------------------------------------

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


class TextObject(OutputObject):
    """Output object where the value is a string."""
    def __init__(self, text):
        super(TextObject, self).__init__(type=OUTPUT_TEXT, value=text)
