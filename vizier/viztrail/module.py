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

"""A workflow is a sequence of modules. Each module is represented by a module
handle. The handle maintains information about the module command, status, the
module outputs, and the module state (datasets).
"""

from abc import abstractmethod

from vizier.core.timestamp import get_current_time


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
    identifier : string
        Unique module identifier
    command : vizier.viztrail.command.ModuleCommand
        Specification of the module (i.e., package, name, and arguments)
    datasets : dict(string)
        Dictionary of resulting datasets. the user-specified name is the key
        and the unique dataset identifier the value.
    external_form: string
        Printable representation of the module command
    outputs : vizier.viztrail.module.ModuleOutputs
        Module output streams STDOUT and STDERR
    prov: vizier.viztrail.module.ModuleProvenance
        Provenance information about datasets that were read and writen by
        previous execution of the module.
    state: int
        Module state (one of PENDING, RUNNING, CANCELED, ERROR, SUCCESS)
    timestamp: vizier.viztrail.module.ModuleTimestamp
        Module timestamp
    """
    def __init__(
        self, identifier, command, external_form, state=MODULE_PENDING,
        timestamp=None, datasets=None, outputs=None, provenance=None
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
        self.provenance = provenance if not provenance is None else ModuleProvenance()

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

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError


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
        """Initialize the datasets that were read and written by a previous
        module execution.

        Parameters
        ----------
        read: dict, optional
            Dictionary of datasets that the module used as input
        write: dict, optional
            Dictionary of datasets that the module modified
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
    """Each module contains three timestamps:created_at, started_at and
    finished_at. The timestamp does not distinguish between the canceled, error,
    and success state. In either case the finished_at timestamp is set when the
    state change occurs.

    The timestamps started_at and finished_at may be None if the module is in
    PENDING state.

    Attributes
    ----------
    created_at: datatime.datetime
        Time when module was first created
    started_at: datatime.datetime
        Time when module execution started
    finished_at: datatime.datetime
        Time when module execution finished (either due to cancel, error or
        success state change)
    """
    def __init__(self, created_at=None, started_at=None, finished_at=None):
        """Initialize the timestamp components. If created_at is None the
        other two timestamps are expected to be None as well. Will raise
        ValueError if created_at is None but one of the other two timestamps
        is not None.

        Parameters
        ----------
        created_at: datatime.datetime
            Time when module was first created
        started_at: datatime.datetime
            Time when module execution started
        finished_at: datatime.datetime
            Time when module execution finished
        """
        # Raise ValueError if created_at is None but one of the other two
        # timestamps is not None
        if created_at is None and not (started_at is None and finished_at is None):
            raise ValueError('invalid timestamp information')
        self.created_at = created_at if not created_at is None else get_current_time()
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
