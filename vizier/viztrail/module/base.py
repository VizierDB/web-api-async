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

"""A workflow is a sequence of modules. Each module is represented by a module
handle. The handle maintains information about the module command, status, the
module outputs, and the module state (datasets).
"""

from abc import abstractmethod

from vizier.viztrail.module.output import ModuleOutputs
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp


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


class ModuleState(object):
    """Object representing the module (and workflow) state. Implements boolean
    properties to test the current state.
    """
    def __init__(self, state):
        """Set the state value. Raises ValueError if given state is not a valid
        value (one of PENDING, RUNNING, CANCELED, ERROR, SUCCESS).

        Parameters
        ----------
        state: int
            Module state value.
        """
        # Raise ValueError if state is not valid
        if not state in MODULE_STATE:
            raise ValueError('invalid module state \'' + str(state) + '\'')
        self.state = state

    @property
    def is_active(self):
        """True if either pending or running.

        Returns
        -------
        bool
        """
        return self.is_pending or self.is_running

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
    def is_stopped(self):
        """True, if either canceled or in error state.

        Returns
        -------
        bool
        """
        return self.is_canceled or self.is_error

    @property
    def is_success(self):
        """True, if module is in SUCCESS state.

        Returns
        -------
        bool
        """
        return self.state == MODULE_SUCCESS


class ModuleHandle(ModuleState):
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
    identifier: string, optional
        Unique module identifier
    command: vizier.viztrail.command.ModuleCommand
        Specification of the module (i.e., package, name, and arguments)
    datasets: dict(vizier.datastore.dataset.DatasetDescriptor)
        Dictionary of resulting datasets. The user-specified name is the key.
    external_form: string
        Printable representation of the module command
    outputs: vizier.viztrail.module.output.ModuleOutputs
        Module output streams STDOUT and STDERR
    provenance: vizier.viztrail.module.provenance.ModuleProvenance
        Provenance information about datasets that were read and writen by
        previous execution of the module.
    state: int
        Module state (one of PENDING, RUNNING, CANCELED, ERROR, SUCCESS)
    timestamp: vizier.viztrail.module.timestamp.ModuleTimestamp
        Module timestamp
    """
    def __init__(
        self, command, external_form, identifier=None, state=None,
        timestamp=None, datasets=None, outputs=None, provenance=None
    ):
        """Initialize the module handle. For new modules, datasets and outputs
        are initially empty.

        Parameters
        ----------
        command : vizier.viztrail.command.ModuleCommand
            Specification of the module (i.e., package, name, and arguments)
        external_form: string
            Printable representation of module command
        identifier : string, optional
            Unique module identifier
        state: int
            Module state (one of PENDING, RUNNING, CANCELED, ERROR, SUCCESS)
        timestamp: vizier.viztrail.module.timestamp.ModuleTimestamp, optional
            Module timestamp
        datasets : dict(vizier.datastore.dataset.DatasetDescriptor), optional
            Dictionary of resulting datasets. The user-specified name is the key
            and the unique dataset identifier the value.
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Module output streams STDOUT and STDERR
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.
        """
        super(ModuleHandle, self).__init__(
            state=state if not state is None else MODULE_PENDING
        )
        self.identifier = identifier
        self.command = command
        self.external_form = external_form
        self.datasets = datasets if not datasets is None else dict()
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.provenance = provenance if not provenance is None else ModuleProvenance()
        self.timestamp = timestamp if not timestamp is None else ModuleTimestamp()

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
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
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
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
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
        datasets : dict(vizier.datastore.dataset.DatasetDescriptor), optional
            Dictionary of resulting datasets. The user-specified name is the
            key for the dataset descriptors.
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.
        """
        raise NotImplementedError

    @abstractmethod
    def update_property(self, external_form):
        """Update the value for the external command representation

        Parameters
        ----------
        external_form: string, optional
            Adjusted external representation for the module command.
        """
        raise NotImplementedError
