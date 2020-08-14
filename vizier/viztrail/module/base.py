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

from typing import Optional
from datetime import datetime

from vizier.core.timestamp import get_current_time
from vizier.viztrail.module.output import ModuleOutputs
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp
from vizier.viztrail.command import ModuleCommand, ModuleArguments

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
    def __init__(self, state:int):
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
    def __init__(self, 
            command: ModuleCommand, 
            external_form: Optional[str], 
            identifier: Optional[str] = None, 
            state: int = MODULE_PENDING,
            timestamp: ModuleTimestamp = ModuleTimestamp(), 
            outputs: ModuleOutputs = ModuleOutputs(), 
            provenance: ModuleProvenance = ModuleProvenance()
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
        self.outputs = outputs if not outputs is None else ModuleOutputs()
        self.provenance = provenance if not provenance is None else ModuleProvenance()
        self.timestamp = timestamp if not timestamp is None else ModuleTimestamp()

    @property
    def artifacts(self):
        if self.provenance is None: 
            return []
        if self.provenance.write is None:
            return {}
        return [ self.provenance.write[k] for k in self.provenance.write.keys() ]
    
    @property
    def state_string(self):
        if self.state ==   MODULE_PENDING:
            return "PENDING"
        elif self.state == MODULE_RUNNING:
            return "RUNNING"
        elif self.state == MODULE_CANCELED:
            return "CANCELED"
        elif self.state == MODULE_ERROR:
            return "ERROR"
        elif self.state == MODULE_SUCCESS:
            return "SUCCESS"
        else:
            return "UNKNOWN ({})".format(self.state)
    
    def __repr__(self):
        return "{}.{} @ {} is {}\n{}\n{}".format(
            self.command.package_id,
            self.command.command_id,
            self.identifier,
            self.state_string,
            "\n".join(self.command.arguments.to_yaml_lines("  ")),
            self.outputs
        )+("\n"+str(self.outputs) if self.outputs is not None else "")

    def set_canceled(self, 
            finished_at: datetime = get_current_time(), 
            outputs: ModuleOutputs = ModuleOutputs()
        ) -> None:
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
        self.state = MODULE_CANCELED
        self.timestamp.finished_at = finished_at
        self.outputs = outputs

    def set_error(self, 
            finished_at: datetime = get_current_time(), 
            outputs: ModuleOutputs = ModuleOutputs()
        ) -> None:
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
        self.state = MODULE_ERROR
        self.timestamp.finished_at = finished_at
        self.outputs = outputs

    def set_running(self, 
            started_at: datetime = get_current_time(), 
            external_form: Optional[str] = None
        ) -> None:
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
        if external_form is not None:
            self.external_form = external_form
        self.state = MODULE_RUNNING
        self.timestamp.started_at = started_at
        self.outputs = ModuleOutputs()

    def set_success(self, 
            finished_at: datetime = get_current_time(), 
            outputs: ModuleOutputs = ModuleOutputs(), 
            provenance: ModuleProvenance = ModuleProvenance(),
            updated_arguments: Optional[ModuleArguments] = None
        ):
        """Set status of the module to success. The finished_at property of the
        timestamp is set to the given value or the current time (if None).

        If case of a successful module execution the database state and module
        provenance information are also adjusted together with the module
        output streams.

        Parameters
        ----------
        finished_at: datetime.datetime, optional
            Timestamp when module started running
        outputs: vizier.viztrail.module.output.ModuleOutputs, optional
            Output streams for module
        provenance: vizier.viztrail.module.provenance.ModuleProvenance, optional
            Provenance information about datasets that were read and writen by
            previous execution of the module.
        """
        # Update state, timestamp, database state, outputs and provenance
        # information.
        self.state = MODULE_SUCCESS
        self.timestamp.finished_at = finished_at
        # If the module is set to success straight from pending state the
        # started_at timestamp may not have been set.
        if self.timestamp.started_at is None:
            self.timestamp.started_at = self.timestamp.finished_at
        if updated_arguments is not None:
            self.command.arguments = updated_arguments
        self.outputs = outputs
        self.provenance = provenance

    def update_property(self, 
            external_form: Optional[str] = None
        ) -> None:
        """Update the value for the external command representation

        Parameters
        ----------
        external_form: string, optional
            Adjusted external representation for the module command.
        """
        self.external_form = external_form
