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

"""Abstract class for viztrails. Viztrails are Vizier's adoption of VisTrails.
Defines and implements the base classes for viztrail objects.
"""

from abc import abstractmethod

from vizier.core.timestamp import get_current_time


"""Key's for default viztrail properties."""
# Human readable viztrail name
PROPERTY_NAME = 'name'


class ViztrailDescriptor(object):
    """Descriptor for a viztrail. The descriptor contains the essential
    information of the viztrail. It is primarily intended for viztrail listings
    that do not need access to the full information and functionality of a
    viztrail.

    Each viztrail descriptor contains the unique viztrail identifier, timestamp
    information, and the viztrail properties set.

    A viztrail is expected to have a name property. If the property is missing
    the viztrail name is None.

    Attributes
    ----------
    identifier : string
        Unique viztrail identifier
    created_at : datetime.datetime
        Timestamp of viztrail creation (UTC)
    last_modified_at : datetime.datetime
        Timestamp when viztrail was last modified (UTC)
    name: string
        Human readable viztrail name
    properties: vizier.core.properties.ObjectPropertiesHandler
        Handler for user-defined properties that are associated with this
        viztrail
    """
    def __init__(
        self, identifier, properties, created_at=None, last_modified_at=None
    ):
        """Initialize the viztrail descriptor.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        properties: vizier.core.properties.ObjectPropertiesHandler
            Handler for user-defined properties that are associated with this
            viztrail
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        last_modified_at : datetime.datetime, optional
            Timestamp when project was last modified (UTC)
        """
        self.identifier = identifier
        self.properties = properties
        # If created_at timestamp is None the viztrail is expected to be a newly
        # created viztrail. For new viztrails the last_modified timestamp is
        # expected to be None. For existing viztrails the last_modified
        # timestamp should not be None.
        if not created_at is None:
            if last_modified_at is None:
                raise ValueError('unexpected value for \'last_modified\'')
            self.created_at = created_at
            self.last_modified_at = last_modified_at
        else:
            if not last_modified_at is None:
                raise ValueError('missing value for \'last_modified\'')
            self.created_at = get_current_time()
            self.last_modified_at = self.created_at

    @property
    def name(self):
        """Get the value of the object property with key 'name'. The result is
        None if no such property exists.

        Returns
        -------
        string
        """
        return self.properties.get_property(PROPERTY_NAME, None)


class ViztrailHandle(ViztrailDescriptor):
    """Handle for a Vizier viztrail. Each viztrail has a unique identifier and
    a dictionary of branches keyed by their name. Branches are sequence of
    workflow version identifiers representing the history of the workflow for
    the respective branch.

    Attributes
    ----------
    identifier : string
        Unique viztrail identifier
    branches : dict(ViztrailBranch)
        Dictionary of branches. Each branch is represented by the sequence
        of workflow versions that represent the history of the workflow for
        the branch.
    exec_env_id: string
        Identifier of the execution environment for the viztrail
    created_at : datetime.datetime
        Timestamp of viztrail creation (UTC)
    last_modified_at : datetime.datetime
        Timestamp when viztrail was last modified (UTC)
    properties: vizier.core.properties.ObjectPropertiesHandler
        Handler for user-defined properties that are associated with this
        viztrail
    """
    def __init__(
        self, identifier, branches, exec_env_id, properties,
        created_at=None, last_modified_at=None, version_counter=0,
        module_counter=0
    ):
        """Initialize the viztrail identifier and branch dictionary.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        branches : dict(ViztrailBranch)
            Dictionary of branches.
        exec_env_id: string
            Identifier of the execution environment for the viztrail
        properties: vizier.core.properties.ObjectPropertiesHandler
            Handler for user-defined properties that are associated with this
            viztrail
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        last_modified_at : datetime.datetime, optional
            Timestamp when project was last modified (UTC)
        version_counter: int, optional
            Counter to generate unique version identifier
        module_counter: int, optional
            Counter to generate unique module identifier
        """
        self.identifier = identifier
        self.branches = branches
        self.exec_env_id = exec_env_id
        self.properties = properties
        self.version_counter = version_counter
        self.module_counter = module_counter
        # If created_at timestamp is None the viztrail is expected to be a newly
        # created viztrail. For new viztrails the last_modified timestamp and
        # branches listing are expected to be None. For existing viztrails
        # last_modified and branches should not be None
        if not created_at is None:
            if last_modified_at is None:
                raise ValueError('unexpected value for \'last_modified\'')
            self.created_at = created_at
            self.last_modified_at = last_modified_at
        else:
            if not last_modified_at is None:
                raise ValueError('missing value for \'last_modified\'')
            self.created_at = get_current_time()
            self.last_modified_at = self.created_at

    def append_workflow_module(self, branch_id=DEFAULT_BRANCH, workflow_version=-1, command=None, before_id=-1):
        """Append a module to an existing workflow in the viztrail. The module
        is appended to the workflow with the given version number. If the
        version number is negative the workflow at the branch HEAD is the one
        that is being modified.

        If before_id is non-negative the new module is inserted into the
        existing workflow before the module with the specified identifier. If no
        module with the given identifier exists the result is None. If before_id
        is negative, the new module is appended to the end of the workflow.

        Returns a new WorkflowHandle. The workflow itself is not executed but
        materialized. All modules in the returned workflow that are in PENDING
        state need to be executed.

        Raises a ValueError if an invalid command specification is given.

        Parameters
        ----------
        branch_id : string, optional
            Unique branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being modified. If negative
            the branch head is being used.
        command : vizier.workflow.module.ModuleCommand, optional
            Specification of the command that is to be evaluated
        before_id : int, optional
            Insert new module before module with given identifier. Append at end
            of the workflow if negative

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        # Get the workflow that is being modified. Result is None if the branch
        # or workflow version are unknown. In that case we return None.
        workflow = self.get_workflow(branch_id, workflow_version)
        if workflow is None:
            return None
        # Create new module.
        module = ModuleHandle(identifier=self.next_module_id(), command=command)
        # Create list of modules for the modified workflow. Insert the new
        # module before the module with identifier before_id or append the new
        # module if before_id is negative. If before_id is not negative but the
        # reference modules does not exist we return None.
        modules = None
        module_index = -1
        if before_id < 0:
            # Append new module to the end of the workflow
            modules = list(workflow.modules) + [module]
            module_index = len(modules) - 1
        else:
            for i in range(len(workflow.modules)):
                m = workflow.modules[i]
                if m.identifier == before_id:
                    # Insert new module before the specified module and keep
                    # track of the index position.
                    modules.append(module)
                    module_index = i
                modules.append(m)
            # If the module index is still negative the module with identifier
            # before_id does not exist. In this case we return None.
            if module_index == -1:
                return None
        # Execute the workflow and return the handle for the resulting workflow
        # state. Execution should persist the generated workflow state.
        result = viztrail.engine.execute_workflow(
            viztrail_id,
            branch_id,
            viztrail.next_version_id(),
            modules,
            module_index
        )
        # Update viztrail information
        return self.persist_workflow_result(
            viztrail,
            branch_id,
            result=result,
            action=ACTION_INSERT,
            package_id=command.module_type,
            command_id=command.command_identifier
        )

    @abstractmethod
    def delete(self):
        """Delete the viztrail. The implementation of this method should remove
        all resources connected to the viztrail and also make sure that the
        viztrail does not appear in any index etc. to avoid errors when
        reloading the application.
        """
        raise NotImplementedError

    @abstractmethod
    def get_workflow(self, branch_id=DEFAULT_BRANCH, version=-1):
        """Get the workflow with the given version number from the workflow
        history of the given branch.

        Returns None if the branch or the workflow version do not exist.

        Parameters
        ----------
        branch_id: string, optional
            Unique branch identifier
        version: int, optional
            Workflow version number

        Returns
        -------
        vizier.workflow.base.WorkflowHandle
        """
        raise NotImplementedError

    def next_module_id(self):
        """Increment the module id counter and return the new value.

        Returns
        -------
        int
        """
        self.module_counter += 1
        return self.module_counter

    def next_version_id(self):
        """Increment the version id counter and return the new value.

        Returns
        -------
        int
        """
        self.version_counter += 1
        return self.version_counter
