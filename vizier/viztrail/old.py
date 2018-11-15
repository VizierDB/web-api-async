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

A viztrail is a set of branches. Each branch has a unique identifier and an
optional name. Each branch is a sequence of workflow versions.
"""

from abc import abstractmethod

from vizier.core.timestamp import get_current_time


"""Key's for default viztrail properties."""
# Human readable object name for viztrails and viztrail branches
PROPERTY_NAME = 'name'


class NamedObject(object):
    """Viztrails and branches are named objects. A named object maintains a set
    of user-defined annotations. The annotations with the key defined in
    PROPERTY_NAME is interpreted as the human-readable object name.

    This base class provides getter and setter methods to access and manipulate
    the human-readable object name.

    Attributes
    ----------
    name: string
        Human readable viztrail name
    properties: vizier.core.annotation.base.ObjectAnnotationSet
        Set of user-defined properties that are associated with this viztrail
    """
    def __init__(self, properties):
        """Initialize the object's properties set.

        Parameters
        ----------
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Handler for user-defined properties
        """
        self.properties = properties

    @property
    def name(self):
        """Get the value of the object property with key 'name'. The result is
        None if no such property exists.

        Returns
        -------
        string
        """
        return self.properties.find_one(
            key=PROPERTY_NAME,
            default_value=None,
            raise_error_on_multi_value=False
        )

    @name.setter
    def name(self, value):
        """Set the value of the object property with key 'name'.

        Parameters
        ----------
        name: string
            Human-readable name for the viztrail
        """
        return self.properties.replace(key=PROPERTY_NAME, value=str(name))


# ------------------------------------------------------------------------------
# Branches
# ------------------------------------------------------------------------------

class BranchProvenance(object):
    """Simple object that contains provenance information for each branch. The
    provenance for a branch includes the source branch identifier, the
    identifier for the workflow, and the identifier for the module in the source
    workflow from which the branch was created. The module identifier defines
    the last module in the source workflow that was copied too the new branch.

    If a branch is not created from a previous branch but from scratch, e.g.,
    the first branch in a viztrail, the source_branch is None and the other
    two identifier -1. If the source-branch is not None the workflow and module
    identifier are greater or equal zero.

    Attributes
    ----------
    source_branch : string
        Unique identifier of source branch
    workflow_id: int
        Identifier of source workflow
    module_id: int
        Identifier of module at which the new branch started

    """
    def __init__(self, source_branch=None, workflow_id=-1, module_id=-1):
        """Initialize the provenance object.

        Raises ValueError if the source branch is not None but the workflow or
        module identifier are negative.

        Parameters
        ----------
        source_branch : string
            Unique identifier of source branch
        workflow_id: int
            Identifier of source workflow
        module_id: int
            Identifier of module at which the new branch started
        """
        # Raise an exception if the source branch is not None but the workflow
        # or module identifier are negative
        if not source_branch is None:
            if workflow_id < 0:
                raise ValueError('invalid workflow identifier \'' + str(workflow_id) + '\'')
            if module_id < 0:
                raise ValueError('invalid module identifier \'' + str(module_id) + '\'')
        self.source_branch = source_branch
        self.workflow_id = workflow_id if not source_branch is None else -1
        self.module_id = module_id if not source_branch is None else -1


class BranchHandle(NamedObject):
    """Branch in a viztrail. Each branch has a unique identifier, a set of user-
    defined properties, and a provenance object that defines the branch source.

    Each branch is a list of workflow versions. The sequence of workflows define
    the history of the branch, i.e., all the previous states and the current
    state of the branch. The last workflow version in the branch represents the
    head of the branch and therefore the current state of the branch.

    Attributes
    ----------
    identifier: string
        Unique branch identifier
    properties: vizier.core.properties.ObjectPropertiesHandler
        User-defined properties for the branch
    provenance: vizier.viztrail.base.BranchProvenance
        Provenance information for this branch
    """
    def __init__(self, identifier, properties, provenance):
        """Initialize the viztrail branch.

        Parameters
        ----------
        identifier: string
            Unique branch identifier
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Handler for user-defined properties
        provenance: vizier.viztrail.base.BranchProvenance
            Provenance information for this branch
        """
        super(BranchHandle, self).__init__(properties=properties)
        self.identifier = identifier
        self.provenance = provenance

    def get_head(self):
        """Shortcut the get the workflow at the head of the branch. The result
        is None if the branch is empty.

        Returns
        -------
        ???
        """
        return self.get_workflow(workflow_id=-1)

    @abstractmethod
    def get_history(self):
        """Get the list of workflows for the branch that define the branch
        history. The result includes the current state of the branch as the
        last element in the list.

        Returns
        -------
        list(???)
        """
        raise NotImplementedError

    @abstractmethod
    def get_workflow(self, workflow_id=-1):
        """Get the workflow with the given identifier. If the identifier is
        negative the head of the branch is returned. The result is None if the
        branch is empty.

        Parameters
        ----------
        workflow_id: int
            Unique workflow identifier

        Returns
        -------
        ???
        """
        raise NotImplementedError


# ------------------------------------------------------------------------------
# Viztrails
# ------------------------------------------------------------------------------

class ViztrailHandle(NamedObject):
    """Handle for a viztrail. The viztrail handle provides access to all the
    vviztrail information and branches.

    Each viztrail has a unique viztrail identifier, timestamp information for
    viztrail creation and the last modification, and the viztrail properties.

    The viztrail properties contain the optional name. If the respective key is
    not set the viztrail name is None.

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
    properties: vizier.core.annotation.base.ObjectAnnotationSet
        Set of user-defined properties that are associated with this viztrail
    """
    def __init__(
        self, identifier, properties, created_at=None, last_modified_at=None
    ):
        """Initialize the viztrail descriptor.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Handler for user-defined properties
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        last_modified_at : datetime.datetime, optional
            Timestamp when project was last modified (UTC)
        """
        super(ViztrailHandle, self).__init__(properties=properties)
        self.identifier = identifier
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

    @abstractmethod
    def create_branch(self, branch_id, workflow_id=-1, module_id=-1, properties=None):
        """Create a new branch. The combination of branch_id, workflow_id and
        module_id specifies the branching point, i.e., the source of the new
        branch. The optional properties set may contain the name for the new
        branch.

        Returns None if the branch source does not exist.

        Parameters
        ----------
        branch_id : string
            Unique identifier for existing branch
        workflow_id: int, optional
            Identifier for workflow in source branch that is the source for the
            branch step.. If negative, the workflow at the branch head is used.
        module_id: int, optional
            Start branch from module with given identifier in workflow.
            The new branch contains all modules from the source workflow up
            until (including) the identified module. Starts at the end of the
            workflow if module_id is negative.
        properties: dict, optional
            Set of properties for the new branch

        Returns
        -------
        vizier.viztrail.base.BranchHandle
        """
        raise NotImplementedError

    @abstractmethod
    def delete_branch(self, branch_id):
        """Delete branch with the given identifier. Returns True if the branch
        existed and False otherwise.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def get_branch(self, branch_id):
        """Get handle for the branch with the given identifier. Returns None if
        no branch with the given identifier exists.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.base.BranchHandle
        """
        raise NotImplementedError

    @abstractmethod
    def list_branches(self):
        """Get a list of branches that are currently defined for the viztrail.

        Returns
        -------
        list(vizier.viztrail.base.BranchHandle)
        """
        raise NotImplementedError


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
