# Copyright (C) 2018 New York University
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

"""Vizier DB Workflow API - Viztrail Repository

Specification of the viztrail repository that manages viztrails and the
execution of viztrail workflows.
"""

from abc import abstractmethod

from vizier.core.system import VizierSystemComponent
from vizier.workflow.base import DEFAULT_BRANCH


class ViztrailRepository(VizierSystemComponent):
    """Repository for viztrails. This is an abstract class that defines all
    necessary methods to maintain viztrails and to manipulate and execute
    workflows.

    By default all methods operate on the workflow that is at the HEAD of the
    defautl branch (if applicable).
    """
    def __init__(self, build):
        """Initialize the build information. Expects a dictionary containing two
        elements: name and version.

        Raises ValueError if build dictionary is invalid.

        Parameters
        ---------
        build : dict()
            Build information
        """
        super(ViztrailRepository, self).__init__(build)

    def append_workflow_module(self, viztrail_id, branch_id=DEFAULT_BRANCH, workflow_version=-1, command=None, before_id=-1):
        """Append a module to a workflow in a given viztrail. The module is
        appended to the workflow that is identified by the given version number.
        If the version number is negative the workflow at the branch HEAD is the
        one that is being modified.

        The modified workflow will be executed. The result is the new head of
        the branch.

        If before_id is non-negative the new module is inserted into the
        existing workflow before the module with the specified identifier. If no
        module with the given identifier exists a ValueError is raised. If
        before_id is negative, the new module is appended to the end of the
        workflow.

        Returns a handle to the state of the executed workflow. Returns None if
        the specified viztrail, branch, or workflow do not exist.

        Raises a ValueError if an invalid command specification is given.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being modified. If negative
            the branch head is being used.
        command : vizier.workflow.module.ModuleSpecification, optional
            Specification of the command that is to be evaluated
        before_id : int, optional
            Insert new module before module with given identifier. Append at end
            of the workflow if negative

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        # Get viztrail. The result is None if no VizTrail with the given
        # identifier exists. In that case we return None.
        viztrail = self.get_viztrail[viztrail_id]
        if viztrail is None:
            return None
        # Get the workflow that is being modified. Result is None if the branch
        # or workflow version are unknown. In that case we return None.
        workflow = viztrail.get_workflow(branch_id, workflow_version)
        if workflow is None:
            return None
        # Validate given command specification. Will raise exception if invalid.
        viztrail.validate_command(command)
        # Create new module.
        module = ModuleHandle(viztrail.next_module_id(), command)
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
    def create_branch(self, viztrail_id, source_branch=DEFAULT_BRANCH, workflow_version=-1, module_id=-1, properties=None):
        """Create a new workflow branch in a given viztrail. The new branch is
        created from the specified workflow in the source branch starting at
        module module_id. If module_id is negative the new branch starts after
        the last module of the source branch head workflow.

        Returns the handle for the new branch or None if the given viztrail does
        not exist. Raises ValueError if (1) the source branch does not exist,
        (2) no module with the specified identifier exists, or (3) an attempt is
        made to branch from an empty workflow.

        Parameters
        ----------
        viztrail_id : string, optional
            Unique viztrail identifier
        source_branch : string, optional
            Unique branch identifier for existing branch
        workflow_version: int, optional
            Version number of the workflow that is being modified. If negative
            the branch head is being used.
        module_id: int, optional
            Start branch from module with given identifier in source_branch.
            The new branch starts at the end of the source branch if module_id
            has a negative value.
        properties: dict, optional
            Set of properties for the new branch

        Returns
        -------
        vizier.workflow.base.ViztrailBranch
        """
        raise NotImplementedError

    @abstractmethod
    def create_viztrail(self, engine_id, properties):
        """Create a new viztrail.

        Raises ValueError if the given workflow engine is unknown.

        Parameters
        ----------
        engine_id: string
            Identifier for workflow engine that is used to execute workflows of
            the new viztrail
        properties: dict
            Set of properties for the new viztrail

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def delete_branch(self, viztrail_id, branch_id=None):
        """Delete the viztrail branch with the given identifier. Returns the
        modified viztrail handle. The result is None if either the branch or the
        viztrail is unknown.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id: string, optional
            Unique workflow branch identifier

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def delete_workflow_module(self, viztrail_id, branch_id=DEFAULT_BRANCH, workflow_version=-1, module_id=-1):
        """Delete the module with the given identifier in the specified
        workflow. The resulting workflow is execute and the resulting workflow
        will form the new head of the given viztrail branch.

        The result is True on success. Returns False if no viztrail, branch, or
        module with given identifier exists.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id: string, optional
            Unique workflow branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being modified. If negative
            the branch head is being used.
        module_id : int, optional
            Module identifier

        Returns
        -------
        bool
        """
        # Get viztrail. The result is None if no VizTrail with the given
        # identifier exists. In that case we return None.
        viztrail = self.get_viztrail[viztrail_id]
        if viztrail is None:
            return None
        # Get the workflow that is being modified. Result is None if the branch
        # or workflow version are unknown. In that case we return None.
        workflow = viztrail.get_workflow(branch_id, workflow_version)
        if workflow is None:
            return None
        # Create list of modules for the modified workflow. We keep all modules
        # except for the one that matches the given module_id. If no module
        # matches the id we return False.
        modules = []
        module_index = -1
        for i in range(len(workflow.modules)):
            m = workflow.modules[i]
            if m.identifier != module_id:
                modules.append(m)
            else:
                # We found the module that is being deleted. Keep track of the
                # module index
                module_index = i
        # If the module index is still negative the module that was supposed to
        # be deleted does not exist. In this case we return None.
        if module_index == -1:
            return False
        # Get the command for the deleted module to keep track of the deleted
        # operation in the branch history
        command = workflow.modules[module_index].command
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
            action=ACTION_DELETE,
            package_id=command.module_type,
            command_id=command.command_identifier
        )

    @abstractmethod
    def delete_viztrail(self, viztrail_id):
        """Delete the viztrail with given identifier. The result is True if a
        viztrail with the given identifier existed, False otherwise.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def get_viztrail(self, viztrail_id):
        """Retrieve the viztrail with the given identifier. The result is None
        if no viztrail with given identifier exists.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        raise NotImplementedError

    @abstractmethod
    def get_workflow(self, viztrail_id, branch_id=DEFAULT_BRANCH, workflow_version=-1):
        """Retrieve the workflow at the HEAD of the branch with branch_id in the
        given viztrail. The result is None if the viztrail or branch do not
        exist.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being retrieved. If negative
            the branch head is returned.

        Returns
        -------
        vizier.workflow.base.WorkflowHandle
        """
        raise NotImplementedError

    @abstractmethod
    def list_viztrails(self):
        """List handles for all viztrails in the repository.

        Returns
        -------
        list(vizier.workflow.base.ViztrailHandle)
            List of viztrail handles
        """
        raise NotImplementedError

    @abstractmethod
    def persist_workflow_result(self, viztrail, branch_id, result, action=None, package_id=None, command_id=None):
        """Persist the result of executing a viztrail workflow. Writes the new
        workflow file and the updated viztrail informaiton. Returns the modified
        viztrail.

        Parameters
        ----------
        viztrail: vizier.workflow.repository.fs.FileSystemViztrailHandle
            handle for updated viztrail
        branch_id: string
            Unique identifier of the updated viztrail branch
        result: vizier.workflow.engine.base.WorkflowExecutionResult
            Result of workflow execution
        actions: string, optional
            Identifier of the action that created the workflow version (create,
            insert, delete, or replace)
        package_id: string, optional
            Identifier of the package the module command is from
        command_id: string, optional
            Identifier of the module command

        Returns
        -------
        vizier.workflow.repository.fs.FileSystemViztrailHandle
        """
        raise NotImplementedError

    def replace_workflow_module(self, viztrail_id, branch_id=DEFAULT_BRANCH, workflow_version=-1, module_id=-1, command=None):
        """Replace an existing module in a workflow. The module is replaced in
        the workflow that is identified by the given version number. If the
        version number is negative the workflow at the branch HEAD is the
        one that is being modified. The modified workflow is executed and the
        result will be the new head of the branch.

        Returns a handle to the state of the executed workflow. Returns None if
        the specified viztrail, branch, workflow, or module do not exist.

        Raises a ValueError if an invalid command specification is given.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being retrieved. If negative
            the branch head is returned.
        module_id : int, optional
            Identifier of the module that is being replaced
        command : vizier.workflow.module.ModuleSpecification
            Specification of the command that is to be evaluated

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        # Get viztrail. The result is None if no VizTrail with the given
        # identifier exists. In that case we return None.
        viztrail = self.get_viztrail[viztrail_id]
        if viztrail is None:
            return None
        # Get the workflow that is being modified. Result is None if the branch
        # or workflow version are unknown. In that case we return None.
        workflow = viztrail.get_workflow(branch_id, workflow_version)
        if workflow is None:
            return None
        # Validate given command specification. Will raise exception if invalid.
        viztrail.validate_command(command)
        # Create modified module list replacing the specified module with the
        # given command. Return None if no module with the specified id exists.
        modules = []
        module_index = -1
        for i in range(len(workflow.modules)):
            m = workflow.modules[i]
            if m.identifier == module_id:
                # We found the module that is being replaced. Keep track of the
                # index position.
                modules.append(ModuleHandle(module_id, command))
                module_index = i
            else:
                modules.append(m)
        # If the module index is still negative the module that was supposed to
        # be replaced does not exist. In this case we return None.
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
            action=ACTION_REPLACE,
            package_id=command.module_type,
            command_id=command.command_identifier
        )


class CachedViztrailRepository(ViztrailRepository):
    """A cached viztrail repository extends the base class. The repository keeps
    all VizTrails in cache.
    """
    def __init__(self):
        """Initialize the dictionary that is used as cache."""
        self.cache = dict()

    def delete_viztrail(self, viztrail_id):
        """Delete the viztrail with given identifier. The result is True if a
        viztrail with the given identifier existed, False otherwise.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        bool
        """
        if viztrail_id in self.cache:
            # Delete viztrail by calling the delete() method from the viztrail
            # object.
            self.cache[viztrail_id].delete()
            # Remove viztrail from cache. It is expected that the previous
            # delete call will remove the viztrail from any index so that
            # no attempt to re-load the viztrail can be made when the cached
            # repository is initialized again.
            del self.cache[viztrail_id]
            return True
        else:
            return False

    def get_viztrail(self, viztrail_id):
        """Retrieve the viztrail with the given identifier. The result is None
        if no viztrail with given identifier exists.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        # Return information directly from the cache
        return self.cache[viztrail_id] if viztrail_id in self.cache else None

    def get_workflow(self, viztrail_id, branch_id=DEFAULT_BRANCH, workflow_version=-1):
        """Retrieve the workflow at the HEAD of the branch with branch_id in the
        given viztrail. The result is None if the viztrail or branch do not
        exist.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        branch_id : string, optional
            Unique branch identifier
        workflow_version: int, optional
            Version number of the workflow that is being retrieved. If negative
            the branch head is returned.

        Returns
        -------
        vizier.workflow.base.WorkflowHandle
        """
        # Return None if worktrail does not exist
        if not viztrail_id in self.cache:
            return None
        # Get workflow in given branch. Result is None if branch or the workflow
        # does not exist.
        return self.cache[viztrail_id].get_workflow(branch_id, workflow_version)

    def list_viztrails(self):
        """List handles for all viztrails in the repository.

        Returns
        -------
        list(vizier.workflow.base.ViztrailHandle)
            List of viztrail handles
        """
        # Return list of values in cache
        return self.cache.values()
