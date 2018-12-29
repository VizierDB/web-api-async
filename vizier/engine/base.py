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

"""The vizier engine defines the interface that is used by the API for creating,
deleting, and manipulating projects as well as for the orchestration of workflow
modules. Different instantiations of the engine will use different
implementations for datasores, filestores, vitrails repositories and backends.

The idea is to have a set of pre-defined 'configurations' for vizier engines
that can be used in different environments (i.e., (a) when running vizier on a
local machine, (b) with a set of celery workers, (c) running each project in
its own container, etc). The engine that is used by a vizier instance is
specified in the configuration file and loaded when the instance is started.
"""

from abc import abstractmethod

from vizier.engine.controller import WorkflowController


class VizierEngine(WorkflowController):
    """Engine that is used by the API to create and manipulate projects. The
    engine maintains viztrails that represent user projects. At its base the
    engine is a wrapper around the viztrails repository and the execution
    backend. Depending on the implementation the engine may further include
    the datastore, and filestore. Different configurations for a vizier instance
    will use different classes for the wrapped objects. Each configuration
    should have a descriptive name and version information (for display purposes
    in the front-end).

    The engine is potentially initialize multiple-times (e.g., when  running the
    web service using Flask. To avoid loading and initializing objects multiple
    times the .init() method is called before the first request is made. This
    allows to intitialize internal copies of objects and caches only once.
    """
    @abstractmethod
    def append_workflow_module(self, branch_id, command):
        """Append module to the workflow at the head of the given viztrail
        branch. The modified workflow will be executed. The result is the new
        head of the branch.

        Returns the handle for the new module in the modified workflow.

        Raises ValueError if the specified branch does not exist.

        Parameters
        ----------
        branch_id : string
            Unique branch identifier
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed by the appended
            workflow module

        Returns
        -------
        vizier.viztrail.module.base.ModuleHandle
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_exec(self, branch_id):
        """Cancel the execution of all active modules for the head workflow of
        the given branch. Sets the status of all active modules to canceled and
        sends terminate signal to running tasks. The finished_at property is
        set to the current time. The module outputs will be empty.

        Returns the handle for the modified workflow. If the specified branch
        is unknown or the branch head is None ValueError is raised.

        Parameters
        ----------
        branch_id : string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
        """
        raise NotImplementedError

    @abstractmethod
    def create_project(self, properties=None):
        """Create a new project. Will create a viztrail in the underlying
        viztrail repository. The initial set of properties is an optional
        dictionary of (key,value)-pairs where all values are expected to either
        be scalar values or a list of scalar values. The properties are passed
        to the create method for the viztrail repository.

        Parameters
        ----------
        properties: dict, optional
            Set of properties for the new viztrail

        Returns
        -------
        vizier.viztrail.engine.project.ProjectHandle
        """
        raise NotImplementedError

    @abstractmethod
    def delete_project(self, project_id):
        """Delete all resources that are associated with the given project.
        Returns True if the project existed and False otherwise.

        Parameters
        ----------
        project_id: string
            Unique project identifier

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def delete_workflow_module(self, branch_id, module_id):
        """Delete the module with the given identifier from the workflow at the
        head of the viztrail branch. The resulting workflow is executed and will
        be the new head of the branch.

        Returns the list of remaining modules in the modified workflow that are
        affected by the deletion. Raises ValueError if the branch does not exist
        or the current head of the branch is active.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier
        module_id : string
            Unique module identifier

        Returns
        -------
        list(vizier.viztrail.module.base.ModuleHandle)
        """
        raise NotImplementedError

    @abstractmethod
    def get_project(self, project_id):
        """Get the handle for the given project. Returns None if the project
        does not exist.

        Returns
        -------
        vizier.viztrail.engine.project.ProjectHandle
        """
        if project_id in self.projects:
            return self.projects[project_id]
        return None

    @abstractmethod
    def init(self):
        """Initialize the engine. This method is called once before the first
        call to any of the other methods is made. This allows to create local
        copies of object instances and initialize caches.
        """
        raise NotImplementedError

    @abstractmethod
    def insert_workflow_module(self, branch_id, before_module_id, command):
        """Insert a new module to the workflow at the head of the given viztrail
        branch. The modified workflow will be executed. The result is the new
        head of the branch.

        The module is inserted into the sequence of workflow modules before the
        module with the identifier that is given as the before_module_id
        argument.

        Returns the list of affected modules in the modified workflow. Raises
        ValueError if specified branch does not exist or the current head of
        the branch is active.

        Parameters
        ----------
        branch_id : string
            Unique branch identifier
        before_module_id : string
            Insert new module before module with given identifier.
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be executed by the inserted
            workflow module

        Returns
        -------
        list(vizier.viztrail.module.base.ModuleHandle)
        """
        raise NotImplementedError

    @abstractmethod
    def list_projects(self):
        """Get a list of all project handles.

        Returns
        -------
        list(vizier.viztrail.engine.project.ProjectHandle)
        """
        raise NotImplementedError

    @abstractmethod
    def replace_workflow_module(self, branch_id, module_id, command):
        """Replace an existing module in the workflow at the head of the
        specified viztrail branch. The modified workflow is executed and the
        result is the new head of the branch.

        Returns the list of affected modules in the modified workflow. Raises
        ValueError if the given branch is unknown or the current head of the
        branch is active.

        Parameters
        ----------
        branch_id : string, optional
            Unique branch identifier
        module_id : string
            Identifier of the module that is being replaced
        command : vizier.viztrail.command.ModuleCommand
            Specification of the command that is to be evaluated

        Returns
        -------
        list(vizier.viztrail.module.base.ModuleHandle)
        """
        raise NotImplementedError
