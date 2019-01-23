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

"""Implementation for branch handles that maintain all resources as objects
and folders in an object store.
"""

from vizier.core.annotation.persistent import PersistentAnnotationSet
from vizier.core.io.base import DefaultObjectStore
from vizier.core.util import init_value
from vizier.core.timestamp import get_current_time, to_datetime
from vizier.viztrail.branch import BranchHandle, BranchProvenance
from vizier.viztrail.objectstore.module import OSModuleHandle
from vizier.viztrail.objectstore.module import get_module_path
from vizier.viztrail.module.base import MODULE_PENDING, MODULE_RUNNING
from vizier.viztrail.module.timestamp import ModuleTimestamp
from vizier.viztrail.workflow import WorkflowDescriptor, WorkflowHandle
from vizier.viztrail.workflow import ACTION_CREATE, ACTION_INSERT


"""Resource identifier"""
OBJ_METADATA = 'branch'
OBJ_PROPERTIES = 'properties'


"""Json object element keys."""
KEY_ACTION = 'action'
KEY_COMMAND_ID = 'commandId'
KEY_CREATED_AT = 'createdAt'
KEY_MODULE_ID = 'moduleId'
KEY_PACKAGE_ID = 'packageId'
KEY_SOURCE_BRANCH = 'sourceBranch'
KEY_WORKFLOW_DESCRIPTOR = 'descriptor'
KEY_WORKFLOW_ID = 'workflowId'
KEY_WORKFLOW_MODULES = 'modules'


"""Default size of the branch cache."""
DEFAULT_CACHE_SIZE = 3


class OSBranchHandle(BranchHandle):
    """Handle for branches that maintains all resources as object and folders in
    an object store.

    A branch is a sequence of workflows. Each workflow is maintained as an
    object in the branch folder. The object contains the descriptor for each
    workflow and the list of identifier for modules in the workflow.

    Note that the objectstore driver does not implement a subclass for the
    WorkflowHandle at this point. Thus, materialization of workflow handles is
    done by the branch handle. The structure of the materialized object that
    represents workflow handles is as follows:

    - id: Workflow identifier
    - descriptor:
      - action: Identifier for action that created the workflow
      - packageId: Identifier of package for command that created the workflow
      - commandId: Identifier for command that created the workflow
      - createdAt: Timestamp of workflow creation. This is the finished_at time
                   of the creating command (or created_at if the workflow was
                   created in an active state).
    - modules: List of module identifier representing the sequence of modules in
               the workflow

    The workflow at the branch head is kept in memory with all modules fully
    loaded. The branch cache allows to keep an additional number of workflows in
    memory with all their modules loaded. Access to all other workflow version
    will require them to be read from the object store.

    Folders and Resources
    ---------------------
    branch.       : Branch provenance object
    properties    : Branch annotations
    workflows     : Sequence of workflow identifier in branch history
    <workflow-id> : Workflow object containing workflow descriptor and
                    sequence of module identifier. Workflow identifier are
                    positive integer and the order of identifiers reflects
                    the order of workflows in the branch history.
    """
    def __init__(
        self, identifier, is_default, base_path, modules_folder, provenance,
        properties, workflows=None, head=None, object_store=None,
        cache_size=None
    ):
        """Initialize the branch handle.

        Parameters
        ----------
        identifier: string
            Unique branch identifier
        is_default: bool
            True if this is the default branch for its viztrail
        base_path: string
            Path to branch resources folder
        modules_folder: string
            Path to module resources folder
        provenance: vizier.viztrail.branch.BranchProvenance
            Branch provenance information
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Branch property set
        workflows: list(vizier.viztrail.workflow.WorkflowDescriptor), optional
            List of descriptors for workflows in branch history
        head: vizier.viztrail.workflow.WorkflowHandle, optional
            Current at the head of the branch
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources
        """
        super(OSBranchHandle, self).__init__(
            identifier=identifier,
            properties=properties,
            provenance=provenance
        )
        self.is_default = is_default
        self.base_path = base_path
        self.modules_folder = modules_folder
        self.object_store = init_value(object_store, DefaultObjectStore())
        self.workflows = init_value(workflows, list())
        self.head = head
        self.cache_size = cache_size if not cache_size is None else DEFAULT_CACHE_SIZE
        self.cache = list()

    def add_to_cache(self, workflow):
        """Add the given workflow the the internal cache. Returns the given
        handle for convenience.

        Parameters
        ----------
        workflow: vizier.viztrailworkflow.WorkflowHandle
            Workflow that is added to the cache

        Returns
        -------
        vizier.viztrailworkflow.WorkflowHandle
        """
        # Ensure that we do not evict an active workflow from the cache.
        # Add workflow to cache unless the cache size is zero and the workflow
        # is not active. Remove first non-active element if the size of the
        # cache exceeds the defined limit.
        if self.cache_size > 0 or workflow.is_active:
            self.cache.append(workflow)
            if len(self.cache) > self.cache_size:
                # Remove all inactive workflows from cache that occur at a
                # position that is before the current cache size
                index = 0
                while index < len(self.cache) - self.cache_size:
                    if not self.cache[index].is_active:
                        del self.cache[index]
                    else:
                        index += 1
        return workflow

    def append_workflow(self, modules, action, command, pending_modules=None):
        """Append a workflow as the new head of the branch. The new workflow may
        contain modules that have not been persisted prevoiusly (pending
        modules). These modules are persisted as part of the workflow being
        created.

        Parameters
        ----------
        modules: list(vizier.viztrail.module.ModuleHandle
            List of modules in the workflow that are completed
        action: string
            Identifier of the action that created the workflow
        command: vizier.viztrail.module.ModuleCommand
            Specification of the executed command that created the workflow
        pending_modules: list(vizier.viztrail.module.ModuleHandle, optional
            List of modules in the workflow that need to be materialized

        Returns
        -------
        vizier.viztrail.workflow.base.WorkflowHandle
        """
        workflow_modules = list(modules)
        if not pending_modules is None:
            for pm in pending_modules:
                # Make sure the started_at timestamp is set if the module is
                # running
                if pm.is_running and pm.timestamp.started_at is None:
                    pm.timestamp.started_at = pm.timestamp.created_at
                module = OSModuleHandle.create_module(
                    command=pm.command,
                    external_form=pm.external_form,
                    state=pm.state,
                    timestamp=pm.timestamp,
                    datasets=pm.datasets,
                    outputs=pm.outputs,
                    provenance=pm.provenance,
                    module_folder=self.modules_folder,
                    object_store=self.object_store
                )
                workflow_modules.append(module)
        # Write handle for workflow at branch head
        descriptor = write_workflow_handle(
            modules=[m.identifier for m in workflow_modules],
            workflow_count=len(self.workflows),
            base_path=self.base_path,
            object_store=self.object_store,
            action=action,
            command=command,
            created_at=get_current_time()
        )
        # Get new workflow and replace the branch head. Move the current head
        # to the cache.
        workflow = WorkflowHandle(
            identifier=descriptor.identifier,
            branch_id=self.identifier,
            modules=workflow_modules,
            descriptor=descriptor
        )
        self.workflows.append(workflow.descriptor)
        if not self.head is None:
            self.add_to_cache(self.head)
        self.head = workflow
        return workflow

    @staticmethod
    def create_branch(
        identifier, base_path, modules_folder, is_default=False, provenance=None,
        properties=None, created_at=None, modules=None, object_store=None
    ):
        """Create a new branch. If the workflow is given the new branch contains
        exactly this workflow. Otherwise, the branch is empty.

        Raises ValueError if any of the modules in the given list is in an
        active state.

        Parameters
        ----------
        identifier: string
            Unique branch identifier
        base_path: string
            path to the folder for branch resources
        modules_folder: string
            Path to module resources folder
        is_default: bool, optional
            True if this is the default branch for its viztrail
        provenance: vizier.viztrail.branch.BranchProvenance, optional
            Branch provenance information
        properties: dict, optional
            Initial set of branch properties
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources
        modules: list(string), optional
            List of module identifier for the modules in the workflow at the
            head of the branch

        Returns
        -------
        vizier.viztrail.objectstore.branch.OSBranchHandle
        """
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # If base path does not exist raise an exception
        if not object_store.exists(base_path):
            raise ValueError('base path does not exist')
        # Read module handles first to ensure that none of the modules is in
        # an active state
        if not modules is None:
            wf_modules = read_workflow_modules(
                modules_list=modules,
                modules_folder=modules_folder,
                object_store=object_store
            )
            for m in wf_modules:
                if m.is_active:
                    raise ValueError('cannot branch from active workflow')
        # Set provenance object if not given
        if provenance is None:
            provenance = BranchProvenance()
        # Write provenance information to disk
        doc = {KEY_CREATED_AT: provenance.created_at.isoformat()}
        if not provenance.source_branch is None:
            # If one propery is not None all are expected to be not None
            doc[KEY_SOURCE_BRANCH] = provenance.source_branch
            doc[KEY_WORKFLOW_ID] = provenance.workflow_id
            doc[KEY_MODULE_ID] = provenance.module_id
        object_store.write_object(
            object_path=object_store.join(base_path, OBJ_METADATA),
            content=doc
        )
        # Create the initial workflow if the list of modules is given
        workflows = list()
        head = None
        if not modules is None:
            # Write handle for workflow at branch head
            descriptor = write_workflow_handle(
                modules=modules,
                workflow_count=0,
                base_path=base_path,
                object_store=object_store,
                action=ACTION_CREATE,
                created_at=provenance.created_at
            )
            workflows.append(descriptor)
            # Set the new workflow as the branch head
            head = WorkflowHandle(
                identifier=descriptor.identifier,
                branch_id=identifier,
                modules=wf_modules,
                descriptor=descriptor
            )
        # Return handle for new viztrail branch
        return OSBranchHandle(
            identifier=identifier,
            is_default=is_default,
            base_path=base_path,
            modules_folder=modules_folder,
            provenance=provenance,
            properties=PersistentAnnotationSet(
                object_path=object_store.join(base_path, OBJ_PROPERTIES),
                object_store=object_store,
                annotations=properties
            ),
            workflows=workflows,
            head=head,
            object_store=object_store
        )

    def delete_branch(self) :
        """Deletes the directory that contains all resoures that are associated
        with this viztrail branch.
        """
        # Raise an exception if this is the default branch
        if self.is_default:
                raise RuntimeError('cannot delete default branch')
        self.object_store.delete_folder(self.base_path)

    def get_history(self):
        """Get the list of descriptors for the workflows in the branch history.

        Returns
        -------
        list(vizier.viztrail.workflow.base.WorkflowDescriptor)
        """
        return self.workflows

    def get_workflow(self, workflow_id=None):
        """Get the workflow with the given identifier. If the identifier is
        none the head of the branch is returned. The result is None if the
        branch is empty.

        Parameters
        ----------
        workflow_id: string, optional
            Unique workflow identifier

        Returns
        -------
        vizier.viztrail.workflow.base.WorkflowHandle
        """
        # If identifier is None the head is returned
        if workflow_id is None:
            return self.head
        elif not self.head is None and self.head.identifier == workflow_id:
            return self.head
        # Check if the workflow is in the internal cache
        for wf in self.cache:
            if wf.identifier == workflow_id:
                return wf
        # Read the workflow and modules from object store
        for wf_desc in self.workflows:
            if wf_desc.identifier == workflow_id:
                wf = read_workflow(
                    branch_id=self.identifier,
                    workflow_descriptor=wf_desc,
                    workflow_path=self.object_store.join(
                        self.base_path,
                        workflow_id
                    ),
                    modules_folder=self.modules_folder,
                    object_store=self.object_store
                )
                # Add workflow to cache.
                return self.add_to_cache(wf)
        # If this point is reached the identifier does not reference a workflow
        # in the brach history
        return None

    @staticmethod
    def load_branch(identifier, is_default, base_path, modules_folder, object_store=None):
        """Load branch from disk. Reads the branch provenance information and
        descriptors for all workflows in the branch history. If the branch
        history is not empty the modules for the workflow at the branch head
        will be read as well.

        Parameters
        ----------
        identifier: string
            Unique branch identifier
        is_default: bool
            True if this is the default branch for its viztrail
        base_path: string
            Path to folder containing branch resources
        modules_folder: string
            Path to folder containing workflow modules
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources

        Returns
        -------
        vizier.viztrail.objectstore.branch.OSBranchHandle
        """
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # Load branch provenance. The object will contain the created_at
        # timestamp and optionally the three entries that define the branch
        # point.
        doc = object_store.read_object(
            object_store.join(base_path, OBJ_METADATA)
        )
        created_at = to_datetime(doc[KEY_CREATED_AT])
        if len(doc) == 4:
            provenance = BranchProvenance(
                source_branch=doc[KEY_SOURCE_BRANCH],
                workflow_id=doc[KEY_WORKFLOW_ID],
                module_id=doc[KEY_MODULE_ID],
                created_at=created_at
            )
        else:
            provenance = BranchProvenance(created_at=created_at)
        # Read descriptors for all branch workflows. Workflow descriptors are
        # objects in the base directory that do no match the name of any of the
        # predefied branch object.
        workflows = list()
        for resource in object_store.list_objects(base_path):
            if not resource in [OBJ_METADATA, OBJ_PROPERTIES]:
                resource_path = object_store.join(base_path, resource)
                obj = object_store.read_object(resource_path)
                desc = obj[KEY_WORKFLOW_DESCRIPTOR]
                workflows.append(
                    WorkflowDescriptor(
                        identifier=obj[KEY_WORKFLOW_ID],
                        action=desc[KEY_ACTION],
                        package_id=desc[KEY_PACKAGE_ID],
                        command_id=desc[KEY_COMMAND_ID],
                        created_at=to_datetime(desc[KEY_CREATED_AT])
                    )
                )
        # Sort workflows in ascending order of their identifier
        workflows.sort(key=lambda x: x.identifier)
        # Read all modules for the workflow at the branch head (if exists)
        head = None
        if len(workflows) > 0:
            # The workflow descriptor is the last element in the workflows list
            descriptor = workflows[-1]
            head = read_workflow(
                branch_id=identifier,
                workflow_descriptor=descriptor,
                workflow_path=object_store.join(
                    base_path,
                    descriptor.identifier
                ),
                modules_folder=modules_folder,
                object_store=object_store
            )
        return OSBranchHandle(
            identifier=identifier,
            is_default=is_default,
            base_path=base_path,
            modules_folder=modules_folder,
            provenance=provenance,
            properties=PersistentAnnotationSet(
                object_path=object_store.join(base_path, OBJ_PROPERTIES),
                object_store=object_store
            ),
            workflows=workflows,
            head=head,
            object_store=object_store
        )


# ------------------------------------------------------------------------------
# Helper Method
# ------------------------------------------------------------------------------

def get_workflow_id(identifier):
    """Get a hexadecimal string of eight characters length for the given
    integer.

    Parameters
    ----------
    identifier: int
        Workflow indentifier

    Returns
    -------
    string
    """
    return hex(identifier)[2:].zfill(8).upper()


def read_workflow(branch_id, workflow_descriptor, workflow_path, modules_folder, object_store):
    """Read workflow from object store.

    If a module is encountered that is in an active state it will be set to
    canceled state while reading the workflow. By definition only the branch
    head should be active and therefore we do not expect to ever read an active
    workflow from disk. This is an indication that the repository has been
    shut down and restarted. In this case we cannot be certain whether the
    process that executes an active task is (or will) still be running.

    Parameters
    ----------
    branch_id: string
        Unique branch identifier
    workflow_descriptor: vizier.viztrail.workflow.WorkflowDescriptor
        Workflow descriptor
    workflow_path: string
        Path to the workflow resource
    modules_folder: string
        Path to the folder containing moudle objects
    object_store: vizier.core.io.base.ObjectStore
        Object store implementation to access and maintain resources

    Returns
    -------
    vizier.viztrail.workflow.WorkflowHandle
    """
    # Read the workflow handle and workflow modules
    obj = object_store.read_object(workflow_path)
    modules = read_workflow_modules(
        modules_list=obj[KEY_WORKFLOW_MODULES],
        modules_folder=modules_folder,
        object_store=object_store
    )
    # If any of the modules is active we set the module state to canceled
    for m in modules:
        if m.is_active:
            m.set_canceled()
    # Return workflow handle
    return WorkflowHandle(
        identifier=workflow_descriptor.identifier,
        branch_id=branch_id,
        modules=modules,
        descriptor=workflow_descriptor
    )


def read_workflow_modules(modules_list, modules_folder, object_store):
    """Read workflow modules from object store.

    Parameters
    ----------
    modules_list: list(string)
        List of module identifier
    modules_folder: string
        Path to the folder containing moudle objects
    object_store: vizier.core.io.base.ObjectStore
        Object store implementation to access and maintain resources

    Returns
    -------
    list(vizier.viztrail.objectstore.module.OSModuleHandle)
    """
    modules = list()
    database_state = dict()
    for module_id in modules_list:
        module_path=get_module_path(
            modules_folder=modules_folder,
            module_id=module_id,
            object_store=object_store
        )
        m = OSModuleHandle.load_module(
            identifier=module_id,
            module_path=module_path,
            prev_state=database_state,
            object_store=object_store
        )
        database_state = m.datasets
        modules.append(m)
    return modules


def write_workflow_handle(
    modules, workflow_count, base_path, object_store, action, command=None,
    created_at=None
):
    """Create a handle for a new workflow. Returns the descriptor for the new
    workflow.

    Parameters
    ----------
    modules: list(string)
        List of identifier for modules in the workflow
    workflow_count: int
        Number of workflows in the branch. the count is used to create an unique
        identifier for the new workflow
    base_path: string
        Folder that will contain the new workflow handle
    object_store: vizier.core.io.base.ObjectStore
        Object store implementation to access and maintain resources
    action: string
        Unique identifier of the action that created the workflow
    command: vizier.viztrail.command.ModuleCommand
        Specification of the executed command that created the workflow
    created_at: datatime.datetime, optional
        Timestamp when workflow was created

    Returns
    -------
    vizier.viztrail.workflow.WorkflowDescriptor
    """
    # Get a new workflow identifier by creating an empty object
    workflow_id = object_store.create_object(
        parent_folder= base_path,
        identifier=get_workflow_id(workflow_count)
    )
    # Create the workflow descriptor depending on whether the module command is
    # given or not
    if command is None:
        descriptor = WorkflowDescriptor(identifier=workflow_id, action=action)
    else:
        descriptor = WorkflowDescriptor(
            identifier=workflow_id,
            action=action,
            package_id=command.package_id,
            command_id=command.command_id,
            created_at=created_at
        )
    # Write the workflow handle to the object store
    workflow_path = object_store.join(base_path, workflow_id)
    object_store.write_object(
        object_path=workflow_path,
        content={
            KEY_WORKFLOW_ID: workflow_id,
            KEY_WORKFLOW_DESCRIPTOR: {
                KEY_ACTION: descriptor.action,
                KEY_PACKAGE_ID: descriptor.package_id,
                KEY_COMMAND_ID: descriptor.command_id,
                KEY_CREATED_AT: descriptor.created_at.isoformat()
            },
            KEY_WORKFLOW_MODULES: modules
        }
    )
    return descriptor
