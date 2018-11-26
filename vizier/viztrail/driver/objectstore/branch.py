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

"""Implementation for branch handles that maintain all resources as objects
and folders in an object store.
"""

from vizier.core.annotation.persistent import PersistentAnnotationSet
from vizier.core.io.base import DefaultObjectStore
from vizier.core.util import init_value
from vizier.core.timestamp import to_datetime
from vizier.viztrail.branch import BranchHandle, BranchProvenance
from vizier.viztrail.driver.objectstore.module import OSModuleHandle
from vizier.viztrail.workflow import WorkflowDescriptor, WorkflowHandle
from vizier.viztrail.workflow import ACTION_CREATE


"""Resource identifier"""
OBJ_METADATA = 'branch.json'
OBJ_PROPERTIES = 'properties.json'


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


class OSBranchHandle(BranchHandle):
    """Handle for branches that maintains all resources as object and folders in
    an object store.

    A branch is a sequence of workflows. Each workflow is maintained as an
    object in the branch base folder. The object contains the descriptor for
    each workflow. Only the workflow at the branch head and at most one other
    workflow are kept in memory together with all their modules at the same
    time. Access to all other workflow version will require them to be read from
    the object store.

    Folders and Resources
    ---------------------
    branch.json        : Branch provenance object
    properties.json    : Branch annotations
    workflows.json     : Sequence of workflow identifier in branch history
    <workflow-id>.json : Workflow object containing workflow descriptor and
                         sequence of module identifier. Workflow identifier are
                         positive integer and the order of identifiers reflects
                         the order of workflows in the branch history.
    """
    def __init__(
        self, identifier, base_path, modules_folder, provenance, properties,
        workflows=None, head=None, object_store=None
    ):
        """Initialize the branch handle.

        Parameters
        ----------
        identifier: string
            Unique branch identifier
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
        self.base_path = base_path
        self.modules_folder = modules_folder
        self.object_store = init_value(object_store, DefaultObjectStore())
        self.workflows = init_value(workflows, list())
        self.head = head
        self.cache = None

    def append_workflow(self, workflow):
        """Append the given workflow handle to the branch. The workflow becomes
        the new head of the branch.

        Parameters
        ----------
        workflow: vizier.viztrail.workflow.WorkflowHandle
            New branch head
        """
        raise NotImplementedError

    @staticmethod
    def create_branch(
        identifier, base_path, modules_folder, provenance=None, properties=None,
        modules=None, object_store=None
    ):
        """Create a new branch. If the workflow is given the new branch contains
        exactly this workflow. Otherwise, the branch is empty.

        Parameters
        ----------
        identifier: string
            Unique branch identifier
        base_path: string
            path to the folder for branch resources
        modules_folder: string
            Path to module resources folder
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
        vizier.viztrail.driver.objectstore.branch.OSBranchHandle
        """
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # If base path does not exist raise an exception
        if not object_store.exists(base_path):
            raise ValueError('base path does not exist')
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
            # Get a new identifier by creating an empty workflow file
            workflow_id = object_store.create_object(
                parent_folder= base_path,
                identifier=get_workflow_id(0),
                suffix='.json'
            )
            # Create the diescriptor and write workflow to store
            descriptor = WorkflowDescriptor(
                identifier=workflow_id,
                action=ACTION_CREATE
            )
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
            head = WorkflowHandle(
                identifier=workflow_identifier,
                branch_id=identifier,
                descriptor=descriptor,
                modules=[
                    OSModuleHandle.load_module(
                        module_path=object_store.join(modules_folder, module_id),
                        object_store=object_store
                    ) for module_id in modules
                ]
            )
            workflows.append(descriptor)
        # Return handle for new viztrail branch
        return OSBranchHandle(
            identifier=identifier,
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
        elif not self.cache is None:
            # Check if the currently caches workflow matches the identifier
            if self.cache.identifier == workflow_id:
                return self.cache
        for wf_desc in self.workflows:
            if wf_desc.identifier == workflow_id:
                # Read the workflow modules and set the workflow as the current
                # cache element
                obj = self.object_store.read_object(
                    self.object_store.join(
                        self.base_path,
                        workflow_id + '.json'
                    )
                )
                modules = list()
                for module_id in obj[KEY_WORKFLOW_MODULES]:
                    modules.append(
                        OSModuleHandle.load_module(
                            module_path=self.object_store.join(
                                self.modules_folder,
                                module_id
                            ),
                            object_store=self.object_store
                        )
                    )
                self.cache = WorkflowHandle(
                    identifier=wf_desc.identifier,
                    branch_id=self.identifier,
                    modules=modules,
                    descriptor=wf_desc
                )
                return self.cache
        # If this point is reached the identifier does not reference a workflow
        # in the brach history
        return None

    @staticmethod
    def load_branch(identifier, base_path, modules_folder, object_store=None):
        """Load branch from disk. Reads the branch provenance information and
        descriptors for all workflows in the branch history. If the branch
        history is not empty the modules for the workflow at the branch head
        will be read as well.

        Parameters
        ----------
        identifier: string
            Unique branch identifier
        base_path: string
            Path to folder containing branch resources
        modules_folder: string
            Path to folder containing workflow modules
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources

        Returns
        -------
        vizier.viztrail.driver.objectstore.branch.OSBranchHandle
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
                        created_at=desc[KEY_CREATED_AT]
                    )
                )
        # Sort workflows in ascending order of their identifier
        workflows.sort(key=lambda x: x.identifier)
        # Read all modules for the workflow at the branch head (if exists)
        head = None
        if len(workflows) > 0:
            # The workflow descriptor is the last element in the workflows list
            descriptor = workflows[-1]
            modules = list()
            for module_id in workflow_obj[KEY_WORKFLOW_MODULES]:
                module_path = object_store.join(modules_folder, module_id)
                modules.append(
                    OSModuleHandle.load_module(
                        module_path=module_path,
                        object_store=object_store
                    )
                )
            head = WorkflowHandle(
                identifier=descriptor.identifier,
                branch_id=identifier,
                modules=modules,
                descriptor=descriptor
            )
        return OSBranchHandle(
            identifier=identifier,
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
