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

"""Implementation for a viztrail handle that maintains all resources as objects
and folders in an object store.
"""

from vizier.core.annotation.persistent import PersistentAnnotationSet
from vizier.core.io.base import DefaultObjectStore
from vizier.core.timestamp import get_current_time, to_datetime
from vizier.core.util import init_value
from vizier.viztrail.driver.objectstore.branch import OSBranchHandle
from vizier.viztrail.base import ViztrailHandle


"""Resource identifier"""
FOLDER_BRANCHES = 'branches'
FOLDER_MODULES = 'modules'
OBJ_BRANCHINDEX = 'branches.json'
OBJ_METADATA = 'viztrail.json'
OBJ_PROPERTIES = 'properties.json'


class OSViztrailHandle(ViztrailHandle):
    """Handle for viztrail that maintains resources as folders and objects in
    an object store.

    Modules are maintained in a subfolder of the viztrails base folder because
    modules may be shared between several workflows. Workflows, on the other
    hand, are specific to the branch they occur in and are maintained withing
    the respective branch subfolder,

    Folders and Resources
    ---------------------
    branches.json   : List of active branches
    properties.json : Viztrail annotations
    viztrail.json   : Viztrail metadata (identifier, timestamp, environment)
    branches/       : Viztrail branches
    modules/        : Modules in viztrail workflows
    """
    def __init__(
        self, identifier, exec_env_id, properties, base_path, object_store=None,
        branches=None, created_at=None, branch_index=None, branch_folder=None,
        modules_folder=None

    ):
        """Initialize the viztrail descriptor.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        exec_env_id: string
            Identifier of the execution environment that is used for the
            viztrail
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Handler for user-defined properties
        base_path: string
            Identifier for folder containing viztrail resources
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources
        branches: list(vizier.viztrail.branch.BranchHandle)
            List of branches in the viztrail
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        branch_index: string, optional
            Path to branch index list
        branch_folder: string, optional
            Path to branches folder
        modules_folder: string, optional
            Path to modules folder
        """
        super(OSViztrailHandle, self).__init__(
            identifier=identifier,
            exec_env_id=exec_env_id,
            properties=properties,
            branches=branches,
            created_at=created_at,
            last_modified_at=created_at
        )
        # Initizlize the object store and identifier for all subfolders.
        self.base_path = base_path
        self.object_store = init_value(object_store, DefaultObjectStore())
        self.branch_index = init_value(branch_index, self.object_store.join(base_path, OBJ_BRANCHINDEX))
        self.branch_folder = init_value(branch_folder, self.object_store.join(base_path, FOLDER_BRANCHES))
        self.modules_folder =  init_value(modules_folder, self.object_store.join(base_path, FOLDER_MODULES))

    def create_branch(self, provenance=None, properties=None, modules=None):
        """Create a new branch. If the list of workflow modules is given this
        defins the branch head. Otherwise, the branch is empty.

        Parameters
        ----------
        provenance: vizier.viztrail.base.BranchProvenance
            Provenance information for the new branch
        properties: dict, optional
            Set of properties for the new branch
        modules: list(string), optional
            List of module identifier for the modules in the workflow at the
            head of the branch

        Returns
        -------
        vizier.viztrail.driver.objectstore.branch.OSBranchHandle
        """
        # Get unique identifier for new branch by creating the subfolder that
        # will contain branch resources
        identifier = self.object_store.create_folder(self.branch_folder)
        branch_path = self.object_store.join(self.branch_folder, identifier)
        # Create materialized branch resource
        branch = OSBranchHandle.create_branch(
            identifier=identifier,
            provenance=provenance,
            properties=properties,
            modules_folder=self.modules_folder,
            modules=modules,
            base_path=branch_path,
            object_store=self.object_store
        )
        # Add the new branch to index and materialize the updated index
        # information
        self.branches[branch.identifier] = branch
        self.object_store.write_object(
            object_path=self.branch_index,
            content=[b for b in self.branches]
        )
        return branch

    @staticmethod
    def create_viztrail(identifier, exec_env_id, base_path, object_store=None, properties=None) :
        """Create a new viztrail resource. Will create the base directory for
        the viztrail. If the directory exists a ValueError is raised.

        Creates subfolders for viztrail resources. Writes viztrail metadata and
        properties to file.

        Parameters
        ----------
        properties: dict
            Set of properties for the new viztrail
        exec_env_id: string
            Identifier of the execution environment that is used for the
            viztrail
        base_path: string
            Identifier for folder containing viztrail resources
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources

        Returns
        -------
        vizier.viztrail.driver.fs.viztrail.FSViztrailHandle
        """
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # If base path does not exist raise an exception
        if not object_store.exists(base_path):
            raise ValueError('base path does not exist')
        # Create empty index file and subfolders for branches, workflows, and
        # modules. The base path folder is expected to exist.
        branch_index = object_store.join(base_path, OBJ_BRANCHINDEX)
        object_store.write_object(object_path=branch_index, content=list())
        branch_folder = object_store.join(base_path, FOLDER_BRANCHES)
        object_store.create_folder(base_path, identifier=FOLDER_BRANCHES)
        modules_folder = object_store.join(base_path, FOLDER_MODULES)
        object_store.create_folder(base_path, identifier=FOLDER_MODULES)
        # Write viztrail metadata to disk
        created_at = get_current_time()
        object_store.write_object(
            object_path=object_store.join(base_path, OBJ_METADATA),
            content={
                'id': identifier,
                'env': exec_env_id,
                'createdAt': created_at.isoformat()
            }
        )
        # Return handle for new viztrail
        return OSViztrailHandle(
            identifier=identifier,
            exec_env_id=exec_env_id,
            properties=PersistentAnnotationSet(
                object_path=object_store.join(base_path, OBJ_PROPERTIES),
                object_store=object_store,
                annotations=properties
            ),
            created_at=created_at,
            base_path=base_path,
            object_store=object_store,
            branch_index=branch_index,
            branch_folder=branch_folder,
            modules_folder=modules_folder
        )

    def delete_viztrail(self) :
        """Deletes the folder that contains all resoures that are associated
        with this viztrail.
        """
        self.object_store.delete_folder(self.base_path)

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
        if branch_id in self.branches:
            # Call the delete method of the branch and update the branch index
            self.branches[branch_id].delete_branch()
            del self.branches[branch_id]
            self.object_store.write_object(
                object_path=self.branch_index,
                content=[b for b in self.branches]
            )
            return True
        else:
            return False

    @staticmethod
    def load_viztrail(base_path, object_store=None):
        """Load all viztrail resources from given object store.

        Parameters
        ----------
        base_path: string
            Identifier for folder containing viztrail resources
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources

        Returns
        -------
        vizier.viztrail.driver.os.viztrail.OSViztrailHandle
        """
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # Load viztrail metadata
        metadata = object_store.read_object(
            object_store.join(base_path, OBJ_METADATA)
        )
        identifier = metadata['id']
        created_at = to_datetime(metadata['createdAt'])
        exec_env_id = metadata['env']
        # Load active branches. The branch index resource contains a list of
        # active branch identifiers.
        branch_index = object_store.join(base_path, OBJ_BRANCHINDEX)
        branch_folder = object_store.join(base_path, FOLDER_BRANCHES)
        modules_folder = object_store.join(base_path, FOLDER_MODULES)
        branches = list()
        for identifier in object_store.read_object(branch_index):
            branches.append(
                OSBranchHandle.load_branch(
                    identifier=identifier,
                    base_path=object_store.join(branch_folder, identifier),
                    modules_folder=modules_folder,
                    object_store=object_store
                )
            )
        # Return handle for new viztrail
        return OSViztrailHandle(
            identifier=identifier,
            exec_env_id=exec_env_id,
            properties=PersistentAnnotationSet(
                object_path=object_store.join(base_path, OBJ_PROPERTIES),
                object_store=object_store
            ),
            branches=branches,
            created_at=created_at,
            base_path=base_path,
            object_store=object_store,
            branch_index=branch_index,
            branch_folder=branch_folder,
            modules_folder=modules_folder
        )
