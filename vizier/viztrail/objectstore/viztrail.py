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

"""Implementation for a viztrail handle that maintains all resources as objects
and folders in an object store.
"""

from vizier.core.annotation.persistent import PersistentAnnotationSet
from vizier.core.io.base import DefaultObjectStore
from vizier.core.timestamp import get_current_time, to_datetime
from vizier.core.util import init_value
from vizier.viztrail.objectstore.branch import OSBranchHandle
from vizier.viztrail.base import ViztrailHandle, PROPERTY_NAME
from vizier.viztrail.branch import BranchProvenance, DEFAULT_BRANCH


"""Resource identifier"""
FOLDER_BRANCHES = 'branches'
FOLDER_MODULES = 'modules'
OBJ_BRANCHINDEX = 'active'
OBJ_METADATA = 'viztrail'
OBJ_PROPERTIES = 'properties'

"""Json labels for serialized object."""
KEY_CREATED_AT = 'createdAt'
KEY_DEFAULT = 'isDefault'
KEY_IDENTIFIER = 'id'

class OSViztrailHandle(ViztrailHandle):
    """Handle for viztrail that maintains resources as folders and objects in
    an object store.

    Modules are maintained in a subfolder of the viztrails base folder because
    modules may be shared between several workflows. Workflows, on the other
    hand, are specific to the branch they occur in and are maintained withing
    the respective branch subfolder,

    Folders and Resources
    ---------------------
    branches/active : List of active branches
    properties      : Viztrail annotations
    viztrail        : Viztrail metadata (identifier, timestamp, environment)
    branches/       : Viztrail branches
    modules/        : Modules in viztrail workflows
    """
    def __init__(
        self, identifier, properties, base_path, branches, default_branch,
        object_store=None, created_at=None, branch_index=None,
        branch_folder=None, modules_folder=None
    ):
        """Initialize the viztrail descriptor.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Handler for user-defined properties
        base_path: string
            Identifier for folder containing viztrail resources
        object_store: vizier.core.io.base.ObjectStore, optional
            Object store implementation to access and maintain resources
        branches: list(vizier.viztrail.branch.BranchHandle)
            List of branches in the viztrail
        default_branch: vizier.viztrail.branch.BranchHandle
            Default branch for the viztrail
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
            properties=properties,
            branches=branches,
            default_branch=default_branch,
            created_at=created_at
        )
        # Initizlize the object store and identifier for all subfolders.
        self.base_path = base_path
        self.object_store = init_value(object_store, DefaultObjectStore())
        self.branch_folder = init_value(branch_folder, self.object_store.join(base_path, FOLDER_BRANCHES))
        self.branch_index = init_value(branch_index, self.object_store.join(self.branch_folder, OBJ_BRANCHINDEX))
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
        vizier.viztrail.objectstore.branch.OSBranchHandle
        """
        branch = create_branch(
            provenance=provenance,
            properties=properties,
            modules=modules,
            branch_folder=self.branch_folder,
            modules_folder=self.modules_folder,
            object_store=self.object_store
        )
        # Add the new branch to index and materialize the updated index
        # information
        self.branches[branch.identifier] = branch
        write_branch_index(
            branches=self.branches,
            object_path=self.branch_index,
            object_store=self.object_store
        )
        return branch

    @staticmethod
    def create_viztrail(identifier, base_path, object_store=None, properties=None) :
        """Create a new viztrail resource. Will create the base directory for
        the viztrail.

        Creates subfolders for viztrail resources. Writes viztrail metadata and
        properties to file. Create an empty default branch

        Parameters
        ----------
        properties: dict
            Set of properties for the new viztrail
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
        # Create empty index file and subfolders for branches, workflows, and
        # modules. The base path folder is expected to exist.
        branch_folder = object_store.join(base_path, FOLDER_BRANCHES)
        object_store.create_folder(base_path, identifier=FOLDER_BRANCHES)
        branch_index = object_store.join(branch_folder, OBJ_BRANCHINDEX)
        object_store.write_object(object_path=branch_index, content=list())
        modules_folder = object_store.join(base_path, FOLDER_MODULES)
        object_store.create_folder(base_path, identifier=FOLDER_MODULES)
        # Write viztrail metadata to disk
        created_at = get_current_time()
        object_store.write_object(
            object_path=object_store.join(base_path, OBJ_METADATA),
            content={
                KEY_IDENTIFIER: identifier,
                KEY_CREATED_AT: created_at.isoformat()
            }
        )
        # Create the default branch for the new viztrail
        default_branch = create_branch(
            provenance=BranchProvenance(created_at=created_at),
            properties={PROPERTY_NAME: DEFAULT_BRANCH},
            modules=None,
            branch_folder=branch_folder,
            modules_folder=modules_folder,
            object_store=object_store,
            is_default=True,
            created_at=created_at
        )
        # Materialize the updated branch index
        write_branch_index(
            branches={default_branch.identifier: default_branch},
            object_path=branch_index,
            object_store=object_store
        )
        # Return handle for new viztrail
        return OSViztrailHandle(
            identifier=identifier,
            properties=PersistentAnnotationSet(
                object_path=object_store.join(base_path, OBJ_PROPERTIES),
                object_store=object_store,
                annotations=properties
            ),
            branches=[default_branch],
            default_branch=default_branch,
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

        Raises ValueError if the given branch is the default branch.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier

        Returns
        -------
        bool
        """
        # Raise an exception is an attempt is made to delete the default branch
        if self.default_branch.identifier == branch_id:
            raise ValueError('cannot delete default branch')
        if branch_id in self.branches:
            # Call the delete method of the branch and update the branch index
            self.branches[branch_id].delete_branch()
            del self.branches[branch_id]
            write_branch_index(
                branches=self.branches,
                object_path=self.branch_index,
                object_store=self.object_store
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
        identifier = metadata[KEY_IDENTIFIER]
        created_at = to_datetime(metadata[KEY_CREATED_AT])
        # Load active branches. The branch index resource contains a list of
        # active branch identifiers.
        branch_folder = object_store.join(base_path, FOLDER_BRANCHES)
        branch_index = object_store.join(branch_folder, OBJ_BRANCHINDEX)
        modules_folder = object_store.join(base_path, FOLDER_MODULES)
        branches = list()
        default_branch = None
        for b in object_store.read_object(branch_index):
            branch_id = b[KEY_IDENTIFIER]
            is_default = b[KEY_DEFAULT]
            branches.append(
                OSBranchHandle.load_branch(
                    identifier=branch_id,
                    is_default=is_default,
                    base_path=object_store.join(branch_folder, branch_id),
                    modules_folder=modules_folder,
                    object_store=object_store
                )
            )
            if is_default:
                default_branch = branches[-1]
        # Return handle for new viztrail
        return OSViztrailHandle(
            identifier=identifier,
            properties=PersistentAnnotationSet(
                object_path=object_store.join(base_path, OBJ_PROPERTIES),
                object_store=object_store
            ),
            branches=branches,
            default_branch=default_branch,
            created_at=created_at,
            base_path=base_path,
            object_store=object_store,
            branch_index=branch_index,
            branch_folder=branch_folder,
            modules_folder=modules_folder
        )

    def set_default_branch(self, branch_id):
        """Set the branch with the given identifier as the default branch.
        Raises ValueError if no branch with the given identifier exists.

        Return the branch handle for the new default.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        if not branch_id in self.branches:
            raise ValueError('unknown branch \'' + str(branch_id) + '\'')
        branch = self.branches[branch_id]
        # Replace the current default branch
        self.default_branch.is_default = False
        branch.is_default = True
        self.default_branch = branch
        # Write modified branch index
        write_branch_index(
            branches=self.branches,
            object_path=self.branch_index,
            object_store=self.object_store
        )
        return branch


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def create_branch(
    provenance, properties, modules, branch_folder, modules_folder,
    object_store, is_default=False, created_at=None
):
    """Create a new branch. If the list of workflow modules is given the list
    defines the branch head. Otherwise, the branch is empty.

    Parameters
    ----------
    provenance: vizier.viztrail.base.BranchProvenance
        Provenance information for the new branch
    properties: dict, optional
        Set of properties for the new branch
    modules: list(string), optional
        List of module identifier for the modules in the workflow at the
        head of the branch
    branch_folder: string
        Path to branches folder
    modules_folder: string
        Path to modules folder
    object_store: vizier.core.io.base.ObjectStore
        Object store implementation to access and maintain resources
    is_default: bool, optional
        True if this is the new default branch for the viztrail

    Returns
    -------
    vizier.viztrail.objectstore.branch.OSBranchHandle
    """
    # Get unique identifier for new branch by creating the subfolder that
    # will contain branch resources
    identifier = object_store.create_folder(branch_folder)
    branch_path = object_store.join(branch_folder, identifier)
    # Create materialized branch resource. This will raise an exception if
    # the list of modules contains an active module.
    try:
        branch = OSBranchHandle.create_branch(
            identifier=identifier,
            is_default=is_default,
            provenance=provenance,
            properties=properties,
            modules_folder=modules_folder,
            modules=modules,
            base_path=branch_path,
            object_store=object_store
        )
    except ValueError as ex:
        # Remove the created folder
        object_store.delete_folder(
            folder_path=branch_path,
            force_delete=True
        )
        raise ex
    return branch


def write_branch_index(branches, object_path, object_store):
    """Write index file for current branch set.

    Parameters
    ----------
    branches: dict(vizier.viztrail.objectstore.branch.OSBranchHandle)
        Current ser of branches in the viztrail
    object_path: string
        Path to branch index list
    object_store: vizier.core.io.base.ObjectStore
        Object store implementation to access and maintain resources
    """
    object_store.write_object(
        object_path=object_path,
        content=[{
                KEY_IDENTIFIER: b,
                KEY_DEFAULT: branches[b].is_default
            } for b in branches
        ]
    )
