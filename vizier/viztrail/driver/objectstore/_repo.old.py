


from vizier.config import ENGINEENV_TEST
from vizier.core.properties import FilePropertiesHandler
from vizier.core.system import build_info, component_descriptor
from vizier.core.timestamp import get_current_time, to_datetime
from vizier.workflow.branch import ViztrailBranch, BranchProvenance
from vizier.workflow.base import VersionDescriptor, WorkflowHandle
from vizier.workflow.base import DEFAULT_BRANCH, DEFAULT_BRANCH_NAME
from vizier.workflow.base import ACTION_CREATE, ACTION_DELETE, ACTION_INSERT, ACTION_REPLACE
from vizier.workflow.command import PACKAGE_SYS, SYS_CREATE_BRANCH
from vizier.workflow.engine.viztrails import DefaultViztrailsEngine
from vizier.workflow.module import ModuleHandle
from vizier.workflow.repository.base import CachedViztrailRepository
from vizier.workflow.viztrail import ViztrailHandle


"""Files containing viztrail information."""
PROPERTIES_FILE = 'properties.yaml'
PROVENANCE_FILE = 'provenance.yaml'
VIZTRAIL_FILE = 'viztrail.yaml'





class FileSystemBranchProvenance(BranchProvenance):
    """Branch provenance object for provenance that is maintained in a Yaml file
    on the local file system.
    """
    def __init__(self, file_name):
        """Read provanence information from file (if exists). Note that the
        file may not exist if the branch is the default branch for which no
        provenance information exists.

        Parameters
        ----------
        file_name: string
            Name of the file that contains provenance information.
        """
        # Initialize a 'empty' provenance object (for the master branch)
        super(FileSystemBranchProvenance, self).__init__(None, -1, -1)
        if os.path.isfile(file_name):
            with open(file_name, 'r') as f:
                doc = yaml.load(f.read())
            self.source_branch = doc['branch']
            self.workflow_version = doc['workflow']
            self.module_id = doc['module']

    @staticmethod
    def to_file(file_name, source_branch, workflow_version, module_id):
        """Write provenance information to file.

        Parameters
        ----------
        source_branch : string
            Unique branch identifier of source branch
        workflow_version: int
            Version number of source workflow.
        module_id: int
            Identifier of module at which the new branch started
        """
        doc = {
            'branch': source_branch,
            'workflow': workflow_version,
            'module': module_id
        }
        with open(file_name, 'w') as f:
            yaml.dump(doc, f, default_flow_style=False)


class FileSystemViztrailHandle(ViztrailHandle):
    """Extend the default viztrail handle with functionality to persist the
    viztrail state on the file system.

    All information about a viztrail is stored in several files within a
    dedicated directory. All information is stored in Yaml format. The following
    files are created:

    - viztrail.yaml: Viztrail state
    - properties.yaml: Viztrail properties
    - <branch-id>_properties.yaml: For each branch a properties file that is
      prefixed by the branch identifier is created. The branch information
      itself (i.e., list of workflow versions in the branch) is maintained as
      part of the viztrail state
    - <branch-id>_provanence.yaml: For each branch provenance information is
      kept in a file prefixed by the branch identifier.
    - <version-identifier>.yaml: For each workflow a separate file containing
      the workflow module specification and generated outputs is created.
    """
    def __init__(
        self, identifier, branches, exec_env_id, properties, created_at=None,
        last_modified_at=None, version_counter=0, module_counter=0, fs_dir=None
    ):
        """Initialize the viztrail handle. Raise a ValueError exception if no
        base directory is given.

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
        fs_dir: string
            Base directory where all viztrail information is stored
        """
        super(FileSystemViztrailHandle, self).__init__(
            identifier=identifier,
            branches=branches,
            exec_env_id=exec_env_id,
            properties=properties,
            created_at=created_at,
            last_modified_at=last_modified_at,
            version_counter=version_counter,
            module_counter=module_counter
        )
        # Ensure that the base directory is not None
        if fs_dir is None:
            raise ValueError('missing base directory for viztrail')
        self.fs_dir = fs_dir

    @staticmethod
    def create_viztrail(fs_dir, identifier, exec_env_id, properties=None):
        """Create a new viztrail handle.

        Parameters
        ----------
        fs_dir: string
            Base directory where all viztrail information is stored
        identifier: string
            Unique viztrail identifier
        exec_env_id: string
            Identifier of the execution environment that is used for the
            viztrail
        properties: dict, optional
            Optional dictionary of viztrail properties

        Returns
        -------
        vizier.workflow.repository.fs.FileSystemViztrailHandle
        """
        # Create the master branch
        master_branch = ViztrailBranch(
            DEFAULT_BRANCH,
            FilePropertiesHandler(
                branch_file(fs_dir, DEFAULT_BRANCH),
                properties={'name': DEFAULT_BRANCH_NAME}
            ),
            FileSystemBranchProvenance(branch_prov_file(fs_dir, DEFAULT_BRANCH))
        )
        # Create viztrail and write to file system
        viztrail = FileSystemViztrailHandle(
            identifier,
            {DEFAULT_BRANCH: master_branch},
            exec_env,
            FilePropertiesHandler(
                os.path.join(fs_dir, PROPERTIES_FILE),
                properties=properties
            ),
            fs_dir=fs_dir
        )
        viztrail.to_file()
        # Return the new viztrail handle
        return viztrail

    def delete(self):
        """Delete the viztrail by removing the base directory."""
        shutil.rmtree(self.fs_dir)

    @staticmethod
    def from_file(fs_dir, envs):
        """Read the viztrail state from file.

        Raises IOError if the viztrail file does not exist.

        Parameters
        ----------
        fs_dir: string
            Base directory where all viztrail information is stored
        envs: dict(string: vizier.config.ExecEnv)
            Dictionary of workflow execution environments

        Returns
        -------
        vizier.workflow.repository.fs.FileSystemViztrailHandle
        """
        # Read vizrail information for file (in Yaml format)
        with open(os.path.join(fs_dir, VIZTRAIL_FILE), 'r') as f:
            doc = yaml.load(f.read())
        # Read information about viztrail branches
        return FileSystemViztrailHandle(
            doc['id'],
            {b['id'] : ViztrailBranch(
                b['id'],
                FilePropertiesHandler(branch_file(fs_dir, b['id'])),
                FileSystemBranchProvenance(branch_prov_file(fs_dir, b['id'])),
                workflows=[VersionDescriptor.from_dict(v) for v in b['versions']]
            ) for b in doc['branches']},
            envs[doc['env']],
            FilePropertiesHandler(os.path.join(fs_dir, PROPERTIES_FILE)),
            to_datetime(doc['timestamps']['createdAt']),
            to_datetime(doc['timestamps']['lastModifiedAt']),
            doc['versionCounter'],
            doc['moduleCounter'],
            fs_dir
        )

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
        """
        # Return None if branch does not exist
        if not branch_id in self.branches:
            return None
        branch = self.branches[branch_id]
        if version <= 0 and len(branch.workflows) == 0:
            # Returns an empty workflow if the branch does not contain any
            # executed workflows yet.
            return WorkflowHandle(branch_id, -1, get_current_time(), [])
        # Get version number of branch HEAD if negative version is given
        wf_file = None
        if version < 0 and len(branch.workflows) > 0:
            wf_file = workflow_file(self.fs_dir, branch.workflows[-1].version)
        else:
            for wf_desc in branch.workflows:
                if wf_desc.version == version:
                    wf_file = workflow_file(self.fs_dir, version)
                    break
        # Return None if version number is not in branch (indicated by an non-
        # existing workflow file)
        if wf_file is None:
            return None
        # Read workflow handle from file
        with open(wf_file, 'r') as f:
            doc = yaml.load(f.read())
        return WorkflowHandle(
            branch_id,
            doc['version'],
            to_datetime(doc['createdAt']),
            [ModuleHandle.from_dict(m) for m in doc['modules']]
        )

    def to_file(self):
        """Write the current state of the viztrail to file. Sets the last
        modified at timestamp to the current time.
        """
        self.last_modified_at = get_current_time()
        # Serialize viztrail
        doc = {
            'id': self.identifier,
            'env': self.exec_env.identifier,
            'branches' : [{
                    'id': b,
                    'versions': [w.to_dict() for w in self.branches[b].workflows]
                } for b in self.branches
            ],
            'timestamps' : {
                'createdAt' : self.created_at.isoformat(),
                'lastModifiedAt' : self.last_modified_at.isoformat()
            },
            'versionCounter': self.version_counter.value,
            'moduleCounter': self.module_counter.value
        }
        # Write viztrail serialization to file
        with open(os.path.join(self.fs_dir, VIZTRAIL_FILE), 'w') as f:
            yaml.dump(doc, f, default_flow_style=False)

    def write_workflow(self, exec_result):
        """Write workflow execution result into a new workflow file.

        Parameters
        ----------
        exec_result: vizier.workflow.engine.base.WorkflowExecutionResult
            Resulting workflow state after execution

        Returns
        -------
        datetime.datetime
        """
        # Create dictionary for workflow information
        created_at = get_current_time()
        doc = {
            'version': exec_result.version,
            'createdAt': created_at.isoformat(),
            'modules': [m.to_dict() for m in exec_result.modules]
        }
        # Write handle to file
        with open(workflow_file(self.fs_dir, exec_result.version), 'w') as f:
            yaml.dump(doc, f, default_flow_style=False)
        return created_at


class FileSystemViztrailRepository(CachedViztrailRepository):
    """Default implementation of the abstract viztrails repository class. This
    implementation uses the file system to maintain information about viztrails.
    All available viztrails information is maitained in an internal cache to
    avoid frequent IO operations when accessing viztrail inforamtion.
    """
    def __init__(self, base_directory, envs):
        """Initialize the base directory and the dictionary of workflow
        execution environments.

        The base directory is created if it does not exist. Viztrail information
        is maintained in subfolders of the base directory. For each viztrail one
        subfolder is created. There shold be no other folders in the base
        directory other than those containing information about a viztrail.

        Parameters
        ---------
        base_directory : string
            Path to base directory
        envs : dict(string: vizier.config.ExecEnv)
            Dictionary of supported execution environments
        """
        super(FileSystemViztrailRepository, self).__init__(
            build_info('FileSystemViztrailRepository')
        )
        self.base_dir = os.path.abspath(base_directory)
        # Create base directory if it doesn't exist
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)
        # Set list of workflow execution environments
        self.envs = envs
        # Read information about all available viztrails into an internal cache.
        # The cache is a dictionary of viztrail handles (keyes by their
        # identifier). Assumes that every directory in the base dir represents
        # a viztrail
        self.cache = dict()
        for filename in os.listdir(self.base_dir):
            fs_dir = os.path.join(self.base_dir, filename)
            if os.path.isdir(fs_dir):
                viztrail = FileSystemViztrailHandle.from_file(
                    fs_dir,
                    self.envs
                )
                self.cache[viztrail.identifier] = viztrail

    def components(self):
        """List containing component descriptor.

        Returns
        -------
        list
        """
        return [component_descriptor('viztrails', self.system_build())]

    def create_branch(self, viztrail_id, source_branch=DEFAULT_BRANCH, workflow_version=-1, properties=None, module_id=-1):
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
        viztrail_id : string
            Unique viztrail identifier
        source_branch : string, optional
            Unique branch identifier for existing branch
        workflow_version: int, optional
            Version number of the workflow that is being modified. If negative
            the branch head is being used.
        properties: dict, optional
            Set of properties for the new branch
        module_id: int, optional
            Start branch from module with given identifier in source_branch.
            The new branch starts at the end of the source branch if module_id
            has a negative value.

        Returns
        -------
        vizier.workflow.base.ViztrailBranch
        """
        # Get viztrail. Return None if the viztrail does not exist
        if not viztrail_id in self.cache:
            return None
        viztrail = self.cache[viztrail_id]
        # Raise exception if source branch does not exist
        if not source_branch in viztrail.branches:
            raise ValueError('unknown branch \'' + source_branch + '\'')
        # Get the referenced workflow. Raise exception if the workflow does not
        # exist oris empty
        workflow = viztrail.get_workflow(source_branch, workflow_version)
        if workflow is None:
            raise ValueError('unknown workflow')
        if len(workflow.modules) == 0:
            raise ValueError('attempt to branch from empty workflow')
        # Copy list of workflow modules depending on value of module_id
        if module_id < 0:
            modules = workflow.modules
        else:
            modules = []
            found = False
            for m in workflow.modules:
                modules.append(m)
                if m.identifier == module_id:
                    found = True
                    break
            if not found:
                raise ValueError('unknown module \'' + str(module_id) + '\'')
        # Make a copy of the source workflow for the branch
        result = viztrail.engine.copy_workflow(
            viztrail.version_counter.inc(),
            modules
        )
        # Create file for new workflow
        created_at = viztrail.write_workflow(result)
        # Create new branch handle
        target_branch = get_unique_identifier()
        # Store provenance information for new branch in file
        prov_file = branch_prov_file(viztrail.fs_dir, target_branch)
        FileSystemBranchProvenance.to_file(
            prov_file,
            source_branch,
            workflow.version,
            result.modules[-1].identifier
        )
        branch = ViztrailBranch(
            target_branch,
            FilePropertiesHandler(
                branch_file(viztrail.fs_dir, target_branch),
                properties
            ),
            FileSystemBranchProvenance(prov_file),
            workflows=[VersionDescriptor(
                result.version,
                action=ACTION_CREATE,
                package_id=PACKAGE_SYS,
                command_id=SYS_CREATE_BRANCH,
                created_at=created_at
            )]
        )
        # Update the viztrail on disk
        viztrail.branches[target_branch] = branch
        viztrail.to_file()
        return branch

    def create_viztrail(self, env_id, properties):
        """Create a new viztrail.

        Raises ValueError if the given execution environment is unknown.

        Parameters
        ----------
        env_id: string
            Identifier for workflow execution environment that is used fot the
            new viztrail
        properties: dict
            Set of properties for the new viztrail

        Returns
        -------
        vizier.workflow.base.ViztrailHandle
        """
        if not env_id in self.envs:
            raise ValueError('unknown execution environment \'' + env_id + '\'')
        # Get unique viztrail identifier
        identifier = get_unique_identifier()
        # Create viztrail directory
        fs_dir = os.path.join(self.base_dir, identifier)
        os.makedirs(fs_dir)
        # Create new viztrail and add to cache
        viztrail = FileSystemViztrailHandle.create_viztrail(
            fs_dir,
            identifier,
            self.envs[env_id],
            properties=properties
        )
        self.cache[viztrail.identifier] = viztrail
        return viztrail

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
        # Raise exception if branch is the default branch
        if branch_id == DEFAULT_BRANCH:
            raise ValueError('attempt to delete default viztrail branch')
        # Get viztrail. Return None if it doen't exist
        if not viztrail_id in self.cache:
            return None
        viztrail = self.cache[viztrail_id]
        # Get viztrail branch. Return None if branch does not exist
        if not branch_id in viztrail.branches:
            return None
        branch = viztrail.branches[branch_id]
        # Delete workflow files associated with the branch
        for wf_desc in branch.workflows:
            os.remove(workflow_file(viztrail.fs_dir, wf_desc.version))
        # Delete branch properties file
        os.remove(branch_file(viztrail.fs_dir, branch_id))
        # Update the viztrail information
        del viztrail.branches[branch_id]
        viztrail.to_file()
        return viztrail


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def branch_file(fs_dir, branch_id):
    """Get file for branch properties.

    Parameters
    ----------
    fs_dir: string
        Base directory for viztrail
    branch_id: string
        Unique branch identifier

    Returns
    -------
    string
    """
    return os.path.join(fs_dir, branch_id + '_' + PROPERTIES_FILE)


def branch_prov_file(fs_dir, branch_id):
    """Get file for branch provenance information.

    Parameters
    ----------
    fs_dir: string
        Base directory for viztrail
    branch_id: string
        Unique branch identifier

    Returns
    -------
    string
    """
    return os.path.join(fs_dir, branch_id + '_' + PROVENANCE_FILE)

def persist_workflow_result(viztrail, branch_id, result, action=None, package_id=None, command_id=None):
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
    created_at = viztrail.write_workflow(result)
    viztrail.branches[branch_id].workflows.append(
        VersionDescriptor(
            result.version,
            action=action,
            package_id=package_id,
            command_id=command_id,
            created_at=created_at
        )
    )
    viztrail.to_file()
    return viztrail


def workflow_file(fs_dir, version):
    """Get file for viztrail workflow.

    Parameters
    ----------
    fs_dir: string
        Base directory for viztrail
    version: int
        Unique version identifier

    Returns
    -------
    string
    """
    return os.path.join(fs_dir, str(version) + '.yaml')
