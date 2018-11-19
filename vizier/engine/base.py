class WorkflowEngine(object):

    def __init__(self, viztrails):
        self.viztrails = viztrails

    def append_workflow_module(self, branch, workflow, command, before_id=None):
        """Append a module to a given workflow. The module is appended to the workflow that is identified by the given version number.
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
        command : vizier.workflow.module.ModuleCommand, optional
            Specification of the command that is to be evaluated
        before_id : int, optional
            Insert new module before module with given identifier. Append at end
            of the workflow if negative

        Returns
        -------
        vizier.viztrail.workflow.WorkflowHandle
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
        if before_id is None:
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

    def create_branch(
        self, viztrail_id, source_branch=None, workflow_id=None,
        module_id=None, properties=None
    ):
        """Create a new branch for a given viztrail. The new branch is created
        from the specified workflow in the source branch starting at the module
        with module_id.

        If the source branch identifier is None the default branch is used. If
        the workflow identifier is None the head of the sourcebranch is used. If
        the module identifier is None the new branch starts after the last
        module of the source workflow.

        The new branch includes all modules from the source workflow that occur
        before and including the module with module_id.

        Returns the handle for the new branch or None if the given viztrail does
        not exist. Raises ValueError if (1) the source branch does not exist,
        (2) the given workflow identifier is unknown, or (3) no module with the
        given identifier exists.

        Parameters
        ----------
        viztrail_id : string
            Unique viztrail identifier
        source_branch : string, optional
            Unique branch identifier for existing source branch
        workflow_id: string, optional
            Unique identifier of the workflow that is being modified.
        module_id: string, optional
            Unique identifier of the last module from the source workflow that
            is included in the new branch.
        properties: dict, optional
            Set of properties for the new branch

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
        # exist or is empty
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
