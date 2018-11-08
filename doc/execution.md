Each workflow module has a state that can take one of five values:

SCHEDULED: The module is waiting to run
ACTIVE: The module is currently being executed.
CANCELED: Module execution has been canceled
ERROR: Module execution was aborted due to an error
SUCCESS: Module executed successfully

Every successfully executed module has a list of resources (unique resource identifier) for those resources the model execution depended on (i.e., accesses as input by the module). Only if these resources change the module needs to be re-executed when the workflow is updated.


The workflow engine executes workflows. We have one separate engine for each execution environment type.
