# Vizier API Command Line Interface

The **Vizier Web API Command Line Interface** allows a user to interact with a running instance of the Vizier web API from the command line of a terminal on their computer. While this is by no means the preferred and intended interface for Vizier it has proven to be a helpful tool during development. It also allows quick and simple manipulations of notebooks and dataset downloads without the need for a web browser.


## Install and Run

The command line interface is included as part of the vizier web-api-async repository. See the installations instructions in the README file for the repository. After cloning the repository, change the current working directory to the directoy that contains the cloned web-api-async repository. You can run the command line interface either either by typing `python vizier` followind by the command that you want to execute or using the simple shell script that is included in the tools folder of the repository. In order to use the shell script include that path to the tools folder in your global PATH variable, e.g.,

```
export PATH=$PATH:./tools
```

## Configuration

The command line interface requires the URL of the Vizier web API that it interacts with. To initialize the URL use the `vizier init` command:

```
vizier init {<url>}
```

If no URL is given it is assumed that the web service is running at the default URL `http://localhost:5000/vizier-db/api/v1`.

## Commands

The `vizier help` command prints a list of the available commands. At this point only the `plot`, `python`, and `vizual` packages for notebooks are supported.
```
vizier help

Vizier Command Line Interface - Version 0.1.0

Settings
  defaults
  set project <project-id>
  set branch <branch-id>

Projects
  create branch <name>
  create branch <name> from module <module-id>
  create branch <name> from workflow <workflow-id> module <module-id>
  create branch <name>
  create project <name>
  delete branch <branch-id>
  delete project <project-id>
  list branches
  list projects
  rename branch <name>
  rename project <name>
  show [history | notebooks]

Notebooks
  [notebook | nb] append <command>
  [notebook | nb] cancel
  [notebook | nb] delete <module-id>
  [notebook | nb] insert before <module-id> <command>
  [notebook | nb] replace <module-id> <command>
  show chart <name> {in <module-id>}
  show [notebook | nb] {<workflow-id>}

Datasets
  download dataset <name> {in <module-id>} to <target-path>
  show dataset <name> {in <module-id>}

Commands
  chart <name> on <dataset> with <column:label:start-end> ...
  delete column <name> from <dataset>
  delete row <row-index> from <dataset>
  drop dataset <dataset>
  filter <column-1>{::<new-name>} ... from <dataset>
  insert column <name> into <dataset> at position <column-index>
  insert row into <dataset> at position <row-index>
  load <name> from file <file>
  load <name> from url <url>
  move column <name> in <dataset> to position <column-index>
  move row <row-index> in <dataset> to position <target-index>
  python [<script> | <file>]
  rename column <name> in <dataset> to <new-name>
  rename dataset <dataset> to <new-name>
  sort <dataset> by <column-1>{::[DESC|ASC]} ...
  update <dataset-name> <column-name> <row-index>{ <value>}
```

### Settings

The `set project` and `set branch` commands are required to specify the default project and branch that the notebook and dataset commands operate on. To list the current settings use `defaults`.
