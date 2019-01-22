# Vizier Web API Command Line Interface

The Vizier Web API Command Line Interface allows a user to interact with a running instance of the Vizier Web API from the command line. While this is by no means the preferred and intended interface for Vizier it has proven to be a helpful tool during development.

## Install and Run

To setup the Python environment clone the repository and run the following commands:

```
git clone https://github.com/VizierDB/vizier-cli.git
cd vizier-cli
conda create --name vcli pip
source activate vcli
pip install -r requirements.txt
pip install -e .
```

To run the command line add the vizier script in the bin folder to your PATH, e.g.,

```
export PATH=$PATH:./bin
```

### Configuration

```
vizier init
```


### Help

```
$> vizier help

Vizier Command Line Interface - Version 0.1.0

Settings
  defaults
  set project <project-id>
  set branch <branch-id>

Projects
  create branch <name>
  create project <name>
  delete branch <branch-id>
  delete project <project-id>
  list branches
  list projects
  rename branch <name>
  rename project <name>
  show [history | notebooks]
  show notebook {<workflow-id>}

Notebooks
  [notebook | nb] append <cmd>
  [notebook | nb] cancel
  [notebook | nb] delete <module-id>
  [notebook | nb] insert before <module-id> <cmd>
  [notebook | nb] replace <module-id> <cmd>
  show chart <name> {in <module-id>}
  show dataset <name> {in <module-id>}

Commands
  chart <chart-name> on <dataset-name> with <column-name:label:start-end> ...
  load <name> from file <file>
  load <name> from url <url>
  python [<script> | <file>]
  update <dataset-name> <column-name> <row-index>{ <value>}
```
