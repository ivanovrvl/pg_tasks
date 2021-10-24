# Features
* A task runs as an OS process
* Tasks queue with priority and process limit per node are supported
* Fully database driven (a workers and tasks are controlled through the table rows)
* A task can be cancelled result in the OS process killed ([see task lifecycle](doc/images/task_lifecycle.png))
* A task can be scheduled at a time or with interval
* Uses postgres NOTIFY feature in opposite to pooling => instant changes discovery, less requests
* [Clustering is supported](doc/clustering.md)
* [Faiover is supported](doc/failover.md)

# Class diagramm
![Class diagramm](doc/images/classes.png)

# Task lifecycle
![Task lifecycle](doc/images/task_lifecycle.png)
# Installation
- Apply liquibase script db/changelog.xml: use update_db.bat for help (backup stored procedures customized before)
- Restore stored procedures customized, adotp them if needed
- Change db_config.json, config.py if needed
- Run Nodes with appropriate command line parameters (see config.py):\
  python node.py [<worker_id> [<group_id> [<max_task_count> [<node_name>]]]]

