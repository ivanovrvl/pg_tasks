# Features
* A task runs as an OS process
* Tasks queue with priority and process limit per node
* Fully database driven (a workers and tasks are controlled through the table rows)
* A task can be cancelled ([see task lifecycle](doc/images/task_lifecycle.png))
* A task can be scheduled at a time or with interval
* Uses postgres NOTIFY feature in opposite to pooling => instant changes discovery, less request
* [Clustering is supported](doc/clustering.md)
* [Faiover is supported](doc/failover.md)

# Class diagramm
![Class diagramm](doc/images/classes.png)

# Task lifecycle
![Task lifecycle](doc/images/task_lifecycle.png)
