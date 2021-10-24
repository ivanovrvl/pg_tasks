# Features
* A task runs as an OS process
* Tasks queue with priority and process limit per node
* Fully database driven (a worker and task can be controlled through the table row)
* A task can be cancelled by changing its state in the table row (OS process terminated)
* A task can be scheduled at a time or with interval
* Uses postgres NOTYFY feature in opposite to pooling (instant changes discovery, less request)
* [Clustering is supported](doc/clustering.md)
* [Faiover is supported](doc/failover.md)

# Class diagramm
![Class diagramm](doc/images/classes.png)

# Task lifecycle
![Task lifecycle](doc/images/task_lifecycle.png)
