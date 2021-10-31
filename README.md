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

# Task management examples
Create new task  in the Draft state
```SQL
INSERT INTO long_task.task(group_id, state_id, priority, command)
VALUES (0, 'DR', 0, '{ping,-n,1,127.0.0.1}')
RETURNING id;
```
Queue draft task for ASAP executing by any worker in the group
```SQL
UPDATE long_task.task
SET state_id='AW', worker_id=null
WHERE id = <id>
```
Re-queue a completed task for ASAP executing by any worker in the group
```SQL
UPDATE long_task.task
SET state_id='AW', worker_id=null
WHERE id = <id> AND state_id like 'C%'
```
Cancel a queued or executing task
```SQL
UPDATE long_task.task
SET state_id='AC'
WHERE id = <id> AND state_id like 'A%'
```
Schedule existing task for each 2 hours from now (start will be skipped if state_id is not like 'C%')
```SQL
UPDATE long_task.task
SET 
	next_start=now(),
	shed_period_id='HOU',
	shed_period_count=2,
	shed_clone=false
WHERE id = <id>
```

# Installation
- git clone https://github.com/ivanovrvl/pg_tasks.git
- pip install -r requirements.txt
- Apply liquibase script db/changelog.xml: use update_db.bat for help (backup stored procedures customized before)
- Restore stored procedures customized, adotp them if needed
- Change db_config.json, config.py if needed
- Run Nodes with appropriate command line parameters (see config.py):\
  python node.py [<worker_id> [<group_id> [<max_task_count> [<node_name>]]]]

# Some perfomance tests [here](doc/test_results.md)
