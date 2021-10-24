# Worker failover
* Several Nodes can reference the same Worker
* Nodes use "worker" table to set an exclusive lock to only one of the Node's dispatcher be active at a time
* Workers observer of a Node reads periodicically all worker table rows to detect any worker's fail state (see below). Recover procedure is lunched for each worker failed.
# Worker failover with two Nodes
![Failover](images/worker_failover.png)
# Worker fail state detection and recovery
Worker is in fail state when (worker.locked_until > now() and worker.active) is true.\
Worker observer running on each Node (group_id, worker_id don`t matter) lunches recovery procedure for any worker for which (worker.locked_until + config.failed_worker_recovery_delay > now() and worker.active) is true.\
Recovery procedure does:
- aquires exclusive lock for the worker
- calls recover_worker_tasks(worker_id) stored procedure that must re-queue or complete Worker`s Tasks
- resets worker.active flag, sets worker.locked_until = NULL
- releases lock
