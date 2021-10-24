# Worker failover
* Several Nodes can reference the same Worker
* Nodes use "worker" table to set an exclusive lock to only one of the Node's dispatcher be active at a time

# Worker failover with two Nodes
![Failover](doc/images/worker_failover.png)
