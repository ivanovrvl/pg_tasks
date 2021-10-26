# Test conditions
Two VMs:
1) Postgres 12, 6 CPU, 8 Gb RAM, SSD
2) Two Nodes, 4 CPU, 4Gb RAM, HDD

# Test 1
Two tasks start each other based on example/looped_task_pairs.py script. \
The results: 7 loop cycles per second => 14 python processes making 3 DB requests each per second

# Test 2
1000 Waiting tasks to run python making 1 DB call \
max_task_count=50 \
The results: 30 python processes per second
