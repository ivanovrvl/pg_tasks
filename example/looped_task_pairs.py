import os
import sys
import json
import time
import random
import psycopg2
from psycopg2.extras import Json

if True:

    task_id = int(sys.argv[1])

    min_delay = float(sys.argv[2]) if len(sys.argv) >= 3 else None
    max_delay = float(sys.argv[3]) if len(sys.argv) >= 3 else None

    task_id2 = ((task_id + 1) // 2) * 2
    if task_id2 == task_id:
        task_id2 -= 1

    if min_delay is not None:
        time.sleep(random.random() * (max_delay - min_delay) + min_delay)

    with open('db_config.json', 'r') as f:
        db_config = json.load(f)

    conn = psycopg2.connect(host=db_config['host'], dbname=db_config['database'], user=db_config['user'], password=db_config['password'])
    try:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            sql = "SELECT worker_id, params FROM long_task.task WHERE id=%s"
            cur.execute(sql, (task_id,))
            row = cur.fetchone()
            worker_id = str(row[0])
            par = row[1]

            if par is None:
                par = {worker_id : 1}
            else:
                w = par.get(worker_id)
                if w is None:
                    par[worker_id] = 1
                else:
                    par[worker_id] = 1 + int(w)

            sql = "UPDATE long_task.task SET state_id='CS', params=%s where id=%s and state_id like %s"
            #sql = "UPDATE long_task.task SET params=%s where id=%s and state_id like %s"
            cur.execute(sql, (Json(par), task_id, 'AE'))

            sql = "UPDATE long_task.task SET state_id='AW', worker_id=NULL where id=%s and state_id like %s"
            cur.execute(sql, (task_id2, 'C%'))

    finally:
        conn.close()

#time.sleep(0.1)