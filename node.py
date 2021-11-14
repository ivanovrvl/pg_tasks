# This code is under MIT licence, you can find the complete file here: https://github.com/ivanovrvl/pg_tasks/blob/main/LICENSE

import json
import sys, os, select, time, datetime
import psycopg2.extensions
import subprocess
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from config import get_config, Messages

class AttrDict(dict):
    def __transform__(value):
        if isinstance(value, AttrDict):
            return value
        if isinstance(value, dict):
            return AttrDict(value)
        return value
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self
    def __setitem__(self, name, value):
        dict.__setitem__(self, name, AttrDict.__transform__(value))

config = AttrDict(get_config())

sys.path.append(os.path.abspath('py_active_objects'))
from active_objects import ActiveObjectWithRetries, ActiveObjectsController

controller = ActiveObjectsController(priority_count=2)

startMoreTasks = None
refreshTasks = None
refreshWorkers = None
worker = None # Node`s worker

conn = None # DB Connection

no_more_waiting_tasks: bool = False # no more AW tasks
executing_task_count:int = 0
locked_other_workers_count:int = 0

def get_msg(msg_const:str, default:str):
    res = messages.get(msg_const, default)

class TaskProcess:
    """
    OS process wrapper
    """
    def __init__(self, commands:list, id, cwd=None):
        cmds = []
        for c in commands:
            if c == '%TASK':
                cmds.append(str(id))
            else:
                cmds.append(c)
        self.id = id
        self.proc = subprocess.Popen(cmds, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd = cwd)
        #self.proc = subprocess.Popen(cmds, stdin=subprocess.PIPE, stdout=None, stderr=None, cwd = cwd)
        #self.proc = subprocess.Popen(cmds, stdin=subprocess.PIPE, cwd = cwd)

    def check_result(self):
        return self.proc.poll()

    def terminate(self):
        self.proc.terminate()

class CommonTask(ActiveObjectWithRetries):

    def __init__(self, controller, type_name = None, id = None):
        super().__init__(controller, type_name, id)
        self.min_retry_interval = config.min_task_retry_delay.total_seconds()
        self.max_retry_interval = config.max_task_retry_delay.total_seconds()

    def info(self, msg:str):
        if msg is not None:
            print(msg)

    def error(self, msg:str):
        if msg is not None:
            print(msg)

    def process_internal(self):
        try:
            super().process_internal()
        except Exception as e:
            self.error(str(e))
            if config.debug:
                raise e

class RefreshWorkers(CommonTask):
    """
    Periodically updates the states of each Worker from the DB, creates new ones if necessary
    """

    def __init__(self, controller):
        super().__init__(controller)
        self.next_refresh = None

    def process(self):
        if self.reached(self.next_refresh):
            self.next_refresh = self.controller.now() + config.workers_refresh_inverval
            self.schedule(self.next_refresh)
            self.info(Messages.REFRESH_WORKERS)
            with conn.cursor() as cur:
                sql = f"""
                    SELECT id,{','.join(Worker.table_fields)}
                    FROM {Worker.table_name}
                    """
                expected_ids = controller.get_ids(Worker.type_name)
                cur.execute(sql)
                refresh_db_states(cur, Worker, set(expected_ids))

    def refresh_all(self):
        self.next_refresh = None
        self.process()

class RefreshTasks(CommonTask):
    """
    Updates the Tasks loaded with the state from the base, if necessary, creates new ones
    takes into account all active tasks, tasks with the nearest start and loaded
    """

    def __init__(self, controller):
        super().__init__(controller)
        self.next_refresh = None

    def process(self):
        global no_more_waiting_tasks
        if self.reached(self.next_refresh):
            no_more_waiting_tasks = True
            self.info(Messages.REFRESH_TASKS)
            # update the status of tasks every 55 minutes
            # because we keep loaded only plans for the next hour
            self.next_refresh = self.controller.now() + timedelta(minutes=55)
            self.schedule(self.next_refresh)
            with conn.cursor() as cur:
                sql = f"""
                    SELECT id,{','.join(Task.table_fields)}
                    FROM {Task.table_name}
                    WHERE (
                    	group_id = %s
                        and (
                        	state_id like %s
                            or
                            (next_start is not null and next_start < now() + interval '1 hour')
                        )
                    ) or id = any(%s)
                    """
                expected_ids = controller.get_ids(Task.type_name)
                cur.execute(sql, (config.group_id, 'A%', expected_ids))
                refresh_db_states(cur, Task, set(expected_ids))

    def refresh_all(self):
        self.next_refresh = None
        self.process()

class DbObject(CommonTask):
    """
    A task object corresponding to the record in table_name table
    Identified by (type_name, id)
    """

    type_name = None
    table_name = None
    table_fields = None

    def info(self, msg:str):
        if msg is not None:
            super().info(self.type_name + str(self.id) + ': ' + msg)

    def error(self, msg:str):
        if msg is not None:
            super().error(self.type_name + str(self.id) + '! ' + msg)

    def __init__(self, controller, id = None):
        super().__init__(controller, self.type_name, id)
        self.db_state = None

    def set_db_state(self, state):
        self.db_state = state
        if state is not None:
            self.signal()

    def set_deleted(self):
        self.set_db_state({"deleted": True})

    @classmethod
    def clear_changes(cls):
        cls.changed.clear()
        cls.deleted.clear()

    @classmethod
    def add_change(cls, msg:str):
        if len(msg) > 2 and msg[1] == ' ' and msg[0] in ('I','U','D'):
            id = int(msg[2:])
            if msg[0] == 'D':
                cls.deleted.add(id)
            else:
                cls.changed.add(id)

    @classmethod
    def apply_changes(cls):
        with conn.cursor() as cur:
            sql = f"""
                SELECT id,{','.join(cls.table_fields)}
                FROM {cls.table_name}
                WHERE id = any(%s)
                """
            cur.execute(sql, (list(cls.changed.difference(cls.deleted)),))
            refresh_db_states(cur, cls)
            cls.changed.clear()

            for id in cls.deleted:
                task = controller.find(type_name, id)
                if task is not None:
                    task.set_deleted()

            cls.deleted.clear()

    def refresh_db_state(self):
        self.db_state = None
        with conn.cursor() as cur:
            sql = f"""
                SELECT id,{','.join(self.__class__.table_fields)}
                FROM {self.__class__.table_name}
                WHERE id = %s"""
            cur.execute(sql, (self.id,))
            refresh_db_states(cur, self.__class__, set([self.id]))

    @classmethod
    def find_or_new(cls, id) -> CommonTask:
        obj = controller.find(cls.type_name, id)
        if obj is None:
            obj = cls(controller, id)
        return obj

class Worker(DbObject):
    """
    worker record
    """
    type_name = "w"
    table_name = f"{config.schema}.worker"
    table_fields = ['active', 'locked_until', 'stop']
    notify_key = '!' + table_name

    changed = set()
    deleted = set()

    def __init__(self, controller, id = None):
        super().__init__(controller, id)
        self.lock_time = None
        self.stop = 0
        self.has_lock = False
        #self.state = 0

    #def set_state(self, val) -> bool:
    #    val = int(val)
    #    if val == self.state: return False
    #    old = self.state
    #    self.state = val
    #    return True

    def set_has_lock(self, val:bool) -> bool:
        global locked_other_workers_count
        if self.has_lock == val: return False
        self.has_lock = val
        if val:
            self.info(Messages.LOCK_AQUIRED)
            if self.id == config.worker_id:
                startMoreTasks.start_more()
                refreshTasks.refresh_all()
            else:
                locked_other_workers_count += 1
        else:
            self.info(Messages.LOCK_RELEASED)
            self.lock_time = None
            if self.id != config.worker_id:
                locked_other_workers_count -= 1
        return True

    def set_stop(self, val) -> bool:
        val = int(val)
        if self.stop == val: return False
        old = self.stop
        self.stop = val
        if self.id == config.worker_id:
            self.info(Messages.STOP_VAL.format(val))
            if old > 0 and val <= 0:
                startMoreTasks.start_more()
            elif old < 3 and val >= 3:
                self.controller.signal(Task.type_name)
            elif old >= 0 and val < 0:
                self.controller.signal(Task.type_name)
        return True

    def lock(self) -> bool:
        """
        Tries to get or prolongate the lock
        """
        with conn.cursor() as cur:
            lock_until = self.controller.now() + config.half_locking_time + config.half_locking_time
            sql = f"SELECT {config.schema}.lock_worker(%s,%s,%s,%s,%s,%s)"
            if self.id == config.worker_id:
                cur.execute(sql, (self.id, config.group_id, config.node_name, executing_task_count, lock_until, self.lock_time))
            else:
                cur.execute(sql, (self.id, -1, config.node_name, -1, lock_until, self.lock_time))
            res = cur.fetchone()[0]
            if res is None:
                self.db_state['locked_until'] = lock_until
                self.lock_time = lock_until
                self.set_has_lock(True)
            else:
                self.db_state['locked_until'] = res
                self.lock_time = None
                self.set_has_lock(False)

        return self.has_lock

    def keep_lock(self):
        """
        Aquires lock and prolongates it periodicically
        """
        if self.has_lock:
            t = self.db_state['locked_until']
            if t is None or t != self.lock_time:
                # somebody intercepted the lock
                self.error(Messages.LOCK_CATCHED)
                self.lock()

        if self.lock_time is None \
        or self.reached(self.lock_time - config.half_locking_time):
            if self.lock():
                t = self.lock_time - config.half_locking_time
            else:
                t = self.db_state['locked_until']
                if t is not None:
                    t = t + config.half_locking_time
                else:
                    t = self.controller.now() + config.half_locking_time
            if self.reached(t):
                self.schedule(self.controller.now() + config.half_locking_time)
        return self.has_lock

    def unlock_and_deactivate(self):

        with conn.cursor() as cur:
            sql = f"UPDATE {Worker.table_name} SET active=false, locked_until=NULL, task_count=%s WHERE id=%s"
            cur.execute(sql, (executing_task_count if self.id==config.worker_id else 0, self.id))
            self.db_state['active'] = False
            self.db_state['locked_until'] = None
            self.set_has_lock(False)

    def recover_worker_tasks(self):
        self.info(Messages.RECOVER_TASKS)
        with conn.cursor() as cur:
            sql = f"SELECT {config.schema}.recover_worker_tasks(%s)"
            cur.execute(sql, (self.id,))

    def process(self):
        if self.db_state is not None:
            try:
                self.set_stop(self.db_state['stop'])
            except KeyError:
                pass

        if self.id == config.worker_id:
            self.keep_lock()
        else:
            if self.db_state['active']:
                t = self.db_state['locked_until']
                if t is None or t + config.failed_worker_recovery_delay < self.controller.now():
                    if self.lock():
                        self.refresh_db_state()
                        if self.db_state['active']: # check again after lock
                            self.recover_worker_tasks()
                        self.unlock_and_deactivate()

    def is_intresting_db_state(db_state:map) -> bool:
        return True

class Task(DbObject):
    """
    task record
    """

    changed = set()
    deleted = set()

    type_name = "t"
    table_name = f"{config.schema}.task"
    check_changed_field = 'started'
    table_fields = ['state_id', 'worker_id', 'group_id', 'next_start', 'shed_period_id', 'shed_period_count', check_changed_field]
    notify_key = f'!{table_name}.{config.group_id}'

    def __init__(self, controller, id = None):
        super().__init__(controller, id)
        self.priority = 1
        self.__process__:TaskProcess = None
        self.next_process_check = None
        self.next_start = None
        self.check_changed_val = None

    def set_process(self, process:TaskProcess):
        global executing_task_count
        if self.__process__ is process:
            return
        if self.__process__ is None:
            if process is not None:
                executing_task_count += 1
        else:
            if process is None:
                executing_task_count -= 1
                startMoreTasks.start_more()
        self.__process__ = process

    def get_process(self) -> TaskProcess:
        return self.__process__

    def terminate_process(self):
        if self.__process__ is None:
            return
        if config.debug:
            self.info('kill process')
        try:
            self.__process__.terminate()
        except Exception as e:
            if config.debug: raise e
            self.error(str(e))
        self.set_process(None)

    def schedule_with_limit(self, t:datetime):
        if t < self.controller.now() + timedelta(hours=1):
            # don`t schedule in a far future to allow task to be unloaded. RefreshTasks will reload sometime
            super().schedule(t)

    def reached_with_limit(self, t:datetime):
        if t is None:
            return True
        else:
            if t <= self.controller.now():
                return True
            else:
                self.schedule_with_limit(t)
                return False

    def get_next_start(self):
        """
        Calculates the time of the next launch. Other algorithms can be implemented here.
        """
        period = self.db_state['shed_period_id']
        count = self.db_state['shed_period_count']
        if count is not None and count > 0:
            add_interval = None
            if period == 'SEC':
                add_interval = relativedelta(seconds=count)
            elif period == 'MIN':
                add_interval = relativedelta(minutes=count)
            elif period == 'HOU':
                add_interval = relativedelta(hours=count)
            elif period == 'DAY':
                add_interval = relativedelta(days=count)
            elif period == 'WEE':
                add_interval = relativedelta(weeks=count)
            elif period == 'MON':
                add_interval = relativedelta(months=count)
            if add_interval is not None:
                return add_period_until(self.next_start, controller.now(), add_interval) + add_interval

    def start(self, command, cwd:str=None):
        if cwd is None or not os.path.isabs(cwd):
            root = config.get("root_dir")
            if root is None:
                root = os.getcwd()
            if cwd is not None:
                cwd = os.path.join(root, cwd)
            else:
                cwd = root

        process = self.get_process()
        if process is not None:
            res_code = process.check_result()
            if res_code is not None:
                self.set_process(None)
                process = None
            elif res_code == 0:
                self.info(Messages.TASK_COMPLETED)
            else:
                self.info(Messages.TASK_FAILED.format(res_code))
        if process is not None:
            self.fail(self.error(Message.RELUNCH_ACTIVE))
        else:
            proc = TaskProcess(list(command), self.id, cwd)
            self.set_process(proc)
            self.next_process_check = None # check the process state immediatelly
        self.refresh_db_state()
        self.info(Messages.TASK_STARTED.format(list(command)))

    def update_status(self, new_state_id, error:str, check_changed_val=None) -> bool:
        with conn.cursor() as cur:
            sql = f"""
                    UPDATE {Task.table_name}
                    SET state_id=%s
                    , error = %s
                    WHERE id=%s AND state_id in('AE','AC')
                    """
            if check_changed_val is not None:
                sql = sql + f" AND {Task.check_changed_field}=%s"
                cur.execute(sql, (new_state_id, error, self.id, check_changed_val))
            else:
                cur.execute(sql, (new_state_id, error, self.id))

            if self.db_state is not None:
                if cur.rowcount > 0:
                    self.db_state['state_id'] = new_state_id
                else:
                    self.db_state = None

            if config.debug:
                if cur.rowcount == 0:
                    self.error(Messages.CANT_UPDATE_TASK_STATE)
            return cur.rowcount > 0

    def fail(self, error:str, canceled:bool=False, force:bool=False):
        res = self.update_status('CC' if canceled else 'CF', error, None if force else self.check_changed_val)
        self.terminate_process()
        if canceled:
            self.info(Messages.TASK_CANCELLED)
        else:
            self.error(error)
        return res

    def complete(self):
        res = self.update_status('CS', None, self.check_changed_val)
        if res:
            self.info(Messages.TASK_COMPLETED)
        return res

    def process(self):

        if worker.stop >= 3:
            if worker.has_lock:
                self.fail(Messages.TASK_INTERRUPTED, force=True)
            self.close()
            return

        # check the DB state
        if self.db_state is not None:

            if self.db_state.get("deleted"):
                self.close()
                return

            self.next_start = self.db_state['next_start']

            process = self.get_process()
            state_id = self.db_state['state_id']
            p_id = self.db_state['worker_id']
            if process is not None:
                if p_id is None or p_id != config.worker_id \
                or self.db_state[Task.check_changed_field] != self.check_changed_val:
                    self.error(Messages.TASK_CATCHED_BY_OTHER_SIDE)
                    self.terminate_process()
                    self.signal()
                elif state_id == 'AC':
                    self.fail(Messages.TASK_CANCELLED, True)
                elif state_id.startswith("C"):
                    if not self.db_state.get("stop_detected", False):
                        self.db_state["stop_detected"] = True # to avoid repeat
                        # the task itself set the completion
                        # let's start checking the status of the process more often
                        self.next_process_check = None
                elif state_id != 'AE':
                    self.error(Messages.TASK_CATCHED_BY_OTHER_SIDE)
                    self.terminate_process()
                    self.signal()
            elif worker.has_lock:
                if p_id is not None and p_id == config.worker_id:
                    if state_id == 'AC':
                        self.fail(Messages.TASK_CANCELLED, canceled=True, force=True)
                    elif state_id == 'AE':
                        self.fail(Messages.TASK_PHANTOM, force=True)

        # check OS process state periodicically
        process = self.get_process()
        if process is not None:
            if self.next_process_check is None:
                self.check_proces_state_interval = config.min_check_proces_state_interval
            if self.reached(self.next_process_check):
                res_code = process.check_result()
                if res_code is None:
                    self.next_process_check = self.controller.now() + self.check_proces_state_interval
                    self.schedule(self.next_process_check)
                    self.check_proces_state_interval = self.check_proces_state_interval + self.check_proces_state_interval
                    if self.check_proces_state_interval > config.max_check_proces_state_interval:
                        self.check_proces_state_interval = config.max_check_proces_state_interval
                elif res_code == 0:
                    self.complete()
                    self.set_process(None)
                else:
                    self.fail(Messages.TASK_FAILED.format(res_code))
                    self.set_process(None)

        # check task.next_start reached
        if self.next_start is not None and self.reached_with_limit(self.next_start):

            if self.db_state is None:
                self.refresh_db_state()
            next_start = self.get_next_start()

            with conn.cursor() as cur:
                sql = f"UPDATE {Task.table_name} SET next_start=%s WHERE id=%s AND next_start=%s"
                cur.execute(sql, (next_start, self.id, self.next_start))
                if cur.rowcount > 0:
                    self.next_start = next_start
                    if self.next_start is not None:
                        self.schedule_with_limit(self.next_start)
                    self.set_db_state(None)
                    sql = f"SELECT {config.schema}.sched_start(%s)"
                    cur.execute(sql, (self.id,))
                    new_task_id = cur.fetchone()[0]
                    if new_task_id is not None:
                        new_task = Task.find_or_new(new_task_id)
                        new_task.refresh_db_state()
                else:
                    # task.next_start has been changed by another node
                    self.next_start = None
                    self.set_db_state(None)

        # if the activities are not planned in the near future
        # then Task is not needed yet, unload it
        if not(self.is_signaled() or self.is_scheduled()):
            #if self.get_process() is None:
                self.close()

    def close(self):
        if config.debug:
            self.info('Close')
        self.terminate_process()
        super().close()

    def is_intresting_db_state(db_state:map) -> bool:
        """
        Decides if the given record should be loaded as a DbObject
        """
        if db_state['group_id'] != config.group_id:
            return False
        if db_state['next_start'] is not None:
            return True
        if db_state['state_id'] == 'AW':
            return False
        if db_state['worker_id'] is None:
            return False
        if db_state['worker_id'] != config.worker_id:
            return False
        if db_state['state_id'].startswith("A"):
            return True
        return False

class StartMoreTasks(CommonTask):
    """
    Runs pending tasks if limits are not exceeded
    """

    def can_start_more(self):
        global no_more_waiting_tasks
        return executing_task_count < config.max_task_count \
            and worker.has_lock and worker.stop == 0

    def process(self):
        global executing_task_count, no_more_waiting_tasks
        if self.can_start_more():
            with conn.cursor() as cur:
                sql = f"SELECT start_task FROM {config.schema}.start_task(%s,%s)"
                cur.execute(sql, (config.group_id, config.worker_id))
                id = cur.fetchone()[0]
                if id is None:
                    no_more_waiting_tasks = True
                    return
                self.signal() # then try one more
                sql = f"SELECT command, cwd, {','.join(Task.table_fields)} \
                    FROM {Task.table_name} \
                    WHERE id = %s"
                cur.execute(sql, (id,))
                row = cur.fetchone()
                if row is None:
                    return
                task = Task.find_or_new(id)
                try:
                    db_state = get_db_state(cur, row)
                    task.check_changed_val = db_state.get(Task.check_changed_field)
                    task.set_db_state(db_state)
                    task.start(row[0], row[1])
                except Exception as e:
                    task.fail(str(e))
                    if config.debug: raise e

    def start_more(self):
        if not no_more_waiting_tasks and self.can_start_more():
            self.signal()

startMoreTasks = StartMoreTasks(controller)
refreshTasks = RefreshTasks(controller)
refreshWorkers = RefreshWorkers(controller)
worker = Worker.find_or_new(config.worker_id) # Node`s worker

def get_db_state(cur, row) -> map:
    return {d.name: row[i] for i, d in enumerate(cur.description)}

def refresh_db_states(cur, object_class, expected_ids:set=None) -> bool:
    global no_more_waiting_tasks
    has_aw = False
    found_ids = set()
    for row in cur.fetchall():
        db_state = get_db_state(cur, row)
        id = db_state['id']
        found_ids.add(id)
        if db_state.get('state_id') == 'AW':
            has_aw = True
        obj = controller.find(object_class.type_name, id)
        if obj is None and object_class.is_intresting_db_state(db_state):
            obj = object_class(controller, db_state['id'])
        if obj is not None:
            obj.set_db_state(db_state)
    if expected_ids is not None:
        for id in expected_ids.difference(found_ids):
            obj = controller.find(object_class.type_name, id)
            if obj is not None:
                obj.set_deleted()
    if object_class is Task:
        if has_aw:
            if no_more_waiting_tasks:
                no_more_waiting_tasks = False
                startMoreTasks.start_more()
    return len(found_ids)

def add_period_until(start:datetime, until:datetime, period:relativedelta):

    def add(start:datetime, period:relativedelta):
        period2 = period + period
        t = start + period2
        if t > until:
            return start
        else:
            t = add(t, period2)
            t1 = t + period
            if t1 <= until:
                return t1
            else:
                return t
    return add(start, period)

def terminate() -> bool:
    if controller.terminated:
        return True
    return worker.stop > 1 and executing_task_count == 0 and locked_other_workers_count == 0

def unlock_workers():
    def stop(w:Worker):
        if w.id != config.worker_id and w.has_lock:
            w.unlock_and_deactivate()
    controller.for_each_object(Worker.type_name, stop)
    if worker.has_lock:
        worker.unlock_and_deactivate()

def run():
    global conn

    delay_after_db_error = config.min_delay_after_db_error
    db_config = config.db
    while True: # DB reconnect loop
        try:
            conn = psycopg2.connect( \
                host=db_config['host'], \
                port=db_config['port'], \
                dbname=db_config['database'], \
                user=db_config['user'], \
                password=db_config['password']  \
            )
            try:

                conn.autocommit = True
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                with conn.cursor() as cur:
                    cur.execute(f'LISTEN "{Task.notify_key}"')
                    cur.execute(f'LISTEN "{Worker.notify_key}"')

                refreshWorkers.refresh_all()
                if not terminate():
                    refreshTasks.refresh_all()

                delay_after_db_error = config.min_delay_after_db_error

                while True: # Main loop

                    next_time = controller.process()
                    wait_time = 5 if config.debug else 60
                    if next_time is not None:
                        dt = (next_time - controller.now()).total_seconds()
                        if dt > 0:
                            if dt < wait_time:
                                wait_time = dt
                        else:
                            wait_time = 0.1

                    if terminate():
                        unlock_workers()
                        return

                    if config.debug: print(wait_time)
                    r, w, e = select.select([conn], [], [], wait_time)

                    Worker.clear_changes()
                    Task.clear_changes()

                    conn.poll()
                    while conn.notifies:
                        n = conn.notifies.pop()
                        if n.channel == Task.notify_key:
                            Task.add_change(n.payload)
                        elif n.channel == Worker.notify_key:
                            Worker.add_change(n.payload)

                    Worker.apply_changes()
                    Task.apply_changes()

            finally:
                conn.close()

        except Exception as e:
            if config.debug: raise e
            print('ERROR', e)
            time.sleep(delay_after_db_error.total_seconds())
            delay_after_db_error += delay_after_db_error
            if delay_after_db_error > config.max_delay_after_db_error:
                delay_after_db_error = config.max_delay_after_db_error

if __name__ == '__main__':

    run()
