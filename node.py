# This code is under MIT licence, you can find the complete file here: https://github.com/ivanovrvl/pg_tasks/blob/main/LICENSE
import json
import sys, os, select, time, datetime
import psycopg2.extensions
import subprocess
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from config import get_config, Messages
import copy
import signal
import eventfd

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
#config.debug = True

capture_stdout = config.capture_stdout
if capture_stdout is not None and capture_stdout > 0:
    import queue
    import threading
else:
    capture_stdout = 0

sys.path.append(os.path.abspath('py_active_objects'))
from active_objects import ActiveObjectWithRetries, ActiveObjectsController

controller = ActiveObjectsController(priority_count=2)

startMoreTasks = None
refreshTasks = None
refreshWorkers = None
worker = None # Node`s worker

conn = None # DB Connection

no_more_waiting_tasks = False # no more AW tasks
locked_other_workers_count = 0

wakeup = eventfd.EventFD()

def get_msg(msg_const:str, default:str):
    return messages.get(msg_const, default)

child_processes = {}

def capture_stdout_func(input, buffer, max_size):
    while True:
        b = input.readline()
        if not b: break
        buffer.put(b)
        while buffer.qsize() > capture_stdout:
            buffer.get()

class TaskProcess:
    """
    OS process wrapper
    """
    def __init__(self, commands:list, id, cwd=None, capture_stdout:int=0):
        cmds = []
        for c in commands:
            if c == '%TASK':
                cmds.append(str(id))
            else:
                cmds.append(c)
        self.id = id
        self.wait_until = None
        self.exit_code = None
        self.capture_stdout = capture_stdout
        if self.capture_stdout > 0:
            self.stdout = queue.Queue()
            self.proc = subprocess.Popen(cmds, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd = cwd)
            self.stdout_thread = threading.Thread(target=capture_stdout_func, args=(self.proc.stdout, self.stdout, self.capture_stdout))
            self.stdout_thread.start()
        else:
            self.proc = subprocess.Popen(cmds, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd = cwd)

    def set_exit_code(self, exit_code:int):
        if self.proc is not None:
            self.exit_code = exit_code
            self.close()

    def check_result(self):
        if self.exit_code is not None:
            return self.exit_code
        exit_code = self.proc.poll()
        if exit_code is not None:
            self.set_exit_code(exit_code)
        return exit_code

    def terminate(self):
        if self.proc is not None:
            self.proc.terminate()

    def kill(self):
        if self.proc is not None:
            self.proc.kill()

    def close(self):
        if self.proc is not None:
            if self.capture_stdout > 0:
                self.proc.stdout.close()
            self.proc = None

    def get_error(self)->str:
        if self.capture_stdout > 0:
            s = b''
            while self.stdout.qsize() > 0:
                s += self.stdout.get()
            return Messages.TASK_FAILED.format(self.exit_code) + '\n' + s.decode('cp866')
        else:
            return Messages.TASK_FAILED.format(self.exit_code)

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
                sql = """
                    SELECT id,""" + ','.join(Worker.table_fields) + """
                    FROM """ + Worker.table_name + """
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
                sql = """
                    SELECT id,""" + ','.join(Task.table_fields) + """
                    FROM """ + Task.table_name + """
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
    #type_name
    #table_name
    #table_fields = ['active', 'locked_until', 'stop']
    #notify_key = '!' + table_name

    type_name = None
    table_name = None
    table_fields = None
    version_field_name = None

    def __init__(self, controller, id = None):
        super().__init__(controller, self.__class__.type_name, id)
        self._old_db_state = None # last known DB state
        self.db_state = None # current state
        self.changed_fields = set()
        self.is_deleted = False

    def set_field(self, name:str, value, set_changed:bool=True):
        if self.db_state is None:
            raise Exception("Can`t set field")
        try:
            cur = self.db_state[name]
            if cur is value or cur == value:
                return False
        except KeyError:
            pass
        self.db_state[name] = value
        if set_changed:
            self.changed_fields.add(name)
        return True

    def set_deleted(self):
        self.info("DELETED")
        self.is_deleted = True
        self._old_db_state = None
        self.db_state = None
        self.changed_fields.clear()
        self.signal()

    def set_db_state(self, state):
        if self._old_db_state is None \
        or (self.__class__.version_field_name is not None \
        and self._old_db_state[self.__class__.version_field_name] != state[self.__class__.version_field_name]):
            self._old_db_state = state
            self.db_state = copy.copy(state)
            self.changed_fields.clear()
        else:
            self._old_db_state = state
            old = self.db_state
            self.db_state = copy.copy(state)
            for n in self.changed_fields:
                self.db_state[n] = old[n]
        self.signal()

    def refresh_db_state(self):
        self._old_db_state = None
        with conn.cursor() as cur:
            sql = """
                SELECT id,""" + ','.join(self.__class__.table_fields) + """
                FROM """ + self.__class__.table_name + """
                WHERE id = %s"""
            cur.execute(sql, (self.id,))
            refresh_db_states(cur, self.__class__, set([self.id]))

    def save_db_state(self):
        if len(self.changed_fields) > 0:
            sql = """
                update """ + self.__class__.table_name + """
                set """ + ','.join([n + '=%s' for n in self.changed_fields]) + """
                where id=%s
            """
            values = [self.db_state[n] for n in self.changed_fields]
            values.append(self.id)
            if self.__class__.version_field_name is not None:
                values.append(self.db_state[self.__class__.version_field_name])
                sql = sql + "and " + self.__class__.version_field_name + "=%s"
            with conn.cursor() as cur:
                cur.execute(sql, values)
                self.changed_fields.clear()
                if cur.rowcount == 0:
                    self.refresh_db_state()
                    if config.debug:
                        self.error(Messages.TASK_CATCHED_BY_OTHER_SIDE)

    def info(self, msg:str):
        if msg is not None:
            super().info(self.type_name + str(self.id) + ': ' + msg)

    def error(self, msg:str):
        if msg is not None:
            super().error(self.type_name + str(self.id) + '! ' + msg)

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
            sql = """
                SELECT id,""" + ','.join(cls.table_fields) + """
                FROM """ + cls.table_name + """
                WHERE id = any(%s)
                """
            cur.execute(sql, (list(cls.changed.difference(cls.deleted)),))
            refresh_db_states(cur, cls)
            cls.changed.clear()

            for id in cls.deleted:
                task = controller.find(cls.type_name, id)
                if task is not None:
                    task.set_deleted()

            cls.deleted.clear()

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
    table_name = config.schema + ".worker"
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
        if old == 4: return False
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
            sql = "SELECT " + config.schema + ".lock_worker(%s,%s,%s,%s,%s,%s)"
            if self.id == config.worker_id:
                cur.execute(sql, (self.id, config.group_id, config.node_name, len(child_processes), lock_until, self.lock_time))
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
            sql = "UPDATE " + Worker.table_name + " SET active=false, locked_until=NULL, task_count=%s WHERE id=%s"
            cur.execute(sql, (len(child_processes) if self.id==config.worker_id else 0, self.id))
            self.db_state['active'] = False
            self.db_state['locked_until'] = None
            self.set_has_lock(False)

    def recover_worker_tasks(self):
        self.info(Messages.RECOVER_TASKS)
        with conn.cursor() as cur:
            sql = "SELECT " + config.schema + ".recover_worker_tasks(%s)"
            cur.execute(sql, (self.id,))

    def process(self):
        self.save_db_state()
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
        self.save_db_state()

    def is_intresting_db_state(db_state:map) -> bool:
        return True

class Task(DbObject):
    """
    task record
    """

    changed = set()
    deleted = set()

    type_name = "t"
    table_name = config.schema + ".task"
    version_field_name = 'last_state_change'
    table_fields = ['state_id', 'worker_id', 'group_id', 'next_start', 'shed_period_id', 'shed_period_count', 'shed_enabled', 'cleanup_pending', version_field_name]
    notify_key = "!" + table_name + "." + str(config.group_id)

    def __init__(self, controller, id = None):
        super().__init__(controller, id)
        self.priority = 1
        self.__process__ = None
        self.next_process_check = None
        self.stop_type = None # тип прерывания 'S' - Stop, 'C' - Cancel

    def set_process(self, process:TaskProcess):
        global child_processes
        if self.__process__ is process:
            return
        if self.__process__ is not None:
            self.__process__.close()
            self.__process__ = None
            child_processes.pop(self.pid)
            if process is None:
                startMoreTasks.start_more()
        if process is not None:
            self.pid = process.proc.pid
            child_processes[self.pid] = self
            self.stop_type = None
        self.__process__ = process

    def get_process(self) -> TaskProcess:
        return self.__process__

    def set_stop(self, stop_type:str) -> bool:
        if self.__process__ is not None:
            if stop_type == 'S':
                if self.stop_type is None:
                    if self.__process__ is not None:
                        self.__process__.terminate()
                    self.stop_type = stop_type
                    self.next_process_check = None # check the process state immediatelly
                    if config.debug:
                        self.info('Stopping')
                    return True
            elif stop_type == 'C':
                if self.stop_type is None or self.stop_type == 'S':
                    if self.__process__ is not None:
                        self.__process__.kill()
                    self.stop_type = stop_type
                    self.next_process_check = None # check the process state immediatelly
                    self.signal()
                    if config.debug:
                        self.info('Cancelling')
                    return True
        return False

    def update_process_state(self):
        process = self.get_process()
        if process is not None:
            res_code = process.check_result()
            if res_code is None: return True
            error = process.get_error()
            self.set_process(None)
            if self.stop_type is None or self.stop_type == 'S':
                if res_code == 0:
                    self.complete()
                else:
                    self.fail(error)
            else:
                self.fail(error, canceled=True)
        return False

    def set_process_exit_code(self, exit_code:int):
        if exit_code is not None:
            process = self.get_process()
            if process is not None:
                process.set_exit_code(exit_code)

    def kill_process(self):
        if self.__process__ is None:
            return
        self.info('kill process')
        try:
            self.__process__.kill()
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
                return add_period_until(self.db_state['next_start'], controller.now(), add_interval) + add_interval

    def start(self, command, cwd:str=None):
        global capture_stdout
        if self.update_process_state():
            raise Exception(Messages.RELUNCH_ACTIVE)

        if cwd is None or not os.path.isabs(cwd):
            root = config.get("root_dir")
            if root is None:
                root = os.getcwd()
            if cwd is not None:
                cwd = os.path.join(root, cwd)
            else:
                cwd = root

        proc = TaskProcess(list(command), self.id, cwd, capture_stdout=capture_stdout)
        self.set_process(proc)
        self.next_process_check = None # check the process state immediatelly

        self.refresh_db_state()
        self.info(Messages.TASK_STARTED.format(list(command)))

    def fail(self, error:str, canceled:bool=False):
        if self.db_state is None: self.refresh_db_state()
        if self.db_state is not None:
            if error is not None:
                error = str(error).replace('\0', '\n')
            if self.db_state['state_id'].startswith('A'):
                self.set_field("state_id", 'CC' if canceled else 'CF')
                self.set_field("error", error)
            self.kill_process()
            if canceled:
                self.info(Messages.TASK_CANCELLED)
            else:
                self.error(error)

    def complete(self):
        if self.db_state['state_id'].startswith('A'):
            self.set_field("state_id", 'CS')
        self.info(Messages.TASK_COMPLETED)

    def set_watchdog(self):
        process = self.get_process()
        if process is not None and process.wait_until is None:
            process.wait_until = self.schedule_seconds(10)

    def process(self):

        self.save_db_state()

        if not worker.has_lock or worker.stop >= 3 or self.is_deleted:
            self.set_stop('C')
        elif self.db_state is not None:
            state_id = self.db_state['state_id']
            p_id = self.db_state['worker_id']
            if p_id is None or p_id != config.worker_id:
                self.next_process_check = None
                self.set_watchdog()
                if state_id.startswith('A') and state_id != 'AW':
                    if self.set_stop('C'):
                        self.error(Messages.TASK_CATCHED_BY_OTHER_SIDE)
            elif state_id == 'AS':
                self.set_stop('S')
            elif state_id == 'AC':
                self.set_stop('C')

        if self.update_process_state():

            process = self.get_process()
            if process.wait_until is not None and self.reached(process.wait_until):
                self.set_stop('C')

            if self.stop_type is None and self.db_state is not None:
                state_id = self.db_state['state_id']
                if state_id != 'AE':
                    self.next_process_check = None
                    self.set_watchdog()
                    if not state_id.startswith('C'):
                        self.error(Messages.TASK_CATCHED_BY_OTHER_SIDE)
                        self.set_stop('C')

            if self.next_process_check is None:
                self.check_proces_state_interval = config.min_check_proces_state_interval
            if self.reached(self.next_process_check):
                self.next_process_check = self.controller.now() + self.check_proces_state_interval
                self.schedule(self.next_process_check)
                self.check_proces_state_interval = self.check_proces_state_interval + self.check_proces_state_interval
                if self.check_proces_state_interval > config.max_check_proces_state_interval:
                    self.check_proces_state_interval = config.max_check_proces_state_interval

        else:
            if worker.has_lock and self.db_state is not None:
                state_id = self.db_state['state_id']
                if state_id.startswith('A') and state_id != 'AW':
                    self.fail(Messages.TASK_PHANTOM)

        # check task.next_start reached
        if self.db_state is not None and self.db_state['shed_enabled'] and not self.db_state['cleanup_pending'] and worker.has_lock and self.stop_type is None:
            next_start = self.db_state['next_start']
            if next_start is not None and self.reached_with_limit(next_start):
                new_next_start = self.get_next_start()
                with conn.cursor() as cur:
                    sql = "UPDATE " + Task.table_name + " SET next_start=%s WHERE id=%s AND next_start=%s"
                    cur.execute(sql, (new_next_start, self.id, next_start))
                    if cur.rowcount > 0:
                        self.set_field('next_start', new_next_start, set_changed=False)
                        if new_next_start is not None:
                            self.schedule_with_limit(new_next_start)
                        sql = "SELECT " + config.schema + ".sched_start(%s)"
                        cur.execute(sql, (self.id,))
                        new_task_id = cur.fetchone()[0]
                        if new_task_id is not None:
                            new_task = Task.find_or_new(new_task_id)
                            new_task.refresh_db_state()
                    else:
                        self.refresh_db_state()

        self.save_db_state()

        if self.get_process() is None:
            if self.is_deleted or not(self.is_signaled() or self.is_scheduled()):
                self.close()

    def close(self):
        if config.debug:
            self.info('Close')
        self.kill_process()
        super().close()

    def is_intresting_db_state(db_state:map) -> bool:
        """
        Decides if the given record should be loaded as a DbObject
        """
        if db_state['group_id'] != config.group_id:
            return False
        if db_state['shed_enabled'] and db_state['next_start'] is not None:
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
        return len(child_processes) < config.max_task_count \
            and worker.has_lock and worker.stop == 0

    def process(self):
        global no_more_waiting_tasks
        if self.can_start_more():
            with conn.cursor() as cur:
                sql = "SELECT start_task FROM " + config.schema + ".start_task(%s,%s)"
                cur.execute(sql, (config.group_id, config.worker_id))
                id = cur.fetchone()[0]
                if id is None:
                    no_more_waiting_tasks = True
                    return
                self.signal() # then try one more
                sql = "SELECT command, cwd, " + ','.join(Task.table_fields) + " \
                    FROM " + Task.table_name + " \
                    WHERE id = %s"
                cur.execute(sql, (id,))
                row = cur.fetchone()
                if row is None:
                    return
                task = Task.find_or_new(id)
                try:
                    db_state = get_db_state(cur, row)
                    task.set_db_state(db_state)
                    task.start(row[0], row[1])
                except Exception as e:
                    task.fail(str(e))
                    task.save_db_state()
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
    return worker.stop > 1 and len(child_processes) == 0 and locked_other_workers_count == 0

def unlock_workers():
    def stop(w:Worker):
        if w.id != config.worker_id and w.has_lock:
            w.unlock_and_deactivate()
    controller.for_each_object(Worker.type_name, stop)
    if worker.has_lock:
        worker.unlock_and_deactivate()


terminated_processes_backlog = []
terminate_backlog = []

def on_child_signal(signum, frame):
    try:
        b = True
        while True:
            child_pid, exit_code = os.waitpid(-1, os.WNOHANG)
            if child_pid <= 0: break
            terminated_processes_backlog.append((child_pid, exit_code))
            if b:
                b = False
                wakeup.set()
    except ChildProcessError as e:
        pass

def on_term_signal(signum, frame):
    print("SIGTERM")
    terminate_backlog.append(4)
    wakeup.set()

def process_signal_backlog():
    try:
        while True:
            worker.set_stop(terminate_backlog.pop())
    except IndexError:
        pass
    try:
        while True:
            child_pid, exit_code = terminated_processes_backlog.pop()
            task = child_processes.get(child_pid)
            if task is not None:
                task.set_process_exit_code(exit_code)
                task.signal()
                if config.debug:
                    print('SIGCHLD', child_pid, exit_code)
    except IndexError:
        pass

def run():
    global conn

    delay_after_db_error = config.min_delay_after_db_error
    db_config = config.db

    try:
        signal.signal(signal.SIGTERM, on_term_signal)
        signal.signal(signal.SIGCHLD, on_child_signal)
    except AttributeError as e:
        print(e)

    nextSignalAll = datetime.now() + timedelta(minutes=5)

    while True: # DB reconnect loop
        try:
            conn = psycopg2.connect( \
                host=db_config['host'], \
                port=db_config['port'], \
                dbname=db_config['database'], \
                user=db_config['user'], \
                password=deobfuscate(db_config['password']) \
            )
            try:

                conn.autocommit = True
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                try:
                    with conn.cursor() as cur:
                        cur.execute("INSERT INTO " + Worker.table_name + "(id) VALUES(%s)", [config.worker_id])
                except Exception as e:
                    pass

                with conn.cursor() as cur:
                    cur.execute('LISTEN "' + Task.notify_key + '"')
                    cur.execute('LISTEN "' + Worker.notify_key + '"')

                refreshWorkers.refresh_all()
                if not terminate():
                    refreshTasks.refresh_all()

                delay_after_db_error = config.min_delay_after_db_error

                while True: # Main loop

                    next_time = controller.process(max_count=100)

                    if terminate():
                        unlock_workers()
                        return

                    if next_time is not None:
                        wait_time = 5 if config.debug else 60
                        dt = (next_time - controller.now()).total_seconds()
                        if dt > 0:
                            if dt < wait_time:
                                wait_time = dt
                        else:
                            wait_time = 0.1

                        #if config.debug: print(wait_time)
                        r, w, e = select.select([conn, wakeup], [], [], wait_time)
                        if wakeup in r:
                            wakeup.clear()
                            process_signal_backlog()

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

                    if nextSignalAll <= datetime.now():
                        if config.debug:
                            nextSignalAll = datetime.now() + timedelta(seconds=10)
                        else:
                            nextSignalAll = datetime.now() + timedelta(minutes=5)
                        controller.signal()

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

