import os
import sys
import json
from datetime import timedelta
import socket

def get_config():

    with open('db_config.json', 'r') as f:
        db_config = json.load(f)

    return {
        "debug": False,

        # должно быть уникальным для каждого запущенного экземпляра
        # must be unique for each node
        "worker_id": 1 if len(sys.argv)<=1 else int(sys.argv[1]),
        
        # группа конкурирующих воркеров
        # the group of concurenting nodes
        "group_id": 0 if len(sys.argv)<=2 else int(sys.argv[2]),

        # максимальное число одновременных задач
        # max simultanious tasks
        "max_task_count": 3 if len(sys.argv)<=3 else int(sys.argv[3]), 
        
        "node_name": socket.gethostname() if len(sys.argv)<=4 else sys.argv[4],

        # половина времени продления блокировки worker
        # half of the locking period to prolongate one
        "half_locking_time": timedelta(seconds=5),
                
        # задержка после истечения блокировки, когда запускается worker failed state recovery
        # delay after the lock deadline when worker failed state recovery has to be start
        "failed_worker_recovery_delay": timedelta(seconds=5), 
        
        # период обновления информации о workers
        # workers state refresh period
        "workers_refresh_inverval": timedelta(seconds=30),

        # задержка повтора обработки после ошибки (удваивается при повторе)
        # delay to retry after error (starts with min and doubled on retry)
        "min_task_retry_delay": timedelta(seconds=1), # минимум (min)
        "max_task_retry_delay": timedelta(seconds=300), # максимум (max)

        # как часто проверяется состояние процесса (сначала минимум, затем удваивается)
        # period to check OS process state (starts with min and doubled on check)
        "min_check_proces_state_interval": timedelta(seconds=1), # минимум (min)
        "max_check_proces_state_interval": timedelta(seconds=10), # максимум (max)

        # задержка переподключения к БД после ошибки, сек (удваивается при повторе)
        # delay to reconnect do DB (starts with min and doubled on check)
        "min_delay_after_db_error": timedelta(seconds=1), # минимум (min)
        "max_delay_after_db_error": timedelta(seconds=30), # максимум (max)

        "db": db_config,
        "schema": "long_task"
    }

class Messages_eng:
    REFRESH_WORKERS = None #"Reloading workers"
    REFRESH_TASKS = None #"Reloading tasks"
    LOCK_AQUIRED = "Lock aquired"
    LOCK_RELEASED = "Lock released"
    STOP_VAL = "stop={}"
    RELUNCH_ACTIVE = "Can`t relunch task with active process"
    CANT_UPDATE_TASK_STATE = "Can`t update task state"
    TASK_STARTED = "Started {}"
    TASK_FAILED = "Stopped with code {}"
    TASK_CANCELLED = "Cancelled"
    TASK_COMPLETED = "Completed"
    TASK_INTERRUPTED = "Interrupted"
    TASK_CATCHED_BY_OTHER_SIDE = "Task catched by the other side"
    TASK_PHANTOM = "Phantom task"
    LOCK_CATCHED = "Lock was catched by the other side"
    RECOVER_TASKS = "Recover failed worker's tasks"

class Messages_rus:
    REFRESH_WORKERS = None #"Reloading workers"
    REFRESH_TASKS = None #"Reloading tasks"
    LOCK_AQUIRED = "Блокировка получена"
    LOCK_RELEASED = "Блокировка снята"
    STOP_VAL = "stop={}"
    RELUNCH_ACTIVE = "Не могу запустить задачу пока процесс активен"
    CANT_UPDATE_TASK_STATE = "Не могу обновить статус задачи"
    TASK_STARTED = "Запущена {}"
    TASK_FAILED = "Завершена с кодом {}"
    TASK_CANCELLED = "Отменена"
    TASK_COMPLETED = "Завершена"
    TASK_INTERRUPTED = "Прервана"
    TASK_CATCHED_BY_OTHER_SIDE = "Задача перехвачена другой стороной"
    TASK_PHANTOM = "Фантомная задача"
    LOCK_CATCHED = "Блокировка кем-то перехвачена"
    RECOVER_TASKS = "Восстановление задач для failed worker"

class Messages(Messages_eng):
    pass

if __name__ == '__main__':

    print(get_config())
