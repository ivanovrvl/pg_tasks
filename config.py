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

        "worker_id": 1 if len(sys.argv)<=1 else int(sys.argv[1]), # должно быть уникальным для каждого запущенного экземпляра
        "group_id": 0 if len(sys.argv)<=2 else int(sys.argv[2]), # группа конкурирующих воркеров

        "max_task_count": 3 if len(sys.argv)<=3 else int(sys.argv[3]), # максимальное число одновременных задач
        "node_name": socket.gethostname() if len(sys.argv)<=4 else int(sys.argv[4]),

        "half_locking_time": timedelta(seconds=5), # половина времени продления блокировки worker
        "failed_worker_recovery_delay": timedelta(seconds=5), # задержка обнаружения worker failed state
        "workers_refresh_inverval": timedelta(seconds=30), # периодичность обновления информации о workers
        "task_retry_delay": timedelta(seconds=5), # задержка повтора обработки Task после ошибки

        # как часто проверяется состояние процесса (начинается с минимума и каждый раз увеличивается вдвое)
        "min_check_proces_state_interval": timedelta(seconds=1), # минимум
        "max_check_proces_state_interval": timedelta(seconds=10), # максимум

        # задержка переподключения к БД после ошибки, сек
        "min_delay_after_db_error": timedelta(seconds=1), # минимум
        "max_delay_after_db_error": timedelta(seconds=30), # максимум

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

class Messages(Messages_rus):
    pass

if __name__ == '__main__':

    print(get_config())
