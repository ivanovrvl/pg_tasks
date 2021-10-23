import os
import sys
import json
from datetime import timedelta

def get_config():

    with open('db_config.json', 'r') as f:
        db_config = json.load(f)

    return {
        "debug": True,

        "worker_id": 1 if len(sys.argv)<=1 else int(sys.argv[1]), # должно быть уникальным для каждого запущенного экземпляра
        "group_id": 0 if len(sys.argv)<=2 else int(sys.argv[2]), # группа конкурирующих воркеров

        "max_task_count": 1 if len(sys.argv)<=3 else int(sys.argv[3]), # максимальное число одновременных задач
        "node_name": os.environ['COMPUTERNAME'] if len(sys.argv)<=4 else int(sys.argv[4]),

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

        "db": db_config
    }

if __name__ == '__main__':

    print(get_config())
