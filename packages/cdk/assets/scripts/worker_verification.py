import sys
import logging
from pyspark.context import SparkContext

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

sc = SparkContext.getOrCreate()

# エグゼキューター数（ドライバーを除く）
num_executors = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
logger.info(f"エグゼキューター数: {num_executors}")
logger.info(f"デフォルト並列度: {sc.defaultParallelism}")


# 並列処理でワーカー情報をログ出力
def log_worker_info(task_id):
    import logging, sys, socket, os, time

    worker_logger = logging.getLogger(__name__)
    if not worker_logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s - %(message)s")
        )
        worker_logger.addHandler(handler)
        worker_logger.setLevel(logging.INFO)

    worker_logger.info(
        f"タスク {task_id}: ホスト={socket.gethostname()}, PID={os.getpid()}"
    )
    time.sleep(5)  # 5秒待機
    worker_logger.info(f"タスク {task_id}: 完了")
    return task_id


tasks = sc.parallelize(range(16), 16)
tasks.foreach(log_worker_info)

logger.info("全タスク完了")
