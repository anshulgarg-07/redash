import redis
from redash.tasks.worker import Queue, Job
from rq.exceptions import NoSuchJobError
from redash.worker import get_job_logger
from redash.utils import utcnow
from redash import models, settings, redis_connection
import duckdb
import json
import pandas as pd
import uuid
from rq.job import JobStatus

logger = get_job_logger(__name__)

def _job_lock_id(job_id):
    return "push_to_jumbo:d:%s" % (job_id)


def _unlock(job_id):
    redis_connection.delete(_job_lock_id(job_id))
    
class PushToJumboTask(object):
    def __init__(self, job_id=None, job=None):
        if job:
            self._job = job
        else:
            self._job = Job.fetch(job_id)

    @property
    def id(self):
        return self._job.id

    @property
    def is_cancelled(self):
        return self._job.get_status() == 'stopped'

    @property
    def status(self):
        return self._job.get_status()

    def get_status(self):
        return self.status


def enqueue_download_audit(push_id, data, user, query, time, format, limit):
    logger.info("Inserting push_to_jumbo for user: %s", user)
    try_count = 0
    job = None

    while try_count < 3:
        try_count += 1

        pipe = redis_connection.pipeline()
        try:
            pipe.watch(_job_lock_id(push_id))
            job_id = pipe.get(_job_lock_id(push_id))
            if job_id:
                logger.info("[%s] Found existing push_to_jumbo job", push_id)
                job_complete = None
                job_cancelled = None

                try:
                    job = PushToJumboTask(job_id=job_id)
                    job_exists = True
                    status = job.get_status()
                    job_complete = status in [JobStatus.FINISHED, JobStatus.FAILED]
                    job_cancelled = job.is_cancelled

                    if job_complete:
                        message = "job found is complete (%s)" % status
                    elif job_cancelled:
                        message = "job found has been cancelled"
                except NoSuchJobError:
                    message = "job found has expired"
                    job_exists = False

                lock_is_irrelevant = job_complete or job_cancelled or not job_exists

                if lock_is_irrelevant:
                    logger.info("[%s] %s, removing lock", push_id, message)
                    redis_connection.delete(_job_lock_id(push_id))
                    job = None

            if not job:
                pipe.multi()

                queue_name = "push_to_jumbo"

                queue = Queue(queue_name)
                logger.info(f"Enqueing push_to_jumbo job for query {query} in {queue_name}")
                result = queue.enqueue(
                    push_to_jumbo, push_id, data, user, query, time, format, limit
                )
                job = PushToJumboTask(job=result)
                logger.info("[%s] Created push_to_jumbo job: %s", push_id, job.id)
                pipe.set(
                    _job_lock_id(push_id),
                    job.id,
                    settings.JOB_EXPIRY_TIME,
                )
                pipe.execute()
            break

        except redis.WatchError:
            continue

    if not job:
        logger.error("[Manager][%s] Failed adding job for push_to_jumbo.", push_id)

    return job


def push_to_jumbo(push_id, data, user, query, time, format, limit):
    try:
        logger.info(f"Processing push_to_jumbo task for user: {user} downloading {limit} rows")
        download_data = data["rows"][:limit]
        
        dt = time.strftime('%Y%m%d')
        s3_base_path = settings.DOWNLOAD_DATA_AUDIT_LOGGING_S3_PATH
        unique_file_name = f"part-{str(uuid.uuid4())}.parquet"
        s3_path = f"{s3_base_path}/dt={dt}/{unique_file_name}"

        download_audit_log: dict = {
            "id": str(uuid.uuid1()),
            "user": user,
            "timestamp": int(time.timestamp()),
            "dt": time.strftime('%Y%m%d'),
            "sample_data": json.dumps(download_data[:2000]),
            "total_row_count": len(download_data),
            "format": format,
            "columns": data["columns"],
            "query": query,
            "data_path": s3_path
        }
        
        download_audit_log = {key: [value] for key, value in download_audit_log.items()}
        df = pd.DataFrame(download_audit_log, index=[0])
        
        conn = duckdb.connect()
        conn.execute('CALL load_aws_credentials()')
        conn.register('df', df)
        conn.execute(f"""
            COPY df TO '{s3_path}'
            (FORMAT PARQUET, PARTITION_BY (dt))
        """)
        _unlock(push_id)
    except Exception as e:
        logger.info(f"Exception occurred while pushing download logs to jumbo: {e}")
        _unlock(push_id)