import redis
from redash.tasks.worker import Queue, Job
from rq.exceptions import NoSuchJobError
from redash.worker import get_job_logger
from redash.utils import utcnow
from redash import models, settings, redis_connection
from flask_restful import abort
import duckdb
import json
import pandas as pd
import uuid
from rq.job import JobStatus
from sqlalchemy.orm.exc import NoResultFound

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


def enqueue_download_audit(push_id, user, query, time, format, limit, query_result_id, current_org_id, source):
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
                logger.info(f"Enqueing push_to_jumbo job in {queue_name}")
                result = queue.enqueue(
                    push_to_jumbo, push_id, user, time, format, limit, query_result_id, current_org_id, source
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


def push_to_jumbo(push_id, user, time, format, limit, query_result_id, current_org_id, source):
    try:
        logger.info(f"[push_to_jumbo] Querying query_results for query_result_id: {query_result_id}")
        if current_org_id:
            current_org = models.Organization.get_by_id(current_org_id)
        if query_result_id and current_org:
            query_result = get_object_or_404(
                models.QueryResult.get_by_id_and_org, query_result_id, current_org
            )
        
        query_data = query_result.data
        download_data = query_data["rows"][:limit]
        columns = query_data["columns"]
        logger.info(f"[push_to_jumbo] Processing task for user: {user} downloading {limit} rows")
        dt = time.strftime('%Y%m%d')
        logging_table_path = settings.DOWNLOAD_DATA_AUDIT_LOGGING_S3_PATH
        data_path = settings.DOWNLOAD_DATA_ARCHIVE_S3_PATH
        unique_table_file_name = f"part-{str(uuid.uuid4())}.parquet"
        data_file_name = f"data-{str(uuid.uuid4())}.parquet"
        s3_table_path = f"{logging_table_path}/dt={dt}/{unique_table_file_name}"
        s3_data_path = f"{data_path}/{data_file_name}"

        download_audit_log: dict = {
            "id": str(uuid.uuid1()),
            "user": user,
            "timestamp": int(time.timestamp()),
            "dt": time.strftime('%Y%m%d'),
            "sample_data": json.dumps(download_data[:10]),
            "total_row_count": limit,
            "format": format,
            "columns": columns,
            "query": query_result.query_text,
            "data_path": s3_data_path,
            "redash_type": settings.REDASH_NAME,
            "source": source
        }
        
        download_audit_log = {key: [value] for key, value in download_audit_log.items()}
        table_df = pd.DataFrame(download_audit_log, index=[0])
        data_df = pd.DataFrame(download_data)
        
        conn = duckdb.connect()
        conn.execute('CALL load_aws_credentials()')
        conn.register('table_df', table_df)
        conn.register('data_df', data_df)
        conn.execute(f"""
            COPY data_df TO '{s3_data_path}'
            (FORMAT PARQUET)
        """)
        conn.execute(f"""
            COPY table_df TO '{s3_table_path}'
            (FORMAT PARQUET)
        """)
        _unlock(push_id)
    except Exception as e:
        logger.info(f"Exception occurred while pushing download logs to jumbo: {e}")
        _unlock(push_id)
        
def get_object_or_404(fn, *args, **kwargs):
    try:
        rv = fn(*args, **kwargs)
        if rv is None:
            abort(404)
    except NoResultFound:
        abort(404)
    return rv
