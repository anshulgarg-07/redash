import redis
import signal
from redash.tasks.worker import Queue, Job
from rq.timeouts import JobTimeoutException
from rq.exceptions import NoSuchJobError, DequeueTimeout
from redash.worker import get_job_logger
from redash.utils import utcnow
from redash import models, settings, redis_connection
from rq.job import JobStatus

logger = get_job_logger(__name__)
TIMEOUT_MESSAGE = "Destination Sync exceeded Redash destination sync time limit."


def _job_lock_id(destination_id):
    return "sync_job:d:%s" % (destination_id)

def _unlock(query_hash, data_source_id):
    redis_connection.delete(_job_lock_id(query_hash, data_source_id))


class InterruptException(Exception):
    pass


def signal_handler(*args):
    raise InterruptException


class DestinationSyncError(Exception):
    pass


class SyncTask(object):
    # TODO: this is mapping to the old Job class statuses. Need to update the client side and remove this
    STATUSES = {
        'queued': 1,
        'started': 2,
        'finished': 3,
        'failed': 4,
        'stopped': 4
    }

    def __init__(self, job_id=None, job=None):
        if job:
            self._job = job
        else:
            self._job = Job.fetch(job_id)

    @property
    def id(self):
        return self._job.id

    def to_dict(self):
        task_status = self._job.get_status()
        result = self._job.result
        if task_status == 'started':
            updated_at = self._job.started_at
        else:
            updated_at = 0

        status = self.STATUSES[task_status]

        if isinstance(result, JobTimeoutException):
            error = TIMEOUT_MESSAGE
            status = 4
        elif isinstance(result, Exception):
            error = result.message
            status = 4
        elif task_status == 'stopped':
            error = 'Destination Sync cancelled.'
        else:
            error = ''

        return {
            'id': self._job.id,
            'updated_at': updated_at,
            'status': status,
            'error': error,
        }

    @property
    def is_cancelled(self):
        return self._job.get_status() == 'stopped'

    @property
    def status(self):
        return self._job.get_status()

    def ready(self):
        return self._job.is_finished

    def cancel(self):
        self._job.cancel()


def enqueue_destination(destination_id, user_id, sync_type, metadata={}):
    logger.info("Inserting job for %s with metadata=%s", destination_id, metadata)
    try_count = 0
    job = None

    while try_count < 5:
        try_count += 1

        pipe = redis_connection.pipeline()
        try:
            pipe.watch(_job_lock_id(destination_id))
            job_id = pipe.get(_job_lock_id(destination_id))
            if job_id:
                logger.info("[%s] Found existing sync job: %s", destination_id, job_id)
                job_complete = None
                job_cancelled = None

                try:
                    sync_task = SyncTask(job_id=job_id)
                    job=sync_task._job
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
                    logger.info("[%s] %s, removing lock", destination_id, message)
                    redis_connection.delete(_job_lock_id(destination_id))
                    job = None

            if not job:
                pipe.multi()

                queue_name = "destination_sync"
                time_limit = settings.REDASH_SYNC_TIME_LIMIT

                queue = Queue(queue_name)
                enqueue_kwargs = {
                    "user_id": user_id,
                    "job_timeout": time_limit,
                    "failure_ttl": settings.JOB_DEFAULT_FAILURE_TTL,
                    "destination_id": destination_id,
                    "sync_type": sync_type
                }

                job = queue.enqueue(
                    sync_destination, destination_id, user_id, sync_type, **enqueue_kwargs
                )

                logger.info("[%s] Created new sync job: %s", destination_id, job.id)
                pipe.set(
                    _job_lock_id(destination_id),
                    job.id,
                    settings.JOB_EXPIRY_TIME,
                )
                pipe.execute()
            break

        except redis.WatchError:
            continue

    if not job:
        logger.error("[Manager][%s] Failed adding job for destination.", destination_id)

    return job


def sync_destination(destination_id, user_id, sync_type):
    try:
        destination = models.Destination.query.get(destination_id)
        error = destination.sync(user_id=user_id, sync_type=sync_type)
        _unlock(destination_id)
        if error:
            logger.warn(u"Some error occured while syncing data for the configuration: {options}"
                            .format(options=destination.options.to_json()))
            raise DestinationSyncError(error)

        logger.info("Successfully synced to google sheets")
        return None
    except JobTimeoutException:
        logger.warn(TIMEOUT_MESSAGE, u" Configuration: {options}".format(options=destination.options.to_json()))
        logger.info("Adding sync time limit failure to db")
        _unlock(destination_id)
        models.DestinationSyncHistory.store_result(
            synced_at=utcnow(),
            sync_type=sync_type,
            sync_duration=settings.REDASH_SYNC_TIME_LIMIT,
            destination_id=destination_id,
            user_id=user_id,
            status="failed",
            error_log=TIMEOUT_MESSAGE,
            rows=destination.options.get('last_sync_rows'),
            columns=destination.options.get('last_sync_columns')
        )
        models.db.session.commit()
        logger.info("Added sync time limit failure to db")
        raise JobTimeoutException
    except DequeueTimeout as e:
        logger.info(f"Job Dequeue Timeout: {e.extra_info}", u"Configuration: {options}".format(options=destination.options.to_json()))
        raise e