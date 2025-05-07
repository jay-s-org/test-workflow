import os
import logging

from celery import Celery
from celery.utils.log import get_task_logger
from celery.signals import setup_logging

# Configure root logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

# Set higher log levels for noisy libraries
logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('amqp').setLevel(logging.WARNING)
logging.getLogger('kombu').setLevel(logging.WARNING)
logging.getLogger('celery.worker.strategy').setLevel(logging.WARNING)
logging.getLogger('celery.app.trace').setLevel(logging.WARNING)
logging.getLogger('pymongo').setLevel(logging.WARNING)
logging.getLogger('pymongo.topology').setLevel(logging.WARNING)
logging.getLogger('pymongo.connection').setLevel(logging.WARNING)
logging.getLogger('pymongo.serverSelection').setLevel(logging.WARNING)
logging.getLogger('pymongo.command').setLevel(logging.WARNING)

# Ensure our custom modules log at DEBUG level
logging.getLogger('ppfl_python_worker.analysis').setLevel(logging.DEBUG)
logging.getLogger('ppfl_python_worker.csw').setLevel(logging.DEBUG)

# Prevent Celery from hijacking root logger
@setup_logging.connect
def setup_celery_logging(loglevel=None, **kwargs):
    pass

broker_url = os.environ.get("CELERY_BROKER_URL")
logger = get_task_logger(__name__)
logger.setLevel(logging.DEBUG)

app = Celery("ppfl_python_worker", broker=broker_url)
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    enable_utc=True,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    result_backend=None,
)

# Configure Celery logging
app.conf.worker_log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
app.conf.worker_task_log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(task_name)s - %(task_id)s'

from ppfl_python_worker.csw import tasks  # noqa
