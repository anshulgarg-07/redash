"""gunicorn configuration file to export server metrics

To launch add `-c webapp/gunicorn_config.py` to the gunicorn command line.
Requires statsd to be configured for gunicorn as well, e.g.
`ENV STATSD_HOST=statsd-exporter:9125`

"""

# Python imports
import ctypes
import os
import socket
import struct
import sys
import threading
import time
from multiprocessing import Value


METRIC_INTERVAL = int(os.environ.get("SATURATION_METRIC_INTERVAL", 5))

# Defaults to None, in which case no metrics will be sent.
statsd_host = os.environ.get("STATSD_HOST")

# Used to differentiate instances; our version combines this with app-specific ID like environment
dogstatsd_tags = "hostname:{}".format(
    socket.gethostname(),
)


class SaturationMonitor(threading.Thread):
    def __init__(self, server):
        super().__init__()
        self.server = server
        self.daemon = True

    def run(self):
        self.server.log.debug(f"Started SaturationMonitor with interval {METRIC_INTERVAL}")
        while True:
            self.server.log.debug(
                f"total workers = {self.server.num_workers}",
                extra={"metric": "gunicorn.total_workers", "value": str(self.server.num_workers), "mtype": "gauge"},
            )
            busy_workers = sum(1 for worker in self.server.WORKERS.values() if worker.busy.value)
            self.server.log.debug(
                f"busy workers = {busy_workers}",
                extra={"metric": "gunicorn.busy_workers", "value": str(busy_workers), "mtype": "gauge"},
            )
            backlog = self.get_backlog()
            if backlog is not None:
                self.server.log.debug(
                    f"socket backlog: {backlog}",
                    extra={"metric": "gunicorn.backlog", "value": str(backlog), "mtype": "gauge"},
                )
            time.sleep(METRIC_INTERVAL)

    def get_backlog(self):
        """Get the number of connections waiting to be accepted by a server"""
        # if not sys.platform == "linux":
        #     return None
        total = 0
        for listener in self.server.LISTENERS:
            if not listener.sock:
                continue

            # tcp_info struct from include/uapi/linux/tcp.h
            fmt = "B" * 8 + "I" * 24
            tcp_info_struct = listener.sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 104)
            # 12 is tcpi_unacked
            total += struct.unpack(fmt, tcp_info_struct)[12]

        return total


def when_ready(server):
    server.log.debug("Starting SaturationMonitor")
    sm = SaturationMonitor(server)
    sm.start()
    server.log.debug("busy workers = 0", extra={"metric": "gunicorn.busy_workers", "value": "0", "mtype": "gauge"})
    server.log.debug(
        "total workers = 0",
        extra={"metric": "gunicorn.total_workers", "value": str(server.num_workers), "mtype": "gauge"},
    )


def pre_fork(server, worker):
    worker.busy = Value(ctypes.c_bool, False)


def pre_request(worker, req):
    worker.busy.value = True


def post_request(worker, req, environ, resp):
    worker.busy.value = False