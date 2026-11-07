import logging
import signal
import threading

from .config import LOG_LEVEL


def setup_logging(service: str) -> logging.Logger:
    logging.basicConfig(
        level=LOG_LEVEL,
        format=f"%(asctime)s %(levelname)-5s [{service}] %(name)s: %(message)s",
    )
    return logging.getLogger(service)


class GracefulShutdown:
    """Sets a flag on SIGINT/SIGTERM so loops can finish the current batch,
    commit, and leave the consumer group cleanly (fast rebalance)."""

    def __init__(self):
        self._event = threading.Event()
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, _frame):
        logging.getLogger("shutdown").info("Received signal %s, shutting down...", signum)
        self._event.set()

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def wait(self, seconds: float) -> bool:
        return self._event.wait(seconds)
