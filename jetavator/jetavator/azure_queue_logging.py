import logging
import os
import socket

from azure.storage.queue import QueueClient
from azure.core.exceptions import ResourceNotFoundError


class AzureQueueHandler(logging.Handler):

    def __init__(self,
        service,
        protocol='https',
        queue='logs'
    ):
        super().__init__()
        self.meta = {'hostname': socket.gethostname(), 'process': os.getpid()}
        self.queue = service.queue_client(
            queue.format(**self.meta)
        )

    def emit(self, record):
        try:
            if not self.queue_exists:
                self.queue.create_queue()
            record.hostname = self.meta['hostname']
            self.queue.send_message(
                self.format(record)
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    @property
    def queue_exists(self):
        try:
            self.queue.get_queue_properties()
            return True
        except ResourceNotFoundError:
            return False
