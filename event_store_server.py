import json
import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

from event_store_core import EventStore

from event_store_pb2 import PublishResponse, Notification, UnsubscribeResponse, GetResponse
from event_store_pb2_grpc import EventStoreServicer, add_EventStoreServicer_to_server


class EventStoreServer(EventStoreServicer):
    """
    Event Store Server class.
    """

    def __init__(self):
        self.core = EventStore(EVENT_STORE_REDIS_HOST, EVENT_STORE_REDIS_PORT)
        self.subscribers = {}

    def publish(self, request, context):
        """
        Publish an event.

        :param request: The client request.
        :param context: The client context.
        :return: An entry ID.
        """
        entry_id = self.core.add(request.event_topic, json.loads(request.event_info))

        return PublishResponse(entry_id=entry_id)

    def subscribe(self, request, context):
        """
        Subscribe to an event.

        :param request: The client request.
        :param context: The client context.
        :return: Notification stream.
        """
        self.subscribers[(request.event_topic, context.peer())] = True

        if request.group_name:
            self.core.create_group(request.event_topic, request.group_name)

        last_id = None
        while self.subscribers[(request.event_topic, context.peer())]:

            if request.group_name:
                result = self.core.read_group(
                    request.event_topic, context.peer(), request.group_name, _no_ack=True
                )
            else:
                result = self.core.read(
                    request.event_topic, last_id
                )

            for stream_name, entries in result:
                for entry_id, entry in entries:
                    last_id = entry_id
                    yield Notification(
                        event_id=entry['event_id'],
                        event_ts=float(last_id.replace('-', '.')),
                        event_action=entry['event_action'],
                        event_data=entry['event_data']
                    )

    def unsubscribe(self, request, context):
        """
        Unsubscribe from an event.

        :param request: The client request.
        :param context: The client context.
        :return: Success.
        """
        self.subscribers[(request.event_topic, context.peer())] = False

        return UnsubscribeResponse(success=True)

    def get(self, request, context):
        """
        Get all events for a topic.

        :param request: The client request.
        :param context: The client context.
        :return: A list with all entities or None.
        """
        events = self.core.get(request.event_topic)

        return GetResponse(events=json.dumps(events) if events else None)


EVENT_STORE_REDIS_HOST = os.getenv('EVENT_STORE_REDIS_HOST', 'localhost')
EVENT_STORE_REDIS_PORT = int(os.getenv('EVENT_STORE_REDIS_PORT', '6379'))
EVENT_STORE_LISTEN_PORT = os.getenv('EVENT_STORE_LISTEN_PORT', '50051')
EVENT_STORE_MAX_WORKERS = int(os.getenv('EVENT_STORE_MAX_WORKERS', '10'))

EVENT_STORE_ADDRESS = '[::]:{}'.format(EVENT_STORE_LISTEN_PORT)
EVENT_STORE_SLEEP_INTERVAL = 1
EVENT_STORE_GRACE_INTERVAL = 0
EVENT_STORE_RUNNING = True


def serve():
    """
    Run the gRPC server.
    """
    server = grpc.server(ThreadPoolExecutor(max_workers=EVENT_STORE_MAX_WORKERS))
    try:
        add_EventStoreServicer_to_server(EventStoreServer(), server)
        server.add_insecure_port(EVENT_STORE_ADDRESS)
        server.start()
    except Exception as e:
        logging.error(e)

    logging.info('serving ...')
    try:
        while EVENT_STORE_RUNNING:
            time.sleep(EVENT_STORE_SLEEP_INTERVAL)
    except (InterruptedError, KeyboardInterrupt):
        server.stop(EVENT_STORE_GRACE_INTERVAL)

    logging.info('done.')


def stop():
    """
    Stop the gRPC server.
    """
    global EVENT_STORE_RUNNING
    EVENT_STORE_RUNNING = False


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    signal.signal(signal.SIGINT, lambda n, h: stop())
    signal.signal(signal.SIGTERM, lambda n, h: stop())

    serve()
