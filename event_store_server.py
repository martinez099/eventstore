import json
import logging
import os
import time
from concurrent import futures

import grpc

from event_store_core import EventStore

from event_store_pb2 import PublishResponse, Notification, UnsubscribeResponse, FindAllResponse, \
    FindOneResponse, ActivateEntityCacheResponse, DeactivateEntityCacheResponse
from event_store_pb2_grpc import EventStoreServicer, add_EventStoreServicer_to_server


class EventStoreServer(EventStoreServicer):
    """
    Event Store Server class.
    """

    def __init__(self):
        self.core = EventStore(EVENT_STORE_REDIS_HOST)
        self.subscribers = {}

    def publish(self, request, context):
        """
        Publish an event.

        :param request: The client request.
        :param context: The client context.
        :return: PublishResponse
        """
        entry_id = self.core.publish(request.event_id, request.event_topic, request.event_action, request.event_entity)

        return PublishResponse(entry_id=entry_id)

    def subscribe(self, request, context):
        """
        Subscribe to an event.

        :param request: The client request.
        :param context: The client context.
        """
        self.subscribers[(request.event_topic, request.event_action, context.peer())] = True

        last_id = '$'
        while self.subscribers[(request.event_topic, request.event_action, context.peer())]:
            for stream_name, entries in self.core.read(last_id, request.event_topic, request.event_action):
                for entry_id, entry in entries:
                    last_id = entry_id
                    yield Notification(
                        event_id=entry['event_id'],
                        event_ts=float(last_id.replace('-', '.')),
                        event_entity=entry['event_entity']
                    )

    def unsubscribe(self, request, context):
        """
        Unsubscribe from an event.

        :param request: The client request.
        :param context: The client context.
        :return: Success.
        """
        self.subscribers[(request.event_topic, request.event_action, context.peer())] = False

        return UnsubscribeResponse(success=True)

    def find_one(self, request, context):
        """
        Find an entity for a topic with an specific id.

        :param request: The client request.
        :param context: The client context.
        :return: A dict with the entity.
        """
        entity = self.core.find_one(request.event_topic, request.event_id)

        return FindOneResponse(entity=json.dumps(entity) if entity else None)

    def find_all(self, request, context):
        """
        Find all entites for a topic.

        :param request: The client request.
        :param context: The client context.
        :return: A list with all entitys.
        """
        entities = self.core.find_all(request.event_topic)

        return FindAllResponse(entities=json.dumps(entities) if entities else None)

    def activate_entity_cache(self, request, context):
        """
        Keep entity cache up to date.

        :param request: The client request.
        :param context: The client context.
        """
        self.core.activate_entity_cache(request.event_topic)

        return ActivateEntityCacheResponse()

    def deactivate_entity_cache(self, request, context):
        """
        Stop keeping entity cache up to date.

        :param request: The client request.
        :param context: The client context.
        """
        self.core.deactivate_entity_cache(request.event_topic)

        return DeactivateEntityCacheResponse()


EVENT_STORE_REDIS_HOST = os.getenv('EVENT_STORE_REDIS_HOST', 'localhost')
EVENT_STORE_LISTEN_PORT = os.getenv('EVENT_STORE_LISTEN_PORT', '50051')
EVENT_STORE_MAX_WORKERS = int(os.getenv('EVENT_STORE_MAX_WORKERS', '10'))

EVENT_STORE_ADDRESS = '[::]:{}'.format(EVENT_STORE_LISTEN_PORT)
EVENT_STORE_SLEEP_INTERVAL = 60 * 60 * 24
EVENT_STORE_GRACE_INTERVAL = 0


def serve():
    """
    Run the gRPC server.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=EVENT_STORE_MAX_WORKERS))
    try:
        add_EventStoreServicer_to_server(EventStoreServer(), server)
        server.add_insecure_port(EVENT_STORE_ADDRESS)
        server.start()
    except Exception as e:
        logging.error(e)

    logging.info('serving ...')
    try:
        while True:
            time.sleep(EVENT_STORE_SLEEP_INTERVAL)
    except (InterruptedError, KeyboardInterrupt):
        server.stop(EVENT_STORE_GRACE_INTERVAL)

    logging.info('done.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve()
