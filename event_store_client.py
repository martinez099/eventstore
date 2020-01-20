import json
import os
import threading
import uuid

import grpc

from event_store_pb2 import PublishRequest, SubscribeRequest, GetRequest
from event_store_pb2_grpc import EventStoreStub

EVENT_STORE_HOSTNAME = os.getenv('EVENT_STORE_HOSTNAME', 'localhost')
EVENT_STORE_PORTNR = os.getenv('EVENT_STORE_PORTNR', '50051')


def create_event(_action, _data):
    """
    Create an event.

    :param _action: The event action.
    :param _data: A dict with the event data.
    :return: A dict with the event information.
    """
    return {
        'event_id': str(uuid.uuid4()),
        'event_action': _action,
        'event_data': json.dumps(_data)
    }


def deduce_entities(_events):
    """
    Deduce entities from events.

    :param _events: The event list.
    :return: A dict mapping entity ID -> entity data.
    """
    # get 'created' events
    created = {json.loads(e[1]['event_data'])['entity_id']: json.loads(e[1]['event_data'])
               for e in filter(lambda x: x[1]['event_action'] == 'entity_created', _events)}

    # del 'deleted' events
    deleted = {json.loads(e[1]['event_data'])['entity_id']: json.loads(e[1]['event_data'])
               for e in filter(lambda x: x[1]['event_action'] == 'entity_deleted', _events)}

    for d_id, d_data in deleted.items():
        del created[d_id]

    # set 'updated' events
    updated = {json.loads(e[1]['event_data'])['entity_id']: json.loads(e[1]['event_data'])
               for e in filter(lambda x: x[1]['event_action'] == 'entity_updated', _events)}

    for u_id, u_data in updated.items():
        created[u_id] = u_data

    return created


class EventStoreClient(object):
    """
    Event Store Client class.
    """

    def __init__(self):
        host, port = EVENT_STORE_HOSTNAME, EVENT_STORE_PORTNR
        self.channel = grpc.insecure_channel('{}:{}'.format(host, port))
        self.stub = EventStoreStub(self.channel)
        self.subscribers = {}

    def __del__(self):
        self.channel.close()

    def publish(self, _topic, _info):
        """
        Publish an event.

        :param _topic: The event topic.
        :param _info: A dict with the event information.
        :return: The entry ID.
        """
        response = self.stub.publish(PublishRequest(
            event_topic=_topic,
            event_info=json.dumps(_info)
        ))

        return response.entry_id

    def subscribe(self, _topic, _handler):
        """
        Subscribe to an event channel.

        :param _topic: The event topic.
        :param _handler: The event handler.
        :return: Success.
        """
        if _topic in self.subscribers:
            self.subscribers[_topic].add_handler(_handler)
        else:
            subscriber = Subscriber(_topic, _handler, self.stub)
            subscriber.start()
            self.subscribers[_topic] = subscriber

        return True

    def unsubscribe(self, _topic, _handler):
        """
        Unsubscribe from an event channel.

        :param _topic: The event topic.
        :param _handler: The event handler.
        :return: Success.
        """
        subscriber = self.subscribers.get(_topic)
        if not subscriber:
            return False

        subscriber.rem_handler(_handler)
        if not subscriber:
            subscriber.stop()
            del self.subscribers[_topic]

        return True

    def get(self, _topic):
        """
        Get events for a topic, optional for a given action.

        :param _topic: The event topic, i.e name of entity.
        :return: A list with entities, optional for a given action.
        """
        request = GetRequest(event_topic=_topic)
        response = self.stub.get(request)

        return json.loads(response.events) if response.events else None


class Subscriber(threading.Thread):
    """
    Subscriber Thread class.
    """

    def __init__(self, _topic, _handler, _stub):
        """
        :param _topic: The topic to subscirbe to.
        :param _handler: A handler function.
        """
        super(Subscriber, self).__init__()
        self._running = False
        self.subscribed = True
        self.handlers = [_handler]
        self.topic = _topic
        self.stub = _stub

    def __len__(self):
        return len(self.handlers)

    def run(self):
        """
        Poll the event stream and call each handler with each entry returned.
        """
        if self._running:
            return

        self._running = True
        while self.subscribed:
            request = SubscribeRequest(event_topic=self.topic)
            for item in self.stub.subscribe(request):
                for handler in self.handlers:
                    handler(item)
        self._running = False

    def stop(self):
        """
        Stop polling the event stream.
        """
        self.subscribed = False

    def add_handler(self, _handler):
        """
        Add an event handler.

        :param _handler: The event handler function.
        """
        self.handlers.append(_handler)

    def rem_handler(self, _handler):
        """
        Remove an event handler.

        :param _handler: The event handler function.
        """
        self.handlers.remove(_handler)
