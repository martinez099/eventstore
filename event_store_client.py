import json
import logging
import os
import threading
import uuid

import grpc

from event_store_pb2 import PublishRequest, SubscribeRequest, UnsubscribeRequest, GetRequest
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

    def subscribe(self, _topic, _handler, _group=None):
        """
        Subscribe to an event topic.

        :param _topic: The event topic.
        :param _handler: The event handler.
        :param _group: Optional group name.
        :return: Success.
        """
        if _topic in self.subscribers:
            self.subscribers[_topic].add_handler(_handler)
        else:
            subscriber = Subscriber(_topic, _handler, self.stub, _group)
            subscriber.start()
            self.subscribers[_topic] = subscriber

        return True

    def unsubscribe(self, _topic, _handler):
        """
        Unsubscribe from an event topic.

        :param _topic: The event topic.
        :param _handler: The event handler.
        :return: Success.
        """
        subscriber = self.subscribers.get(_topic)
        if not subscriber:
            return False

        response = self.stub.unsubscribe(UnsubscribeRequest(event_topic=_topic))

        subscriber.rem_handler(_handler)
        if not subscriber:
            del self.subscribers[_topic]

        return response.success

    def get(self, _topic):
        """
        Get events for a topic.

        :param _topic: The event topic, i.e name of event stream.
        :return: A list with entities.
        """
        response = self.stub.get(GetRequest(event_topic=_topic))

        return json.loads(response.events) if response.events else None


class Subscriber(threading.Thread):
    """
    Subscriber Thread class.
    """

    def __init__(self, _topic, _handler, _stub, _group=None):
        """
        :param _topic: The topic to subscirbe to.
        :param _handler: A handler function.
        :param _group: The name of the subscriber.
        """
        super(Subscriber, self).__init__()
        self._running = False
        self.handlers = [_handler]
        self.topic = _topic
        self.stub = _stub
        self.group = _group

    def __len__(self):
        return len(self.handlers)

    def run(self):
        """
        Poll the event stream and call each handler with each entry returned.
        """
        if self._running:
            return

        self._running = True
        for item in self.stub.subscribe(
                SubscribeRequest(event_topic=self.topic, group_name=self.group)):
            for handler in self.handlers:
                try:
                    handler(item)
                except Exception as e:
                    logging.error(
                        'error calling handler function ({}) for {}.{}: {}'.format(
                            e.__class__.__name__, self.topic, handler.__name__, str(e)
                        )
                    )

        self._running = False

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
