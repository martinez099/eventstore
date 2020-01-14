import json
import threading
import time
import uuid

from redis import StrictRedis


class EventStore(object):
    """
    Event Store class.
    """

    def __init__(self, host='localhost', port=6379):
        """
        :param host: The Redis host.
        :param port: The Redis port.
        """
        self.redis = StrictRedis(decode_responses=True, host=host, port=port)
        self.subscribers = {}

    def publish(self, _topic, _action, _data):
        """
        Publish an event.

        :param _topic: The event topic.
        :param _action: The event action.
        :param _data: The event data.
        :return: The entry ID.
        """
        key = f'events:{_topic}'
        entry_id = self.redis.xadd(
            key,
            {
                'event_id': str(uuid.uuid4()),
                'event_action': _action,
                'event_data': json.dumps(_data)
            },
            id='{0:.6f}'.format(time.time()).replace('.', '-')
        )

        return entry_id

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
            subscriber = Subscriber(_topic, _handler, self.redis)
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

    def read(self, _last_id, _topic, _block=1000):
        """
        Read new event stream entries.

        :param _last_id: The ID of the last entry read.
        :param _topic: The event topic.
        :param _block: The time to block in ms or None, defaults to 1000.
        :return: A list of event entries or None.
        """
        return self.redis.xread({f'events:{_topic}': _last_id}, block=_block)

    def get(self, _topic, _action=None):
        """
        Get all events for a topic, optional for a given action.

        :param _topic: The event topic.
        :param _action: The event action, defaults to None (i.e. all events).
        :return:
        """
        all_events = self.redis.xrange(f'events:{_topic}')
        if _action:
            return list(filter(lambda x: x[1]['event_action'] == _action, all_events))
        return all_events


class Subscriber(threading.Thread):
    """
    Subscriber Thread class.
    """

    def __init__(self, _topic, _handler, _redis):
        """
        :param _topic: The topic to subscirbe to.
        :param _handler: A handler function.
        :param _redis: A Redis instance.
        """
        super(Subscriber, self).__init__()
        self._running = False
        self.key = f'events:{_topic}'
        self.subscribed = True
        self.handlers = [_handler]
        self.redis = _redis
        self.last_id = '$'

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
            for item in self._read_stream():
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

    def _read_stream(self):
        """
        Get next entry from the stream.

        :return: A stream entry.
        """
        streams = {self.key: self.last_id}
        for stream_name, entries in self.redis.xread(streams, block=1000):
            for entry_id, entry in entries:
                self.last_id = entry_id
                yield entry
