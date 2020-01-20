import threading
import time

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

    def publish(self, _topic, _info):
        """
        Publish an event.

        :param _topic: The event topic.
        :param _info: A dict with the event information.
        :return: The entry ID.
        """
        return self.redis.xadd(
            f'events:{_topic}',
            _info,
            id='{0:.6f}'.format(time.time()).replace('.', '-')
        )

    def read(self, _last_id, _topic, _block=100):
        """
        Read new event stream entries.

        :param _last_id: The ID of the last entry read.
        :param _topic: The event topic.
        :param _block: The time to block in ms or None, defaults to 1000.
        :return: A list of event entries or None.
        """
        return self.redis.xread({f'events:{_topic}': _last_id}, block=_block)

    def get(self, _topic):
        """
        Get all events for a topic.

        :param _topic: The event topic.
        :return:
        """
        return self.redis.xrange(f'events:{_topic}')
