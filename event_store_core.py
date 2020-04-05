import time

import redis

EVENT_STREAM_NAME = 'events:{}'
EVENT_STREAM_ID = '{0:.6f}'


class EventStore(object):
    """
    Event Store class.
    """

    def __init__(self, host='localhost', port=6379):
        """
        :param host: The Redis host.
        :param port: The Redis port.
        """
        self.redis = redis.StrictRedis(decode_responses=True, host=host, port=port)

    def add(self, _topic, _info):
        """
        Add an event to the stream.

        :param _topic: The event topic.
        :param _info: A dict with the event information.
        :return: The entry ID, i.e. timestamp in ms.
        """
        return self.redis.xadd(
            EVENT_STREAM_NAME.format(_topic),
            _info,
            id=EVENT_STREAM_ID.format(time.time()).replace('.', '-')
        )

    def get(self, _topic):
        """
        Get all events for a topic.

        :param _topic: The event topic.
        :return:
        """
        return self.redis.xrange(EVENT_STREAM_NAME.format(_topic))

    def read(self, _topic, _name, _last_id=None, _block=1000):
        """
        Read new event stream entries.

        :param _topic: The event topic.
        :param _name: The name of the consumer.
        :param _last_id: Optional ID of the last entry read.
        :param _block: The time to block in ms, defaults to 1000.
        :return: A list of event entries or None if timed out.
        """
        last_id = _last_id if _last_id else '>'

        try:
            self.redis.xgroup_create(EVENT_STREAM_NAME.format(_topic), _topic, mkstream=True)
        except redis.ResponseError as e:
            if 'BUSYGROUP' not in e.args[0]:
                raise e

        return self.redis.xreadgroup(_topic, _name, {EVENT_STREAM_NAME.format(_topic): last_id}, block=_block)
