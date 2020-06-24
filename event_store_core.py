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

    def read(self, _topic, _last_id=None, _block=1000):
        """
        Read from a stream. This is a blocking operation.

        :param _topic:
        :param _last_id:
        :param _block:
        :return:
        """
        last_id = _last_id if _last_id else '$'

        return self.redis.xread({EVENT_STREAM_NAME.format(_topic): last_id}, block=_block)

    def create_group(self, _topic, _name):
        """
        Create a consumer group, ignore if already exists.

        :param _topic:
        :param _name:
        :return:
        """
        try:
            self.redis.xgroup_create(EVENT_STREAM_NAME.format(_topic), _name, mkstream=True)
        except redis.ResponseError as e:
            if 'BUSYGROUP' not in e.args[0]:
                raise e

    def read_group(self, _topic, _name, _group, _block=1000, _no_ack=False):
        """
        Read new event stream entries from a group.

        :param _topic: The event topic.
        :param _name: The name of the consumer.
        :param _group: The consumer group name.
        :param _block: The time to block in ms, defaults to 1000.
        :param _no_ack: Boolean if acknowledge is required.
        :return: A list of event entries or None if timed out.
        """
        return self.redis.xreadgroup(
            _group, _name, {EVENT_STREAM_NAME.format(_topic): '>'}, block=_block, noack=_no_ack
        )

    def ack_group(self, _topic, _group, _ids):
        """
        Acknowledge processing of a group event.

        :param _topic:
        :param _group:
        :param _ids:
        :return:
        """
        return self.redis.xack(EVENT_STREAM_NAME.format(_topic), _group, _ids)
