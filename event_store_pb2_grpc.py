# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import event_store.event_store_pb2 as event__store__pb2


class EventStoreStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.publish = channel.unary_unary(
        '/eventstore.EventStore/publish',
        request_serializer=event__store__pb2.PublishRequest.SerializeToString,
        response_deserializer=event__store__pb2.PublishResponse.FromString,
        )
    self.subscribe = channel.unary_stream(
        '/eventstore.EventStore/subscribe',
        request_serializer=event__store__pb2.SubscribeRequest.SerializeToString,
        response_deserializer=event__store__pb2.Notification.FromString,
        )
    self.unsubscribe = channel.unary_unary(
        '/eventstore.EventStore/unsubscribe',
        request_serializer=event__store__pb2.UnsubscribeRequest.SerializeToString,
        response_deserializer=event__store__pb2.UnsubscribeResponse.FromString,
        )
    self.find_one = channel.unary_unary(
        '/eventstore.EventStore/find_one',
        request_serializer=event__store__pb2.FindOneRequest.SerializeToString,
        response_deserializer=event__store__pb2.FindOneResponse.FromString,
        )
    self.find_all = channel.unary_unary(
        '/eventstore.EventStore/find_all',
        request_serializer=event__store__pb2.FindAllRequest.SerializeToString,
        response_deserializer=event__store__pb2.FindAllResponse.FromString,
        )
    self.activate_entity_cache = channel.unary_unary(
        '/eventstore.EventStore/activate_entity_cache',
        request_serializer=event__store__pb2.ActivateEntityCacheRequest.SerializeToString,
        response_deserializer=event__store__pb2.ActivateEntityCacheResponse.FromString,
        )
    self.deactivate_entity_cache = channel.unary_unary(
        '/eventstore.EventStore/deactivate_entity_cache',
        request_serializer=event__store__pb2.DeactivateEntityCacheRequest.SerializeToString,
        response_deserializer=event__store__pb2.DeactivateEntityCacheResponse.FromString,
        )


class EventStoreServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def publish(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def subscribe(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def unsubscribe(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def find_one(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def find_all(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def activate_entity_cache(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def deactivate_entity_cache(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_EventStoreServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'publish': grpc.unary_unary_rpc_method_handler(
          servicer.publish,
          request_deserializer=event__store__pb2.PublishRequest.FromString,
          response_serializer=event__store__pb2.PublishResponse.SerializeToString,
      ),
      'subscribe': grpc.unary_stream_rpc_method_handler(
          servicer.subscribe,
          request_deserializer=event__store__pb2.SubscribeRequest.FromString,
          response_serializer=event__store__pb2.Notification.SerializeToString,
      ),
      'unsubscribe': grpc.unary_unary_rpc_method_handler(
          servicer.unsubscribe,
          request_deserializer=event__store__pb2.UnsubscribeRequest.FromString,
          response_serializer=event__store__pb2.UnsubscribeResponse.SerializeToString,
      ),
      'find_one': grpc.unary_unary_rpc_method_handler(
          servicer.find_one,
          request_deserializer=event__store__pb2.FindOneRequest.FromString,
          response_serializer=event__store__pb2.FindOneResponse.SerializeToString,
      ),
      'find_all': grpc.unary_unary_rpc_method_handler(
          servicer.find_all,
          request_deserializer=event__store__pb2.FindAllRequest.FromString,
          response_serializer=event__store__pb2.FindAllResponse.SerializeToString,
      ),
      'activate_entity_cache': grpc.unary_unary_rpc_method_handler(
          servicer.activate_entity_cache,
          request_deserializer=event__store__pb2.ActivateEntityCacheRequest.FromString,
          response_serializer=event__store__pb2.ActivateEntityCacheResponse.SerializeToString,
      ),
      'deactivate_entity_cache': grpc.unary_unary_rpc_method_handler(
          servicer.deactivate_entity_cache,
          request_deserializer=event__store__pb2.DeactivateEntityCacheRequest.FromString,
          response_serializer=event__store__pb2.DeactivateEntityCacheResponse.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'eventstore.EventStore', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
