import random
import string
import threading
import time
import uuid

from event_store_client import EventStoreClient, create_event


def get_any_id(_entities, _but=None):
    """
    Get a random id out of entities.

    :param _entities: The entities to chose from.
    :param _but: Exclude this entity.
    :return: The randomly chosen ID.
    """
    _id = None
    while not _id:
        idx = random.randrange(len(_entities))
        entity = _entities[idx]
        _id = entity['entity_id'] if entity['entity_id'] != _but else None

    return _id


def create_billing(_order_id):
    """
    Create a billing entity.

    :param _order_id: The order ID the billing belongs to.
    :return: A dict with the entity properties.
    """
    return {
        'entity_id': str(uuid.uuid4()),
        'order_id': _order_id,
        'done': time.time()
    }


def create_order(_customers, _products):
    """
    Create an order entity.

    :param _customers: The customers of the order.
    :param _products: The products of the order.
    :return: A dict with the entity properties.
    """
    return {
        'entity_id': str(uuid.uuid4()),
        'product_ids': [get_any_id(_products) for _ in range(random.randint(1, 10))],
        'customer_id': get_any_id(_customers)
    }


def create_inventory(_product_id, _amount):
    """
    Create an inventory entity.

    :param _product_id: The product ID the inventory is for.
    :param _amount: The amount of products in the inventory.
    :return: A dict with the entity properties.
    """
    return {
        'entity_id': str(uuid.uuid4()),
        'product_id': _product_id,
        'amount': _amount
    }


def create_customer():
    """
    Create a customer entity.

    :return: A dict with the entity properties.
    """
    name = "".join(random.choice(string.ascii_lowercase) for _ in range(10))

    return {
        'entity_id': str(uuid.uuid4()),
        'name': name.title(),
        'email': "{}@server.com".format(name)
    }


def create_product():
    """
    Create a product entity.

    :return: A dict with the entity properties.
    """
    return {
        'entity_id': str(uuid.uuid4()),
        'name': "".join(random.choice(string.ascii_lowercase) for _ in range(10)),
        'price': random.randint(10, 1000)
    }


es = EventStoreClient()

customers = [create_customer() for _ in range(0, 100)]
products = [create_product() for _ in range(0, 100)]
inventory = [create_inventory(product['entity_id'], 1000) for product in products]
orders = [create_order(customers, products) for _ in range(0, 100)]
billings = [create_billing(order['entity_id']) for order in orders]

for customer in customers:
    es.publish('customer', create_event('entity_created', customer))

for product in products:
    es.publish('product', create_event('entity_created', product))

for inventory in inventory:
    es.publish('inventory', create_event('entity_created', inventory))

for order in orders:
    es.publish('order', create_event('entity_created', order))

for billing in billings:
    es.publish('billing', create_event('entity_created', billing))

# subscribe to event
es.subscribe('order', lambda x: print(f'order received {x}'), _group='order-service')


def order_service(_es):

    # publish event
    es.publish('order', create_event('entity_deleted', orders[0]))

    # get all order events
    order_events = _es.get('order')

    # check result
    assert len(order_events) == 101

    time.sleep(1)


t1 = threading.Thread(target=order_service, args=(es,))
t1.start()
