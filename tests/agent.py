import threading
import uuid
import random
import string
import time

from event_store_client import EventStoreClient


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
        _id = entity['id'] if entity['id'] != _but else None
    return _id


def create_billing(_order_id):
    """
    Create a billing entity.

    :param _order_id: The order ID the billing belongs to.
    :return: A dict with the entity properties.
    """
    return {
        'id': str(uuid.uuid4()),
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
        'id': str(uuid.uuid4()),
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
        'id': str(uuid.uuid4()),
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
        'id': str(uuid.uuid4()),
        'name': name.title(),
        'email': "{}@server.com".format(name)
    }


def create_product():
    """
    Create a product entity.

    :return: A dict with the entity properties.
    """
    return {
        'id': str(uuid.uuid4()),
        'name': "".join(random.choice(string.ascii_lowercase) for _ in range(10)),
        'price': random.randint(10, 1000)
    }


es = EventStoreClient()
es.activate_entity_cache('order')
es.activate_entity_cache('product')
es.activate_entity_cache('customer')
es.activate_entity_cache('billing')
es.activate_entity_cache('inventory')

customers = [create_customer() for _ in range(0, 100)]
products = [create_product() for _ in range(0, 100)]
inventory = [create_inventory(product['id'], 1000) for product in products]
orders = [create_order(customers, products) for _ in range(0, 100)]
billings = [create_billing(order['id']) for order in orders]

for customer in customers:
    es.publish('customer', 'created', **customer)

for product in products:
    es.publish('product', 'created', **product)

for inventory in inventory:
    es.publish('inventory', 'created', **inventory)

for order in orders:
    es.publish('order', 'created', **order)

for billing in billings:
    es.publish('billing', 'created', **billing)


def order_service():

    order = es.find_one('order', orders[0]['id'])

    print(order)

    for i in range(0, 100):
        es.publish('order', 'deleted', **orders[i])

    found = es.find_one('order', orders[1]['id'])
    assert not found


t1 = threading.Thread(target=order_service)
t1.start()
