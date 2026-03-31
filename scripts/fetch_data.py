import requests
import random
import uuid
from datetime import datetime, timedelta

def fetch_data():
    """
    Datele sunt extrase din:
        -   API
        -   Generate (Mock Data)
    """
    print("Fetching data from API...")
    users_data = requests.get('https://dummyjson.com/users?limit=50').json()['users']
    carts_data = requests.get('https://dummyjson.com/carts').json()['carts']
    products_data = requests.get('https://dummyjson.com/products?limit=100').json()['products']

    user_ids = [u['id'] for u in users_data]
    product_ids = [p['id'] for p in products_data]

    print("Generating mock data...")

    addresses = generate_addresses(user_ids)
    courier_companies = generate_courier_companies()
    couriers = generate_couriers(courier_companies)
    warehouses = generate_warehouses()
    inventory = generate_inventory(product_ids, warehouses)
    orders, order_items = generate_orders(carts_data, addresses, products_data)
    payments = generate_payments(orders)
    shipments, shipment_items, shipment_statuses = generate_shipments(
        orders, order_items, couriers, warehouses
    )

    return {
        "users_data":  users_data,
        "products_data": products_data,
        "carts_data": carts_data,
        "addresses":  addresses,
        "courier_companies": courier_companies,
        "couriers": couriers,
        "warehouses": warehouses,
        "inventory": inventory,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
        "shipments": shipments,
        "shipment_items": shipment_items,
        "shipment_statuses": shipment_statuses,
    }

def generate_addresses(user_ids):
    """1, 2 adrese per utilizator"""
    cities = ['București', 'Cluj-Napoca', 'Iași', 'Timișoara',
                'Brașov', 'Constanța', 'Craiova', 'Galați', 'Sibiu', 'Oradea']
    streets = ['Calea Victoriei', 'Bd. Unirii', 'Str. Mihai Eminescu',
                'Bd. Magheru', 'Str. Independenței', 'Aleea Trandafirilor',
                'Str. Republicii', 'Bd. Decebal', 'Calea Floreasca']

    addresses = []
    addr_id = 1
    for user_id in user_ids:
        for _ in range(random.randint(1, 2)):
            addresses.append({
                'id': addr_id,
                'user_id': user_id,
                'street': f'{random.choice(streets)} nr. {random.randint(1, 200)}',
                'city': random.choice(cities),
                'postal_code': str(random.randint(100000, 999999)),
                'country':'Romania',
            })
            addr_id += 1
    return addresses

def generate_courier_companies():
    return [
        {'id': 1, 'name': 'FanCourier', 'contact_email': 'contact@fancourier.ro'},
        {'id': 2, 'name': 'Cargus', 'contact_email': 'contact@cargus.ro'},
        {'id': 3, 'name': 'DHL Romania', 'contact_email': 'contact@dhl.ro'},
        {'id': 4, 'name': 'GLS Romania', 'contact_email': 'contact@gls-romania.ro'},
        {'id': 5, 'name': 'Sameday', 'contact_email': 'contact@sameday.ro'},
    ]


def generate_couriers(courier_companies):
    first_names = ['Andrei', 'Ion', 'Mihai', 'Alexandru', 'Cristian',
                    'Bogdan', 'Daniel', 'Florin', 'George', 'Radu',
                    'Elena', 'Maria', 'Ioana', 'Ana', 'Gabriela']
    last_names = ['Popescu', 'Ionescu', 'Popa', 'Stan', 'Gheorghe',
                    'Stoica', 'Constantin', 'Marin', 'Dumitrescu', 'Dima']
    vehicle_types  = ['bike', 'car', 'van', 'truck']
    vehicle_weights = [0.15, 0.40, 0.30, 0.15]

    couriers = []
    for i in range(1, 21):
        company = random.choice(courier_companies)
        couriers.append({
            'id': i,
            'company_id':company['id'],
            'full_name': f'{random.choice(first_names)} {random.choice(last_names)}',
            'phone': f'07{random.randint(10000000, 99999999)}',
            'vehicle_type': random.choices(vehicle_types, weights=vehicle_weights)[0],
        })
    return couriers


def generate_warehouses():
    return [
        {'id': 1, 'name': 'Depozit Central București',  'city': 'București',   'address': 'Bd. Timișoara 26'},
        {'id': 2, 'name': 'Depozit Nord Cluj',           'city': 'Cluj-Napoca', 'address': 'Str. Fabricii 12'},
        {'id': 3, 'name': 'Depozit Est Iași',            'city': 'Iași',        'address': 'Calea Chișinăului 45'},
        {'id': 4, 'name': 'Depozit Vest Timișoara',      'city': 'Timișoara',   'address': 'Str. Industrială 8'},
        {'id': 5, 'name': 'Depozit Sud Craiova',         'city': 'Craiova',     'address': 'Bd. Dacia 101'},
    ]


def generate_inventory(product_ids, warehouses):
    """Fiecare produs apare in 1–3 depozite"""
    inventory = []
    inv_id = 1
    for product_id in product_ids:
        selected_warehouses = random.sample(warehouses, k=random.randint(1, 3))
        for wh in selected_warehouses:
            stock = random.randint(0, 200)
            reserved = random.randint(0, min(stock, 30))
            inventory.append({
                'id': inv_id,
                'product_id': product_id,
                'warehouse_id': wh['id'],
                'stock_level': stock,
                'reserved_quantity': reserved,
                'min_stock_level': random.randint(5, 20),
            })
            inv_id += 1
    return inventory


def generate_orders(carts_data, addresses, products_data):
    """~70% din cosuri devin comenzi finalizate"""
    product_price_map = {p['id']: p['price'] for p in products_data}
    addr_by_user = {}
    for a in addresses:
        addr_by_user.setdefault(a['user_id'], []).append(a)

    statuses = ['pending', 'paid', 'cancelled', 'shipped', 'delivered']
    status_weights = [0.10, 0.15, 0.10, 0.25, 0.40]

    orders = []
    order_items = []
    order_id = 1
    oi_id = 1

    selected = random.sample(carts_data, k=int(len(carts_data) * 0.7))

    for cart in selected:
        user_addrs = addr_by_user.get(cart['userId'], [])
        if not user_addrs:
            continue

        addr = random.choice(user_addrs)
        status = random.choices(statuses, weights=status_weights)[0]
        created_at = datetime.now() - timedelta(days=random.randint(1, 365))

        items_total = sum(
            product_price_map.get(item['id'], item['price']) * item['quantity']
            for item in cart['products']
        )
        discount_rate = random.uniform(0.05, 0.25)
        discounted_total = round(items_total * (1 - discount_rate), 2)

        orders.append({
            'id': order_id,
            'user_id': cart['userId'],
            'cart_id': cart['id'],
            'delivery_address_id': addr['id'],
            'total': round(items_total, 2),
            'discounted_total': discounted_total,
            'status':  status,
            'created_at': created_at,
        })

        for item in cart['products']:
            price = product_price_map.get(item['id'], item['price'])
            order_items.append({
                'id': oi_id,
                'order_id': order_id,
                'product_id': item['id'],
                'quantity': item['quantity'],
                'price': price,
            })
            oi_id += 1

        order_id += 1

    return orders, order_items


def generate_payments(orders):
    """O plata per comanda, cu status derivat din statusul comenzii"""
    methods = ['card', 'cash', 'transfer']
    payments = []
    pay_id = 1

    for order in orders:
        if order['status'] in ('paid', 'shipped', 'delivered'):
            pay_status = 'completed'
        elif order['status'] == 'cancelled':
            pay_status = random.choice(['failed', 'refunded'])
        else:
            pay_status = 'pending'

        paid_at = (
            order['created_at'] + timedelta(hours=random.randint(1, 24))
            if pay_status == 'completed' else None
        )

        payments.append({
            'id': pay_id,
            'order_id': order['id'],
            'amount': order['discounted_total'],
            'method': random.choice(methods),
            'transaction_id': str(uuid.uuid4()),
            'status': pay_status,
            'paid_at': paid_at,
        })
        pay_id += 1

    return payments


def generate_shipments(orders, order_items, couriers, warehouses):
    """Livrari pentru comenzile cu status shipped sau delivered"""
    oi_by_order   = {}
    for oi in order_items:
        oi_by_order.setdefault(oi['order_id'], []).append(oi)

    transit_cities = ['București', 'Ploiești', 'Pitești', 'Sinaia',
                        'Cluj-Napoca', 'Sibiu', 'Brașov', 'Iași', 'Focșani']

    shipments         = []
    shipment_items    = []
    shipment_statuses = []
    ship_id = si_id = ss_id = 1

    shippable = [o for o in orders if o['status'] in ('shipped', 'delivered')]

    for order in shippable:
        courier   = random.choice(couriers)
        warehouse = random.choice(warehouses)
        created_at = order['created_at'] + timedelta(hours=random.randint(2, 48))

        shipments.append({
            'id': ship_id,
            'order_id': order['id'],
            'courier_id': courier['id'],
            'origin_warehouse_id': warehouse['id'],
            'tracking_number': f"TRK-{str(uuid.uuid4())[:8].upper()}",
            'created_at': created_at,
        })

        for oi in oi_by_order.get(order['id'], []):
            shipment_items.append({
                'id': si_id,
                'shipment_id': ship_id,
                'order_item_id': oi['id'],
                'quantity': oi['quantity'],
            })
            si_id += 1

        seq = (['pending', 'picked_up', 'in_transit', 'delivered']
               if order['status'] == 'delivered'
               else ['pending', 'picked_up', 'in_transit'])

        t = created_at
        for status in seq:
            obs = ('Clientul nu a răspuns la interfon'
                   if status == 'rejected' else None)
            shipment_statuses.append({
                'id': ss_id,
                'shipment_id': ship_id,
                'status': status,
                'location': random.choice(transit_cities),
                'updated_at': t,
                'observation': obs,
            })
            ss_id += 1
            t += timedelta(hours=random.randint(4, 24))

        ship_id += 1

    return shipments, shipment_items, shipment_statuses