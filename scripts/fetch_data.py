import requests

def fetch_data():
    print("Fetching data...")
    users_data = requests.get('https://dummyjson.com/users?limit=50').json()['users']
    carts_data = requests.get('https://dummyjson.com/carts').json()['carts']
    products_data = requests.get('https://dummyjson.com/products?limit=100').json()['products']
    
    return {
        "users_data": users_data,
        "carts_data": carts_data,
        "products_data": products_data
    }
    