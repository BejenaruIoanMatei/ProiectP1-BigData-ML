import requests

def fetch_data():
    print("Fetching data...")
    users_data = requests.get('https://dummyjson.com/users?limit=50').json()['users']
    return users_data