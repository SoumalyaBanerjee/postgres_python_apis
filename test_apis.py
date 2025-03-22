import requests

BASE_URL = "https://your-fastapi-app.onrender.com"  # Replace with your actual URL

# Sample data for bulk insert
users = [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"}
]

# Bulk Insert Users
def bulk_insert():
    response = requests.post(f"{BASE_URL}/users/bulk-insert", json=users)
    print("Bulk Insert Response:", response.json())

# Get Users
def get_users():
    response = requests.get(f"{BASE_URL}/users")
    print("Get Users Response:", response.json())

# Sample data for bulk update (Replace IDs with real ones from get_users)
updated_users = [
    {"id": 1, "name": "Alice Updated", "email": "alice_updated@example.com"},
    {"id": 2, "name": "Bob Updated", "email": "bob_updated@example.com"}
]

# Bulk Update Users
def bulk_update():
    response = requests.put(f"{BASE_URL}/users/bulk-update", json=updated_users)
    print("Bulk Update Response:", response.json())

# Sample data for bulk delete (Replace IDs with real ones)
delete_users = [{"id": 1}, {"id": 2}]

# Bulk Delete Users
def bulk_delete():
    response = requests.delete(f"{BASE_URL}/users/bulk-delete", json=delete_users)
    print("Bulk Delete Response:", response.json())

if __name__ == "__main__":
    bulk_insert()
    get_users()
    bulk_update()
    get_users()
    bulk_delete()
    get_users()
