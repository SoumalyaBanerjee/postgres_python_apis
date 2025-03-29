from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import psycopg2

# Database connection details from Render
DB_URL = "postgresql://soumalya:v4t55tNSZli8mbGZnSQhvE4IhlkhQLsj@dpg-cvf6i6tsvqrc73cteo3g-a.singapore-postgres.render.com/thrift_9efb"

# FastAPI app
app = FastAPI()

# Pydantic models for request validation
class User(BaseModel):
    name: str
    email: str

class UserUpdate(BaseModel):
    id: int
    name: str
    email: str

class UserDelete(BaseModel):
    id: int

# Database connection function
def get_db_connection():
    return psycopg2.connect(DB_URL)

# API: Bulk Insert Users
@app.post("/users/bulk-insert")
def bulk_insert_users(users: list[dict]):
    print("Inside bulk insert")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # values = [(user.name, user.email) for user in users]
        # query = "INSERT INTO users (name, email) VALUES %s RETURNING id"
        # psycopg2.extras.execute_values(cursor, query, values)
        for i in users:
            name = i['name']
            email = i['email']
            print(name)
            print(email)
            query = f"INSERT INTO users (name,email) VALUES ('{name}','{email}') RETURNING id"
            cursor.execute(query)
            conn.commit()
        # conn.commit()
        return {"message": "Users inserted successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# API: Bulk Update Users
@app.put("/users/bulk-update")
def bulk_update_users(users: List[dict]):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for user in users:
            name = user['name']
            email = user['email']
            id = user['id']
            query =f"UPDATE users SET name='{name}', email='{email}' WHERE id='{id}'"
            cursor.execute(query)
        conn.commit()
        return {"message": "Users updated successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# API: Bulk Delete Users
@app.delete("/users/bulk-delete")
def bulk_delete_users(users: List[dict]):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for user in users:
            id=user['id']
            query = f"DELETE FROM users WHERE id = '{id}'"
            cursor.execute(query)
            conn.commit()
        return {"message": "Users deleted successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# API: Select Users
@app.get("/users")
def get_users():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM users")
        users = cursor.fetchall()
        return {"users": users}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/")
def home():
    return {"message": "FastAPI is running on Render!"}
