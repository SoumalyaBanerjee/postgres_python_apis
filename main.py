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
def bulk_insert_users(users: List[User]):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        values = [(user.name, user.email) for user in users]
        query = "INSERT INTO users (name, email) VALUES %s RETURNING id"
        psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
        return {"message": "Users inserted successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# API: Bulk Update Users
@app.put("/users/bulk-update")
def bulk_update_users(users: List[UserUpdate]):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for user in users:
            cursor.execute("UPDATE users SET name=%s, email=%s WHERE id=%s", (user.name, user.email, user.id))
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
def bulk_delete_users(users: List[UserDelete]):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        ids = [user.id for user in users]
        cursor.execute("DELETE FROM users WHERE id = ANY(%s)", (ids,))
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
