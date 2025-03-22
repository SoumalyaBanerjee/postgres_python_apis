import psycopg2

# Replace with your Render database connection details
DB_URL = "postgresql://soumalya:v4t55tNSZli8mbGZnSQhvE4IhlkhQLsj@dpg-cvf6i6tsvqrc73cteo3g-a.singapore-postgres.render.com/thrift_9efb"
try:
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()
    print("Connected to PostgreSQL database!")
    
    # Create a sample table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        )
    """)
    conn.commit()
    print("Table 'users' created successfully.")

    # Insert sample records
    cursor.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Alice", "alice@example.com"))
    cursor.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Bob", "bob@example.com"))
    conn.commit()
    print("Inserted records successfully.")

    # Fetch and print records
    cursor.execute("SELECT * FROM users")
    for row in cursor.fetchall():
        print(row)

    cursor.close()
    conn.close()
except Exception as e:
    print("Error:", e)
