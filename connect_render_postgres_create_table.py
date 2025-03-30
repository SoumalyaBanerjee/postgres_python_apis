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


import psycopg2

# Replace with your Render database connection details
DB_URL = "postgresql://soumalya:v4t55tNSZli8mbGZnSQhvE4IhlkhQLsj@dpg-cvf6i6tsvqrc73cteo3g-a.singapore-postgres.render.com/thrift_9efb"
try:
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()
    print("Connected to PostgreSQL database!")
    
    # Create a sample table
    cursor.execute("""
    DROP TABLE IF EXISTS power_logs;
    CREATE TABLE IF NOT EXISTS power_logs (
            log_id BIGINT NOT NULL UNIQUE,
            device_id INT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            voltage_L1_N DECIMAL(10, 3),
            voltage_L2_N DECIMAL(10, 3),
            voltage_L3_N DECIMAL(10, 3),
            voltage_L1_L2 DECIMAL(10, 3),
            voltage_L2_L3 DECIMAL(10, 3),
            voltage_L3_L1 DECIMAL(10, 3),
            current_L1 DECIMAL(10, 3),
            current_L2 DECIMAL(10, 3),
            current_L3 DECIMAL(10, 3),
            active_power_L1 DECIMAL(10, 3),
            active_power_L2 DECIMAL(10, 3),
            active_power_L3 DECIMAL(10, 3),
            active_power_total DECIMAL(10, 3),
            reactive_power_L1 DECIMAL(10, 3),
            reactive_power_L2 DECIMAL(10, 3),
            reactive_power_L3 DECIMAL(10, 3),
            reactive_power_total DECIMAL(10, 3),
            apparent_power_L1 DECIMAL(10, 3),
            apparent_power_L2 DECIMAL(10, 3),
            apparent_power_L3 DECIMAL(10, 3),
            apparent_power_total DECIMAL(10, 3),
            power_factor_L1 DECIMAL(5, 3),
            power_factor_L2 DECIMAL(5, 3),
            power_factor_L3 DECIMAL(5, 3),
            power_factor_total DECIMAL(5, 3),
            frequency DECIMAL(5, 2),
            temperature DECIMAL(5, 2),
            harmonic_distortion_current DECIMAL(5, 2),
            other_data JSON,
            insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
    conn.commit()
    print("Table 'power_logs' created successfully.")

except Exception as e:
    print("Error while creating table or trigger:", e)
finally:
    # Close communication
    cursor.close()
    conn.close()



conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()
print("Connected to PostgreSQL database!")

# SQL command to create the trigger function
create_trigger_function_query = """
CREATE OR REPLACE FUNCTION update_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_timestamp = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
cursor.execute(create_trigger_function_query)

# SQL command to create the trigger
create_trigger_query = """
CREATE TRIGGER set_update_timestamp
BEFORE UPDATE ON power_logs
FOR EACH ROW
EXECUTE FUNCTION update_timestamp_column();
"""
cursor.execute(create_trigger_query)

conn.commit()
print("Table and trigger created successfully")


def get_db_connection():
    return psycopg2.connect(DB_URL)

# Function to insert records into power_logs
def insert_power_logs(num_records=1000):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for _ in range(num_records):
            query = """
            INSERT INTO power_logs (
                log_id, device_id, timestamp, voltage_L1_N, voltage_L2_N, voltage_L3_N, voltage_L1_L2,
                voltage_L2_L3, voltage_L3_L1, current_L1, current_L2, current_L3, active_power_L1,
                active_power_L2, active_power_L3, active_power_total, reactive_power_L1,
                reactive_power_L2, reactive_power_L3, reactive_power_total, apparent_power_L1,
                apparent_power_L2, apparent_power_L3, apparent_power_total, power_factor_L1,
                power_factor_L2, power_factor_L3, power_factor_total, frequency, temperature,
                harmonic_distortion_current, other_data
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                int(uuid.uuid4().int % (10**18)), random.randint(1000, 9999), datetime.utcnow(), random.uniform(220, 240),
                random.uniform(220, 240), random.uniform(220, 240), random.uniform(380, 420),
                random.uniform(380, 420), random.uniform(380, 420), random.uniform(5, 20),
                random.uniform(5, 20), random.uniform(5, 20), random.uniform(1000, 5000),
                random.uniform(1000, 5000), random.uniform(1000, 5000), random.uniform(3000, 15000),
                random.uniform(500, 2000), random.uniform(500, 2000), random.uniform(500, 2000),
                random.uniform(1500, 6000), random.uniform(1000, 6000), random.uniform(1000, 6000),
                random.uniform(1000, 6000), random.uniform(3000, 18000), random.uniform(0.8, 1.0),
                random.uniform(0.8, 1.0), random.uniform(0.8, 1.0), random.uniform(0.8, 1.0),
                random.uniform(49.5, 50.5), random.uniform(20, 40), random.uniform(2, 10), json.dumps({})
            )
            cursor.execute(query, values)
        conn.commit()
        print(f"Inserted {num_records} records successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting records: {e}")
    finally:
        cursor.close()
        conn.close()

conn = get_db_connection()
cursor = conn.cursor()
insert_power_logs()
cursor.execute("SELECT * FROM power_logs")
records = cursor.fetchall()
records
cursor.close()
conn.close()
