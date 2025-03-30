from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import psycopg2
import random
import uuid
from datetime import datetime

# Database connection details
DB_URL = "postgresql://soumalya:v4t55tNSZli8mbGZnSQhvE4IhlkhQLsj@dpg-cvf6i6tsvqrc73cteo3g-a.singapore-postgres.render.com/thrift_9efb"

# FastAPI app
app = FastAPI()

# Pydantic model for inserting power logs
class PowerLog(BaseModel):
    log_id: int
    device_id: int
    timestamp: datetime
    voltage_L1_N: float
    voltage_L2_N: float
    voltage_L3_N: float
    voltage_L1_L2: float
    voltage_L2_L3: float
    voltage_L3_L1: float
    current_L1: float
    current_L2: float
    current_L3: float
    active_power_L1: float
    active_power_L2: float
    active_power_L3: float
    active_power_total: float
    reactive_power_L1: float
    reactive_power_L2: float
    reactive_power_L3: float
    reactive_power_total: float
    apparent_power_L1: float
    apparent_power_L2: float
    apparent_power_L3: float
    apparent_power_total: float
    power_factor_L1: float
    power_factor_L2: float
    power_factor_L3: float
    power_factor_total: float
    frequency: float
    temperature: float
    harmonic_distortion_current: float
    other_data: dict

# Database connection function
def get_db_connection():
    return psycopg2.connect(DB_URL)

# API: Bulk Insert Power Logs
@app.post("/power_logs/bulk-insert")
def bulk_insert_power_logs(logs: List[PowerLog]):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for log in logs:
            query = """
                INSERT INTO power_logs (
                    id, log_id, device_id, timestamp, voltage_L1_N, voltage_L2_N, voltage_L3_N,
                    voltage_L1_L2, voltage_L2_L3, voltage_L3_L1, current_L1, current_L2, current_L3,
                    active_power_L1, active_power_L2, active_power_L3, active_power_total,
                    reactive_power_L1, reactive_power_L2, reactive_power_L3, reactive_power_total,
                    apparent_power_L1, apparent_power_L2, apparent_power_L3, apparent_power_total,
                    power_factor_L1, power_factor_L2, power_factor_L3, power_factor_total,
                    frequency, temperature, harmonic_distortion_current, other_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                str(uuid.uuid4()), log.log_id, log.device_id, log.timestamp,
                log.voltage_L1_N, log.voltage_L2_N, log.voltage_L3_N, log.voltage_L1_L2, log.voltage_L2_L3, log.voltage_L3_L1,
                log.current_L1, log.current_L2, log.current_L3, log.active_power_L1, log.active_power_L2, log.active_power_L3, log.active_power_total,
                log.reactive_power_L1, log.reactive_power_L2, log.reactive_power_L3, log.reactive_power_total,
                log.apparent_power_L1, log.apparent_power_L2, log.apparent_power_L3, log.apparent_power_total,
                log.power_factor_L1, log.power_factor_L2, log.power_factor_L3, log.power_factor_total,
                log.frequency, log.temperature, log.harmonic_distortion_current, log.other_data
            )
            cursor.execute(query, values)
        conn.commit()
        return {"message": "Power logs inserted successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# API: Bulk Update Power Logs
@app.put("/power_logs/bulk-update")
def bulk_update_power_logs(logs: List[PowerLog]):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for log in logs:
            query = """
                UPDATE power_logs SET
                    device_id=%s, timestamp=%s, voltage_L1_N=%s, voltage_L2_N=%s, voltage_L3_N=%s,
                    voltage_L1_L2=%s, voltage_L2_L3=%s, voltage_L3_L1=%s, current_L1=%s, current_L2=%s,
                    current_L3=%s, active_power_L1=%s, active_power_L2=%s, active_power_L3=%s, active_power_total=%s,
                    reactive_power_L1=%s, reactive_power_L2=%s, reactive_power_L3=%s, reactive_power_total=%s,
                    apparent_power_L1=%s, apparent_power_L2=%s, apparent_power_L3=%s, apparent_power_total=%s,
                    power_factor_L1=%s, power_factor_L2=%s, power_factor_L3=%s, power_factor_total=%s,
                    frequency=%s, temperature=%s, harmonic_distortion_current=%s, other_data=%s
                WHERE log_id=%s
            """
            values = (
                log.device_id, log.timestamp, log.voltage_L1_N, log.voltage_L2_N, log.voltage_L3_N,
                log.voltage_L1_L2, log.voltage_L2_L3, log.voltage_L3_L1, log.current_L1, log.current_L2,
                log.current_L3, log.active_power_L1, log.active_power_L2, log.active_power_L3, log.active_power_total,
                log.reactive_power_L1, log.reactive_power_L2, log.reactive_power_L3, log.reactive_power_total,
                log.apparent_power_L1, log.apparent_power_L2, log.apparent_power_L3, log.apparent_power_total,
                log.power_factor_L1, log.power_factor_L2, log.power_factor_L3, log.power_factor_total,
                log.frequency, log.temperature, log.harmonic_distortion_current, log.other_data,
                log.log_id
            )
            cursor.execute(query, values)
        conn.commit()
        return {"message": "Power logs updated successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# API: Select Power Logs
@app.get("/power_logs")
def get_power_logs():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM power_logs")
        logs = cursor.fetchall()
        return {"power_logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
