import random
import threading
import time
import mysql.connector
DB = {"host": "localhost", 
      "user": "root", 
      "password": "", 
      "database": "sensor_db"}
def set_password(password):
    DB["password"] = password
def set_login(host, user, password):
    DB["host"] = host
    DB["user"] = user
    DB["password"] = password
DEVICES = ["Sensor-1", "Sensor-2", "Sensor-3"]
BUCKET_SECONDS = 60
KEEP = 3
SETTINGS = {"window": 20, "threshold": 3.0}
state = {"running": False, "last_id": 0, "last_bucket": None, "spike": None, "error": ""}
def connect(with_db=True):
    cfg = dict(DB)
    if not with_db:
        cfg.pop("database")
    return mysql.connector.connect(**cfg)
def run(sql, args=()):
    con = connect()
    cur = con.cursor()
    cur.execute(sql, args)
    con.commit()
    cur.close()
    con.close()
def query(sql, args=()):
    con = connect()
    cur = con.cursor(dictionary=True)
    cur.execute(sql, args)
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows
def setup():
    con = connect(False)
    cur = con.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS " + DB["database"])
    cur.close()
    con.close()
    run("""CREATE TABLE IF NOT EXISTS readings (
        id BIGINT AUTO_INCREMENT,
        device_id VARCHAR(20),
        voltage DOUBLE,
        ts DATETIME,
        bucket INT,
        PRIMARY KEY (id, bucket),
        KEY (device_id, id))
        PARTITION BY RANGE (bucket) (PARTITION pmax VALUES LESS THAN MAXVALUE)""")
    run("""CREATE TABLE IF NOT EXISTS alerts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        device_id VARCHAR(20),
        ts DATETIME,
        voltage DOUBLE,
        z_score DOUBLE,
        severity VARCHAR(5))""")
    run("""CREATE TABLE IF NOT EXISTS readings_summary (
        device_id VARCHAR(20),
        bucket INT,
        avg_v DOUBLE,
        min_v DOUBLE,
        max_v DOUBLE,
        cnt INT,
        PRIMARY KEY (device_id, bucket))""")
def bucket_now():
    return int(time.time() // BUCKET_SECONDS)
def add_readings(bucket):
    rows = []
    for i, d in enumerate(DEVICES):
        v = random.gauss(230 + i * 1.5, 1.2)
        if state["spike"] == d or random.random() < 0.012:
            v += random.uniform(22, 50)
        rows.append((d, v, bucket))
    state["spike"] = None
    con = connect()
    cur = con.cursor()
    cur.executemany("INSERT INTO readings (device_id, voltage, ts, bucket) VALUES (%s, %s, NOW(), %s)", rows)
    con.commit()
    cur.close()
    con.close()
def detect():
    w = int(SETTINGS["window"])
    sql = f"""SELECT device_id, ts, voltage, (voltage - a) / s AS z FROM (
        SELECT id, device_id, ts, voltage,
               AVG(voltage) OVER w AS a,
               STDDEV_SAMP(voltage) OVER w AS s,
               COUNT(voltage) OVER w AS c
        FROM readings
        WINDOW w AS (PARTITION BY device_id ORDER BY id ROWS BETWEEN {w} PRECEDING AND 1 PRECEDING)
    ) t
    WHERE id > %s AND c >= 5 AND s > 0 AND ABS((voltage - a) / s) > %s"""
    for r in query(sql, (state["last_id"], SETTINGS["threshold"])):
        z = float(r["z"])
        level = "HIGH" if abs(z) > 8 else "LOW"
        run("INSERT INTO alerts (device_id, ts, voltage, z_score, severity) VALUES (%s, %s, %s, %s, %s)",
            (r["device_id"], r["ts"], r["voltage"], z, level))
    state["last_id"] = query("SELECT COALESCE(MAX(id), 0) AS m FROM readings")[0]["m"]
def refresh_summary():
    run("""REPLACE INTO readings_summary
        SELECT device_id, bucket, AVG(voltage), MIN(voltage), MAX(voltage), COUNT(*)
        FROM readings GROUP BY device_id, bucket""")
def maintain(b):
    rows = query("""SELECT PARTITION_NAME AS name FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'readings'""", (DB["database"],))
    names = [r["name"] for r in rows]
    if f"p{b}" not in names:
        run(f"""ALTER TABLE readings REORGANIZE PARTITION pmax INTO (
            PARTITION p{b} VALUES LESS THAN ({b + 1}),
            PARTITION pmax VALUES LESS THAN MAXVALUE)""")
    for n in names:
        if n != "pmax" and int(n[1:]) <= b - KEEP:
            run(f"ALTER TABLE readings DROP PARTITION {n}")
def loop():
    while state["running"]:
        try:
            b = bucket_now()
            if b != state["last_bucket"]:
                maintain(b)
                state["last_bucket"] = b
            add_readings(b)
            detect()
            refresh_summary()
            state["error"] = ""
        except Exception as e:
            state["error"] = str(e)
        time.sleep(1)
def start():
    if state["running"]:
        return
    setup()
    state["last_id"] = query("SELECT COALESCE(MAX(id), 0) AS m FROM readings")[0]["m"]
    state["running"] = True
    threading.Thread(target=loop, daemon=True).start()
def stop():
    state["running"] = False
def inject_spike(device):
    state["spike"] = device
def set_settings(window, threshold):
    SETTINGS["window"] = int(window)
    SETTINGS["threshold"] = float(threshold)
def get_devices():
    return DEVICES
def get_recent(device, n=60):
    w = int(SETTINGS["window"])
    sql = f"""SELECT ts, voltage, a AS roll_avg, s AS roll_std FROM (
        SELECT id, ts, voltage,
               AVG(voltage) OVER w AS a,
               STDDEV_SAMP(voltage) OVER w AS s
        FROM readings WHERE device_id = %s
        WINDOW w AS (ORDER BY id ROWS BETWEEN {w} PRECEDING AND 1 PRECEDING)
    ) t ORDER BY id DESC LIMIT %s"""
    return query(sql, (device, n))[::-1]
def get_alerts(limit=7):
    return query("SELECT ts, device_id, voltage, z_score, severity FROM alerts ORDER BY id DESC LIMIT %s", (limit,))
def get_partitions():
    return query("SELECT CONCAT('p', bucket) AS name, COUNT(*) AS row_count FROM readings GROUP BY bucket ORDER BY bucket")
def get_summary(device, limit=6):
    return query("""SELECT bucket, avg_v, min_v, max_v, cnt FROM readings_summary
        WHERE device_id = %s ORDER BY bucket DESC LIMIT %s""", (device, limit))
def get_stats():
    rows_now = query("SELECT COUNT(*) AS n FROM readings")[0]["n"]
    alerts_total = query("SELECT COUNT(*) AS n FROM alerts")[0]["n"]
    return {"rows_now": rows_now, "alerts_total": alerts_total, "error": state["error"]}
if __name__ == "__main__":
    import getpass
    set_password(getpass.getpass("MySQL password: "))
    start()
    while True:
        time.sleep(1)
        print(get_stats())
        print(get_alerts(3))