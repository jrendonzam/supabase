import mysql.connector
from mysql.connector import Error
import os
import time

def get_db_connection():
    attempts = 5
    last_exception = None
    for i in range(attempts):
        try:
            conn = mysql.connector.connect(
                host=os.getenv("MYSQL_HOST"),
                user=os.getenv("MYSQL_USER"),
                password=os.getenv("MYSQL_PASSWORD"),
                database=os.getenv("MYSQL_DB")
            )
            print("Conexión a MySQL exitosa.")
            return conn
        except Error as e:
            last_exception = e
            print(f"Intento {i+1}/{attempts}: Error al conectar a MySQL: {e}. Reintentando en 5 segundos...")
            time.sleep(5)
    
    print("No se pudo conectar a MySQL después de varios intentos.")
    if last_exception and last_exception.errno == 1049:
        print(f"La base de datos '{os.getenv('MYSQL_DB')}' no existe. Intentando crearla...")
        try:
            conn_server = mysql.connector.connect(host=os.getenv('MYSQL_HOST'), user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'))
            cursor = conn_server.cursor()
            cursor.execute(f"CREATE DATABASE {os.getenv('MYSQL_DB')}")
            conn_server.close()
            print("Base de datos creada. Intenta reiniciar la aplicación para que se conecte y cree las tablas.")
        except Error as create_e:
            print(f"No se pudo crear la base de datos: {create_e}")
    return None

def init_db():
    conn = get_db_connection()
    if conn is None: return
    
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES LIKE 'time_logs'")
    if not cursor.fetchone():
        print("Creando tabla 'time_logs' en MySQL...")
        cursor.execute("""
            CREATE TABLE time_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                task_id INT NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                duration_minutes INT NOT NULL,
                log_date DATE NOT NULL
            )""")
        time_logs_data = [(1, 'user-id-ejemplo', 30, '2025-10-20'), (1, 'user-id-ejemplo', 45, '2025-10-21'), (2, 'user-id-ejemplo', 60, '2025-10-22')]
        cursor.executemany("INSERT INTO time_logs (task_id, user_id, duration_minutes, log_date) VALUES (%s, %s, %s, %s)", time_logs_data)
        conn.commit()
        print("Tabla 'time_logs' creada y poblada.")
    cursor.close()
    conn.close()