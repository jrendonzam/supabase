import sqlite3
DB_FILE = "local_database.db"

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='categories'")
    if cursor.fetchone() is None:
        print("Creando tabla 'categories' en SQLite...")
        cursor.execute("""
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )
        """)
        categories_data = [('Trabajo',), ('Personal',), ('Estudio',), ('Urgente',)]
        cursor.executemany("INSERT INTO categories (name) VALUES (?)", categories_data)
        conn.commit()
        print("Tabla 'categories' creada y poblada.")
    conn.close()