from flask import Flask, render_template, request, redirect, session, url_for, flash, jsonify, Response
from supabase import create_client, Client, PostgrestAPIError, AuthApiError
from dotenv import load_dotenv
import os
from functools import wraps
import pandas as pd
import io
from datetime import date

import db_sqlite
import db_mysql

load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

db_sqlite.init_db()
db_mysql.init_db()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY")

def get_supabase_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    access_token = session.get("access_token")
    client = create_client(url, key)
    if access_token:
        client.auth.set_session(access_token, session.get("refresh_token"))
    return client

@app.route("/")
def home():
    if "user_id" not in session:
        return redirect("/login")
    
    user_id = session["user_id"]
    client = get_supabase_client()
    res = client.table("tasks").select("*").eq("user_id", user_id).execute()
    tasks = res.data

    conn_sqlite = db_sqlite.get_db_connection()
    categories = conn_sqlite.execute("SELECT * FROM categories").fetchall()
    conn_sqlite.close()
    category_map = {cat['id']: cat['name'] for cat in categories}
    for task in tasks:
        task['category_name'] = category_map.get(task['category_id'])

    return render_template("tasks.html", tasks=tasks, categories=categories)

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]
        try:
            result = supabase.auth.sign_in_with_password({"email": email, "password": password})
            if result.user:
                session["user_id"] = result.user.id
                session["user_email"] = result.user.email
                session["access_token"] = result.session.access_token
                session["refresh_token"] = result.session.refresh_token
                print(f"DEBUG: User {email} logged in successfully. User ID: {result.user.id}") 
                return redirect("/")
            return render_template("login.html", error="Credenciales incorrectas.")
        except AuthApiError as e:
            return render_template("login.html", error=e.message)
        except Exception as e:
            return render_template("login.html", error=f"Error inesperado: {e}")
    return render_template("login.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]

        try:
            result = supabase.auth.sign_up({"email": email, "password": password})
            if result.user:
                flash("¡Registro exitoso! Ahora puedes iniciar sesión.", "success")
                return redirect(url_for("login"))
            else:
                flash("No se pudo completar el registro. Inténtalo de nuevo.", "error")
        except (AuthApiError, Exception) as e:
            return render_template("register.html", error=str(e))

    return render_template("register.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

@app.route("/add", methods=["POST"])
def add():
    if "user_id" not in session:
        return redirect("/login")
    title = request.form["title"]
    category_id = request.form.get("category_id")
    user_id = session["user_id"]
    
    task_data = {"title": title, "done": False, "user_id": user_id}
    if category_id:
        task_data['category_id'] = int(category_id)
    try:
        client = get_supabase_client()
        client.table("tasks").insert(task_data).execute()
        flash("Tarea agregada en la base de datos Supabase", "success")
    except (PostgrestAPIError, Exception) as e:
        print(f"ERROR AL INSERTAR: {getattr(e, 'message', e)}")
        flash(f"Error al agregar la tarea: {getattr(e, 'message', e)}", "error")
    return redirect("/")

@app.route("/done/<int:task_id>")
def done(task_id):
    if "user_id" not in session:
        return redirect("/login")
    user_id = session["user_id"]
    try:
        client = get_supabase_client()
        client.table("tasks").update({"done": True}).eq("id", task_id).eq("user_id", user_id).execute()
        flash("Tarea completada, status actualizado en la base de datos de Supabase", "info")
    except (PostgrestAPIError, Exception) as e:
        flash(f"No se pudo actualizar la tarea: {getattr(e, 'message', e)}", "error")
    return redirect("/")

@app.route("/delete/<int:task_id>")
def delete(task_id):
    if "user_id" not in session:
        return redirect("/login")
    try:
        user_id = session["user_id"]
        client = get_supabase_client()
        client.table("tasks").delete().eq("id", task_id).eq("user_id", user_id).execute()
        flash("Tarea eliminada de Supabase.", "success")
    except (PostgrestAPIError, Exception) as e:
        flash(f"No se pudo eliminar la tarea: {getattr(e, 'message', e)}", "error")
    return redirect("/")

@app.route("/categories")
def categories_page():
    conn = db_sqlite.get_db_connection()
    categories = conn.execute("SELECT * FROM categories").fetchall()
    conn.close()
    return render_template("categories.html", categories=categories)

@app.route("/categories/add", methods=["POST"])
def add_category():
    name = request.form["name"]
    try:
        conn = db_sqlite.get_db_connection()
        conn.execute("INSERT INTO categories (name) VALUES (?)", (name,))
        conn.commit()
        conn.close()
        flash(f"Categoría {name} guardada en la base de datos SQLite", "success")
    except Exception as e:
        flash(f"Error al guardar en SQLite: {e}", "error")
    return redirect("/categories")

@app.route("/categories/delete/<int:category_id>")
def delete_category(category_id):
    try:
        conn = db_sqlite.get_db_connection()
        conn.execute("DELETE FROM categories WHERE id = ?", (category_id,))
        conn.commit()
        conn.close()
        flash("Categoría eliminada de la base de datos SQLite", "info")
    except Exception as e:
        flash(f"Error al eliminar de SQLite: {e}", "error")
    return redirect("/categories")

@app.route("/edit/<int:task_id>", methods=["GET", "POST"])
def edit_task(task_id):
    if "user_id" not in session:
        return redirect("/login")

    if request.method == "POST":
        title = request.form["title"]
        category_id = request.form.get("category_id")
        
        update_data = {"title": title}
        if category_id:
            update_data['category_id'] = int(category_id)
        else:
            update_data['category_id'] = None

        try:
            client = get_supabase_client()
            client.table("tasks").update(update_data).eq("id", task_id).eq("user_id", session["user_id"]).execute()
            flash(f"Tarea #{task_id} actualizada en la base de datos Supabase", "success")
        except Exception as e:
            flash(f"Error al actualizar la tarea: {e}", "error")
        return redirect("/")

    client = get_supabase_client()
    res = client.table("tasks").select("*").eq("id", task_id).eq("user_id", session["user_id"]).single().execute()
    task = res.data

    conn_sqlite = db_sqlite.get_db_connection()
    categories = conn_sqlite.execute("SELECT * FROM categories").fetchall()
    conn_sqlite.close()

    return render_template("edit_task.html", task=task, categories=categories)

@app.route("/dashboard")
def dashboard():
    if "user_id" not in session:
        return redirect("/login")

    summary = []
    try:
        conn = db_mysql.get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        query_simple = """
            SELECT task_id, SUM(duration_minutes) as total_minutes
            FROM time_logs
            WHERE user_id = %s
            GROUP BY task_id
            ORDER BY total_minutes DESC
        """
        cursor.execute(query_simple, (session["user_id"],))
        summary_raw = cursor.fetchall()
        task_ids = [item['task_id'] for item in summary_raw]
        if task_ids:
            client = get_supabase_client()
            res = client.table("tasks").select("id, title").in_("id", task_ids).execute()
            task_titles = {task['id']: task['title'] for task in res.data}
            
            for item in summary_raw:
                item['task_title'] = task_titles.get(item['task_id'], 'Tarea no encontrada')
            summary = summary_raw

        cursor.close()
        conn.close()
    except Exception as e:
        flash(f"No se pudo cargar el dashboard desde MySQL: {e}", "error")

    return render_template("dashboard.html", summary=summary)

@app.route("/export")
def export_page():
    return render_template("export.html")

def check_auth(username, password):
    return username == os.getenv("API_USER") and password == os.getenv("API_PASSWORD")

def authenticate():
    return Response(
    'Autenticación requerida.', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

def get_combined_data(user_id_filter=None):
    client = get_supabase_client()
    try:
        query = supabase.table("tasks").select("id, title, done, user_id, category_id")
        if user_id_filter:
            query = query.eq("user_id", user_id_filter)
        
        supabase_res = query.execute()
        tasks_df = pd.DataFrame(supabase_res.data)
        tasks_df.rename(columns={'id': 'task_id'}, inplace=True)
    except Exception as e:
        print(f"Error al obtener datos de Supabase: {e}")
        tasks_df = pd.DataFrame()

    try:
        conn_sqlite = db_sqlite.get_db_connection()
        categories_df = pd.read_sql_query("SELECT * FROM categories", conn_sqlite)
        categories_df.rename(columns={'id': 'category_id', 'name': 'category_name'}, inplace=True)
        conn_sqlite.close()
    except Exception as e:
        print(f"Error al obtener datos de SQLite: {e}")
        categories_df = pd.DataFrame()

    try:
        conn_mysql = db_mysql.get_db_connection()
        sql_query = "SELECT * FROM time_logs"
        params = None
        if user_id_filter:
            sql_query += " WHERE user_id = %s"
            params = (user_id_filter,)
        time_logs_df = pd.read_sql_query(sql_query, conn_mysql, params=params)
        conn_mysql.close()
    except Exception as e:
        print(f"Error al obtener datos de MySQL: {e}")
        time_logs_df = pd.DataFrame()

    if not tasks_df.empty and not categories_df.empty:
        # Asegura que las claves de unión sean compatibles (el tipo 'Int64' de pandas maneja nulos).
        tasks_df['category_id'] = tasks_df['category_id'].astype('Int64')
        categories_df['category_id'] = categories_df['category_id'].astype('Int64')
        tasks_with_categories_df = pd.merge(tasks_df, categories_df, on='category_id', how='left')
    else:
        tasks_with_categories_df = tasks_df

    if not time_logs_df.empty:
        time_summary = time_logs_df.groupby('task_id')['duration_minutes'].sum().reset_index()
        time_summary.rename(columns={'duration_minutes': 'total_minutes_logged'}, inplace=True)
        
        final_df = pd.merge(tasks_with_categories_df, time_summary, on='task_id', how='left')
        final_df['total_minutes_logged'] = final_df['total_minutes_logged'].fillna(0).astype(int)
        final_df['category_name'] = final_df['category_name'].fillna('Sin Categoría')
    else:
        final_df = tasks_with_categories_df
        final_df['total_minutes_logged'] = 0

    return final_df, time_logs_df

@app.route("/api/data")
@requires_auth
def api_data():
    tasks_df, time_logs_df = get_combined_data()
    
    result = {
        "tasks_report": tasks_df.to_dict(orient='records'),
        "time_logs_details": time_logs_df.to_dict(orient='records')
    }
    return jsonify(result)

@app.route("/export/csv")
def export_csv():
    report_type = request.args.get('report', 'master')
    user_id = session.get("user_id")

    df_to_export = pd.DataFrame()
    filename = "reporte.csv"

    if report_type == 'master':
        df_to_export, _ = get_combined_data()
        filename = "reporte_maestro.csv"
    
    elif report_type == 'personal':
        if not user_id: return redirect("/login")
        df_to_export, _ = get_combined_data(user_id_filter=user_id)
        filename = f"reporte_personal_{user_id[:8]}.csv"

    elif report_type == 'tasks':
        client = get_supabase_client()
        res = client.table("tasks").select("*").execute()
        df_to_export = pd.DataFrame(res.data)
        filename = "tabla_tareas.csv"

    elif report_type == 'categories':
        conn = db_sqlite.get_db_connection()
        df_to_export = pd.read_sql_query("SELECT * FROM categories", conn)
        conn.close()
        filename = "tabla_categorias.csv"

    elif report_type == 'time_logs':
        conn = db_mysql.get_db_connection()
        df_to_export = pd.read_sql_query("SELECT * FROM time_logs", conn)
        conn.close()
        filename = "tabla_registros_tiempo.csv"

    if df_to_export.empty:
        flash("No hay datos para exportar para el reporte seleccionado.", "info")
        return redirect(url_for('export_page'))

    output = io.StringIO()
    df_to_export.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return Response(
        output,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )

@app.route("/history/<int:task_id>")
def history_page(task_id):
    conn = db_mysql.get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM time_logs WHERE task_id = %s", (task_id,))
    logs = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template("history.html", logs=logs, task_id=task_id)

@app.route("/log_time/<int:task_id>", methods=["POST"])
def log_time(task_id):
    minutes = request.form["minutes"]
    if "user_id" not in session:
        return redirect("/login")
    
    try:
        minutes_val = int(minutes)
        user_id = session["user_id"]
        current_date = date.today()

        conn = db_mysql.get_db_connection()
        cursor = conn.cursor()
        sql = "INSERT INTO time_logs (task_id, user_id, duration_minutes, log_date) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (task_id, user_id, minutes_val, current_date))
        conn.commit()
        cursor.close()
        conn.close()

        flash(f"Se registraron {minutes} minuto(s) en la base de datos MySQL", "success")
    except Exception as e:
        flash(f"Error al registrar el tiempo en MySQL: {e}", "error")
        
    return redirect("/")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)