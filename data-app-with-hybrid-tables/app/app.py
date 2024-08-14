from flask import Flask, render_template, g, request, redirect, url_for, session
from flask_caching import Cache
from functools import wraps
from typing import List, Dict
import snowflake_connection
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
import datetime

from model import Label, Task, User
from viewmodel import HomeViewModel, ItemViewModel
from Controller import Controller

load_dotenv(override=True)

def get_connection()->Session:
    print(f'Recreating connection')
    conn = snowflake_connection.getSession()
    cwd = os.getcwd()
    conn.query_tag = f'{cwd}/app.py'
    print(f'Connected to {conn.get_current_account()} as ROLE {conn.get_current_role()}')
    return conn

def get_controller()->Controller:
    print(f'Recreating controller')
    ctrl = Controller(connection)
    return ctrl

def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv('APP_SECRET')
    return app

app = create_app()

with app.app_context():
    cache = Cache(config={'CACHE_TYPE': 'SimpleCache'})
    cache.init_app(app)

connection:Session = get_connection()
controller:Controller = get_controller()

@cache.cached(timeout=500, key_prefix='users_list')
def get_users() -> List[User]:

    users = controller.load_users()
    return users

@cache.cached(timeout=50, key_prefix='label_stats')
def get_label_stats(user_id) -> Dict[str, Label]:

    label_stats = controller.load_label_stats(user_id)
    return label_stats

@cache.memoize(50)
def get_items_stats(user_id, label):
    task_stats = controller.load_items_stats(user_id, label)
    return task_stats

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:        
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)        
    return decorated_function

@app.route("/label/<label>", strict_slashes=False)
@app.route("/", defaults={'label': None}, strict_slashes=False)
@login_required
def home(label:str):
    user_id = session['user_id']

    tasks_overview = controller.load_items_overview(user_id, label)
    tasks_overview_by_status = controller.get_tasks_by_status(tasks_overview)
    tasks_ids = Controller.get_task_ids_for_overview(tasks_overview_by_status)
    labels = controller.load_task_labels(tasks_ids)

    task_stats = get_items_stats(user_id, label)# controller.load_items_stats(user_id, label)
    label_stats = get_label_stats(user_id)
    users = get_users()
    for user in users:
        user.active = user.id == user_id

    viewmodel = HomeViewModel(
        users,
        user_id,
        task_stats["overdue"].count, 
        task_stats["today"].count, 
        task_stats["upcoming"].count, 
        [Controller.map_labels(_, labels, label_stats) for _ in tasks_overview_by_status[0]],
        [Controller.map_labels(_, labels, label_stats) for _ in tasks_overview_by_status[1]],
        [Controller.map_labels(_, labels, label_stats) for _ in tasks_overview_by_status[2]],
        label_stats,
        label)
    
    return render_template("main.html", model=viewmodel)

@login_required
@app.route("/new", defaults={'id': None})
@app.route("/item/<id>")
def item(id:int):
    user_id = session['user_id']
    
    label_stats = get_label_stats(user_id)
    users = get_users()
    
    if id:
        task = controller.load_task(user_id, id, label_stats)
    else:
        task = Task(-1, 'New task', datetime.datetime.today(), 0, False, [], 'today')

    viewmodel = ItemViewModel(users, user_id, label_stats, task)

    return render_template("item.html", model=viewmodel, data=task, labels=label_stats)

@login_required
@app.route("/add", defaults={'id': None}, methods=["POST"])
@app.route("/update/<id>", methods=["POST"])
def update(id:int):
    action = request.form.get("task_action")
    view_url = request.form.get("view_url") if "view_url" in request.form else url_for("home")

    user_id = session['user_id']
    
    if action == 'create' or action == 'update':
        item = request.form.get("task_item")
        date_due_str = request.form.get("task_date_due")
        if date_due_str and date_due_str != '':
            date_due = datetime.datetime.strptime(date_due_str, '%Y-%m-%d')
        else:
            date_due = datetime.datetime.today()
        updated_labels = request.form.get("task_labels")
        if updated_labels:
            updated_labels = [_.strip().lower() for _ in updated_labels.split(',')]
        else:
            updated_labels = []

    if action == 'create':
        controller.create_task(user_id, item, date_due, updated_labels)
        cache.delete_memoized(get_label_stats)
        cache.delete_memoized(get_items_stats)
        return redirect(view_url)        

    if action == 'update':
        existing_labels = request.form.get("task_labels_existing")
        if existing_labels:
            existing_labels = [_.strip().lower() for _ in existing_labels.split(',')]
        else:
            existing_labels = []

        controller.update_task(id, item, date_due, updated_labels, existing_labels)
        cache.delete_memoized(get_label_stats)
        cache.delete_memoized(get_items_stats)
        return redirect(view_url)

    if action == 'delete':
        controller.delete_task(id)
        cache.delete_memoized(get_label_stats)
        cache.delete_memoized(get_items_stats)
        return redirect(view_url)

    if action == 'complete':
        controller.complete_task(id)
        cache.delete_memoized(get_label_stats)
        cache.delete_memoized(get_items_stats)
        return redirect(view_url)

@app.route("/login", methods=["POST", "GET"])
def login():
    username = ''
    if request.method == "POST":
        username = request.form.get('username')
        password = request.form.get('password')
        
        if username and password:
            user = controller.loginUser(username, password)
            if user:                
                session['user_id'] = user.id
                return redirect('/')

    if 'user_id' in session:
        session.pop('user_id')
    if 'username' in request.args:
        username = request.args.get('username')
    return render_template("login.html", username=username)

@app.route("/logout", methods=["GET"])
def logout():
    session.pop('user_id')
    return redirect(url_for('login'))

if __name__ == "__main__":
    app.run(debug=True)
