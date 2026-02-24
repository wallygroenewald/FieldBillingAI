import sqlite3
from datetime import datetime
from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import requests
import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

# ---------------------------------------------------
# BASIC SETUP
# ---------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
DB_PATH = BASE_DIR / "billing.db"

load_dotenv(dotenv_path=ENV_FILE)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="supersecretkey")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# ---------------------------------------------------
# DATABASE INIT
# ---------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        role TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS engineers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        home_address TEXT,
        lat REAL,
        lng REAL
    )
    """)

    cursor.execute("""
CREATE TABLE IF NOT EXISTS route_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    origin TEXT,
    destination TEXT,
    distance_km REAL,
    duration_minutes REAL,
    added_by TEXT,
    created_at TEXT
)
""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS billing_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        origin TEXT,
        destination TEXT,
        travel_km REAL,
        travel_minutes REAL,
        created_at TEXT
    )
    """)
    cursor.execute("""
CREATE TABLE IF NOT EXISTS upload_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    engineer TEXT,
    reference TEXT,
    site_name TEXT,
    client_name TEXT,

    site_address TEXT,
    site_gps TEXT,

    recorded_km REAL,
    recorded_travel_minutes REAL,
    recorded_onsite_minutes REAL,

    system_km REAL,
    system_minutes REAL,

    visits INTEGER DEFAULT 1,

    final_km REAL,
    final_travel_minutes REAL,
    final_onsite_minutes REAL,

    fss_comments TEXT,

    status TEXT DEFAULT 'OK',
    approved INTEGER DEFAULT 0
)
""")
    # Create default admin if not exists
    cursor.execute("SELECT * FROM users WHERE username='admin'")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO users (username, password, role) VALUES (?, ?, ?)",
            ("admin", "admin123", "admin")
        )
    conn.commit()
    conn.close()

init_db()

# ---------------------------------------------------
# LOGIN SYSTEM
# ---------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/admin/login")
def login(request: Request,
          username: str = Form(...),
          password: str = Form(...)):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT role FROM users WHERE username=? AND password=?",
        (username, password)
    )
    user = cursor.fetchone()
    conn.close()

    if not user:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Invalid username or password"
        })

    request.session["user"] = username
    request.session["role"] = user[0]

    return RedirectResponse("/", status_code=302)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login")

# ---------------------------------------------------
# HOME
# ---------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request):

    if not request.session.get("user"):
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM billing_jobs ORDER BY id DESC")
    jobs = cursor.fetchall()
    conn.close()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "jobs": jobs,
        "user": request.session.get("user"),
        "role": request.session.get("role")
    })

# ---------------------------------------------------
# DISTANCE CALC
# ---------------------------------------------------

@app.post("/calculate-distance")
def calculate_distance(origin: str = Form(...),
                       destination: str = Form(...)):

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"

    params = {
        "origins": origin,
        "destinations": destination,
        "mode": "driving",
        "units": "metric",
        "key": GOOGLE_API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    if not data.get("rows"):
        return {"status": "error"}

    element = data["rows"][0]["elements"][0]

    if element["status"] != "OK":
        return {"status": "error"}

    km = round(element["distance"]["value"] / 1000, 2)
    minutes = round(element["duration"]["value"] / 60, 1)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id FROM route_cache
        WHERE origin=? AND destination=?
    """, (origin, destination))
    existing = cursor.fetchone()
    conn.close()

    return {
        "status": "OK",
        "distance_km": km,
        "duration_minutes": minutes,
        "cached": True if existing else False
    }

# ---------------------------------------------------
# SAVE ROUTE TO CACHE (ADMIN)
# ---------------------------------------------------

@app.post("/admin/save-route")
def save_route(origin: str = Form(...),
               destination: str = Form(...),
               distance_km: float = Form(...),
               duration_minutes: float = Form(...),
               request: Request = None):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO route_cache
    (origin, destination, distance_km, duration_minutes, added_by, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (
        origin,
        destination,
        distance_km,
        duration_minutes,
        request.session.get("user"),
        datetime.now().isoformat()
    ))

    conn.commit()
    conn.close()

    return {"status": "saved"}

# ---------------------------------------------------
# SAVE BILLING JOB
# ---------------------------------------------------

@app.post("/save-job")
def save_job(origin: str = Form(...),
             destination: str = Form(...),
             distance_km: float = Form(...),
             duration_minutes: float = Form(...)):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO billing_jobs
    (origin, destination, travel_km, travel_minutes, created_at)
    VALUES (?, ?, ?, ?, ?)
    """, (
        origin,
        destination,
        distance_km,
        duration_minutes,
        datetime.now().isoformat()
    ))

    conn.commit()
    conn.close()

    return {"status": "saved"}

# ---------------------------------------------------
# ADMIN DASHBOARD
# ---------------------------------------------------

@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    return templates.TemplateResponse("admin_dashboard.html", {
        "request": request,
        "user": request.session.get("user"),
        "role": request.session.get("role")
    })

# ---------------------------------------------------
# MANAGE USERS
# ---------------------------------------------------

@app.get("/admin/users", response_class=HTMLResponse)
def manage_users(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, role FROM users")
    users = cursor.fetchall()
    conn.close()

    return templates.TemplateResponse("users.html", {
        "request": request,
        "users": users
    })


@app.post("/admin/delete-user/{user_id}")
def delete_user(user_id: int, request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()

    return RedirectResponse("/admin/users", status_code=302)


@app.post("/admin/reset-password/{user_id}")
def reset_password(user_id: int,
                   new_password: str = Form(...),
                   request: Request = None):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET password=? WHERE id=?",
                   (new_password, user_id))
    conn.commit()
    conn.close()

    return RedirectResponse("/admin/users", status_code=302)

# ---------------------------------------------------
# MANAGE ENGINEERS
# ---------------------------------------------------

@app.get("/engineers", response_class=HTMLResponse)
def engineers_page(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM engineers")
    engineers = cursor.fetchall()
    conn.close()

    return templates.TemplateResponse("engineers.html", {
        "request": request,
        "engineers": engineers
    })


@app.post("/admin/delete-engineer/{engineer_id}")
def delete_engineer(engineer_id: int, request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM engineers WHERE id=?", (engineer_id,))
    conn.commit()
    conn.close()

    return RedirectResponse("/engineers", status_code=302)

@app.post("/admin/update-route")
def update_route(id: int = Form(...),
                 distance_km: float = Form(...),
                 duration_minutes: float = Form(...),
                 request: Request = None):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE route_cache
        SET distance_km=?, duration_minutes=?
        WHERE id=?
    """, (distance_km, duration_minutes, id))

    conn.commit()
    conn.close()

    return {"status": "updated"}
#------
#add user admin
#-------
@app.post("/admin/add-user")
def add_user(username: str = Form(...),
             password: str = Form(...),
             role: str = Form(...),
             request: Request = None):

    if request.session.get("role") != "admin":
        return {"status": "error"}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO users (username, password, role) VALUES (?, ?, ?)",
            (username, password, role)
        )
        conn.commit()
        conn.close()
        return {"status": "success"}

    except sqlite3.IntegrityError:
        conn.close()
        return {"status": "exists"}
#----
#ADD ENGINEER ROUTE
#------
@app.post("/add-engineer")
def add_engineer(name: str = Form(...),
                 home_address: str = Form(...),
                 request: Request = None):

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": home_address,
        "key": GOOGLE_API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    if not data.get("results"):
        return {"status": "error"}

    location = data["results"][0]["geometry"]["location"]
    lat = location["lat"]
    lng = location["lng"]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO engineers (name, home_address, lat, lng)
        VALUES (?, ?, ?, ?)
    """, (name, home_address, lat, lng))

    conn.commit()
    conn.close()

    return {"status": "success"}
#------
#route cache
#------
@app.get("/route-cache", response_class=HTMLResponse)
def route_cache_page(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, origin, destination, distance_km,
               duration_minutes, added_by, created_at
        FROM route_cache
        ORDER BY id DESC
    """)

    routes = cursor.fetchall()
    conn.close()

    return templates.TemplateResponse("route_cache.html", {
        "request": request,
        "routes": routes
    })
#---------
#UPLOAD PAGE
#---------
@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    return templates.TemplateResponse("upload.html", {
        "request": request
    })

#----
#handle upload
#----
@app.post("/upload")
async def handle_upload(request: Request, file: UploadFile = File(...)):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    df = pd.read_excel(file.file)

    print("====== UPLOAD STARTED ======")
    print("Columns detected:", df.columns)
    print("Total rows in file:", len(df))

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM upload_rows")

    for index, row in df.iterrows():

        print("\n----- NEW ROW -----")
        print(row)

        try:
            engineer_raw = str(row.get("Engineer")).strip()
            engineer = engineer_raw.split("-")[0].strip()

            reference = str(row.get("Reference")).strip()
            site_address = str(row.get("Address")).strip()
            site_gps = str(row.get("GPS")).strip()

            print("Engineer:", engineer)
            print("Reference:", reference)
            print("Address:", site_address)
            print("GPS:", site_gps)
            
            reference = str(row.get("Reference")).strip()
            site_name = str(row.get("Site Name")).strip()
            client_name = str(row.get("Client Name")).strip()
            site_address = str(row.get("Address")).strip()
            site_gps = str(row.get("GPS")).strip()

            recorded_km = float(row.get("Recorded KM") or 0)
            recorded_travel = float(row.get("Recorded Travel Time") or 0)
            recorded_onsite = float(row.get("Recorded Onsite Time") or 0)

            if not site_gps or site_gps.lower() == "nan":
                print("Skipping row - No GPS")
                continue

            # Try match engineer
            cursor.execute("""
                SELECT lat, lng FROM engineers
                WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))
            """, (engineer,))
            eng = cursor.fetchone()

            # If engineer not found → insert error row
            if not eng:
                print(f"Engineer not found: {engineer}")

                cursor.execute("""
INSERT INTO upload_rows
(engineer, reference, site_name, client_name,
 site_address, site_gps,
 recorded_km, recorded_travel_minutes, recorded_onsite_minutes,
 system_km, system_minutes,
 final_km, final_travel_minutes, final_onsite_minutes,
 fss_comments, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (
    engineer,
    reference,
    site_name,
    client_name,
    site_address,
    site_gps,
    recorded_km,
    recorded_travel,
    recorded_onsite,
    0,
    0,
    recorded_km,
    recorded_travel,
    recorded_onsite,
    "",
    "ENGINEER NOT FOUND"
))

                continue

            # Engineer found
            origin = f"{eng[0]},{eng[1]}"
            destination = site_gps

            # Check route cache
            cursor.execute("""
                SELECT distance_km, duration_minutes
                FROM route_cache
                WHERE origin=? AND destination=?
            """, (origin, destination))

            cached = cursor.fetchone()

            if cached:
            
    print("Using cached route")
    km, minutes = cached
else:
    print("Calling Google API")

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin,
        "destinations": destination,
        "mode": "driving",
        "units": "metric",
        "key": GOOGLE_API_KEY
    }

    r = requests.get(url, params=params)
    data = r.json()

    if not data.get("rows"):
        print("Google API returned no rows")
        continue

    element = data["rows"][0]["elements"][0]

    if element["status"] != "OK":
        print("Google API error:", element["status"])
        continue

    km = round(element["distance"]["value"] / 1000, 2)
    minutes = round(element["duration"]["value"] / 60, 1)

    cursor.execute("""
        INSERT INTO route_cache
        (origin, destination, distance_km,
         duration_minutes, added_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        origin,
        destination,
        km,
        minutes,
        request.session.get("user"),
        datetime.now().isoformat()
    ))

# ✅ INSERT ROW ALWAYS HAPPENS HERE
cursor.execute("""
INSERT INTO upload_rows
(engineer, reference, site_name, client_name,
 site_address, site_gps,
 recorded_km, recorded_travel_minutes, recorded_onsite_minutes,
 system_km, system_minutes,
 final_km, final_travel_minutes, final_onsite_minutes,
 fss_comments, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (
    engineer,
    reference,
    site_name,
    client_name,
    site_address,
    site_gps,
    recorded_km,
    recorded_travel,
    recorded_onsite,
    km,
    minutes,
    km,
    minutes,
    recorded_onsite,
    "",
    "OK"
))

            print("Row inserted successfully")

        except Exception as e:
            print("ERROR PROCESSING ROW:", e)
            continue

    conn.commit()
    conn.close()

    print("====== UPLOAD COMPLETE ======")

    return RedirectResponse("/review-upload", status_code=302)
#----
#review page
#----
@app.get("/review-upload", response_class=HTMLResponse)
def review_upload(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM upload_rows")
    rows = cursor.fetchall()
    conn.close()

    return templates.TemplateResponse("review_upload.html", {
        "request": request,
        "rows": rows
    })
#-------
#Recalculate
#-----
@app.post("/admin/recalculate-upload/{row_id}")
def recalculate_upload(row_id: int, request: Request):

    if request.session.get("role") != "admin":
        return {"status": "error"}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT engineer, site_gps
        FROM upload_rows
        WHERE id=?
    """, (row_id,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        return {"status": "error"}

    engineer, site_gps = row

    cursor.execute("""
        SELECT lat, lng FROM engineers
        WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))
    """, (engineer,))
    eng = cursor.fetchone()

    if not eng:
        conn.close()
        return {"status": "error"}

    origin = f"{eng[0]},{eng[1]}"
    destination = site_gps

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin,
        "destinations": destination,
        "mode": "driving",
        "units": "metric",
        "key": GOOGLE_API_KEY
    }

    r = requests.get(url, params=params)
    data = r.json()

    element = data["rows"][0]["elements"][0]

    km = round(element["distance"]["value"] / 1000, 2)
    minutes = round(element["duration"]["value"] / 60, 1)

    cursor.execute("""
        UPDATE upload_rows
        SET system_km=?, system_minutes=?, status='OK'
        WHERE id=?
    """, (km, minutes, row_id))

    conn.commit()
    conn.close()

    return {"status": "updated"}
