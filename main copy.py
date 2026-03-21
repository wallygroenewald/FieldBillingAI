import sqlite3
import re
from datetime import datetime
from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import requests
import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

# --------------------------------
# BASIC SETUP
# --------------------------------

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "billing.db"
ENV_FILE = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_FILE)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="supersecretkey")

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# --------------------------------
# DATABASE INIT
# --------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        role TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS engineers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        home_address TEXT,
        lat REAL,
        lng REAL
    )
    """)

    c.execute("""
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

    c.execute("""
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
        status TEXT,
        approved INTEGER DEFAULT 0
    )
    """)

    c.execute("SELECT * FROM users WHERE username='admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)",
                  ("admin", "admin123", "admin"))

    conn.commit()
    conn.close()

init_db()

# --------------------------------
# SAFE PARSERS
# --------------------------------

def parse_distance(value):
    if not value:
        return 0.0
    value = str(value).lower()
    numbers = re.findall(r"\d+\.?\d*", value)
    if not numbers:
        return 0.0
    number = float(numbers[0])
    if "m" in value and "km" not in value:
        return round(number / 1000, 2)
    return number

def parse_minutes(value):
    if not value:
        return 0.0
    value = str(value).lower()

    if ":" in value:
        days = hours = minutes = seconds = 0
        parts = value.split(":")
        for p in parts:
            if "d" in p: days = int(p.replace("d", ""))
            if "h" in p: hours = int(p.replace("h", ""))
            if "m" in p: minutes = int(p.replace("m", ""))
            if "s" in p: seconds = int(p.replace("s", ""))
        return days*1440 + hours*60 + minutes + seconds/60

    numbers = re.findall(r"\d+\.?\d*", value)
    if not numbers:
        return 0.0
    return float(numbers[0])

# --------------------------------
# LOGIN
# --------------------------------

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/admin/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT role FROM users WHERE username=? AND password=?", (username, password))
    user = c.fetchone()
    conn.close()

    if not user:
        return templates.TemplateResponse("login.html",
            {"request": request, "error": "Invalid credentials"})

    request.session["user"] = username
    request.session["role"] = user[0]
    return RedirectResponse("/", status_code=302)

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login")

# --------------------------------
# HOME
# --------------------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login")
    return templates.TemplateResponse("index.html",
        {"request": request,
         "user": request.session.get("user"),
         "role": request.session.get("role")})

# --------------------------------
# UPLOAD
# --------------------------------

@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request):
    if request.session.get("role") != "admin":
        return RedirectResponse("/login")
    return templates.TemplateResponse("upload.html", {"request": request})

@app.post("/upload")
async def handle_upload(request: Request, file: UploadFile = File(...)):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    df = pd.read_excel(file.file)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM upload_rows")

    for _, row in df.iterrows():

        engineer = str(row.get("Engineer", "")).strip()
        reference = str(row.get("Reference", "")).strip()
        site_name = str(row.get("site") or row.get("Site Name") or "").strip()
        client_name = str(row.get("client") or row.get("Client Name") or "").strip()
        site_address = str(row.get("Address", "")).strip()
        site_gps = str(row.get("GPS", "")).strip()

        recorded_km = parse_distance(row.get("Recorded KM"))

        recorded_travel = parse_minutes(
        row.get("Recorded Travel") or row.get("Recorded Travel Time")
        )

        recorded_onsite = parse_minutes(
        row.get("Recorded Onsite") or row.get("Recorded Onsite Time")
        )

        if not site_gps:
            continue

        # ---------------------------------------------------
        # MATCH ENGINEER
        # ---------------------------------------------------

        c.execute("SELECT lat, lng FROM engineers WHERE LOWER(name)=LOWER(?)", (engineer,))
        eng = c.fetchone()

        if not eng:
            c.execute("""
            INSERT INTO upload_rows
            (engineer, reference, site_name, client_name, site_address, site_gps,
             recorded_km, recorded_travel_minutes, recorded_onsite_minutes,
             system_km, system_minutes,
             visits,
             final_km, final_travel_minutes, final_onsite_minutes,
             fss_comments, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 'ENGINEER NOT FOUND')
            """,
            (engineer, reference, site_name, client_name, site_address, site_gps,
             recorded_km, recorded_travel, recorded_onsite,
             0, 0,
             1,
             recorded_km, recorded_travel, recorded_onsite))
            continue

        origin = f"{eng[0]},{eng[1]}"
        destination = site_gps

        # ---------------------------------------------------
        # CHECK CACHE
        # ---------------------------------------------------

        c.execute(
            "SELECT distance_km, duration_minutes FROM route_cache WHERE origin=? AND destination=?",
            (origin, destination)
        )
        cached = c.fetchone()

        if cached:
            km, minutes = cached
        else:
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
                continue

            element = data["rows"][0]["elements"][0]

            if element.get("status") != "OK":
                continue

            km = round(element["distance"]["value"] / 1000, 2)
            minutes = round(element["duration"]["value"] / 60, 1)

            c.execute("""
            INSERT INTO route_cache
            (origin, destination, distance_km, duration_minutes, added_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (origin, destination, km, minutes,
             request.session.get("user"), datetime.now().isoformat()))

        # ---------------------------------------------------
        # INSERT VALID ROW
        # ---------------------------------------------------

        c.execute("""
        INSERT INTO upload_rows
        (engineer, reference, site_name, client_name, site_address, site_gps,
         recorded_km, recorded_travel_minutes, recorded_onsite_minutes,
         system_km, system_minutes,
         visits,
         final_km, final_travel_minutes, final_onsite_minutes,
         fss_comments, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 'OK')
        """,
        (engineer, reference, site_name, client_name, site_address, site_gps,
         recorded_km, recorded_travel, recorded_onsite,
         km, minutes,
         1,
         km, minutes, recorded_onsite))

    conn.commit()
    conn.close()

    return RedirectResponse("/review-upload", status_code=302)

# --------------------------------
# REVIEW
# --------------------------------

@app.get("/review-upload", response_class=HTMLResponse)
def review(request: Request):
    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT * FROM upload_rows")
    rows = c.fetchall()
    conn.close()

    return templates.TemplateResponse("review_upload.html",
        {"request": request, "rows": rows})

@app.post("/admin/update-upload-field")
def update_field(id: int = Form(...), field: str = Form(...), value: str = Form(...)):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(f"UPDATE upload_rows SET {field}=? WHERE id=?", (value, id))
    conn.commit()
    conn.close()
    return {"status": "updated"}

@app.post("/admin/approve-upload/{row_id}")
def approve_row(row_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE upload_rows SET approved=1 WHERE id=?", (row_id,))
    conn.commit()
    conn.close()
    return {"status": "approved"}

@app.post("/admin/approve-all-upload")
def approve_all():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE upload_rows SET approved=1 WHERE status='OK'")
    conn.commit()
    conn.close()
    return {"status": "all approved"}

@app.get("/admin/export-excel")
def export_excel():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM upload_rows WHERE approved=1", conn)
    conn.close()

    file_path = BASE_DIR / "billing_export.xlsx"
    df.to_excel(file_path, index=False)

    return FileResponse(file_path, filename="billing_export.xlsx")

@app.post("/admin/recalculate-upload/{row_id}")
def recalculate_upload(row_id: int, request: Request):

    if request.session.get("role") != "admin":
        return {"status": "error"}

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        SELECT engineer, site_gps
        FROM upload_rows
        WHERE id=?
    """, (row_id,))
    row = c.fetchone()

    if not row:
        conn.close()
        return {"status": "error"}

    engineer, site_gps = row

    c.execute("SELECT lat, lng FROM engineers WHERE LOWER(name)=LOWER(?)", (engineer,))
    eng = c.fetchone()

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

    if not data.get("rows"):
        conn.close()
        return {"status": "error"}

    element = data["rows"][0]["elements"][0]

    if element.get("status") != "OK":
        conn.close()
        return {"status": "error"}

    km = round(element["distance"]["value"] / 1000, 2)
    minutes = round(element["duration"]["value"] / 60, 1)

    c.execute("""
        UPDATE upload_rows
        SET system_km=?, system_minutes=?,
            final_km=?, final_travel_minutes=?,
            status='OK'
        WHERE id=?
    """, (km, minutes, km, minutes, row_id))

    conn.commit()
    conn.close()

    return {"status": "updated"}
# --------------------------------
# ADMIN DASHBOARD
# --------------------------------

@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    return templates.TemplateResponse("admin_dashboard.html", {
        "request": request,
        "user": request.session.get("user"),
        "role": request.session.get("role")
    })
# --------------------------------
# MANAGE USERS
# --------------------------------

@app.get("/admin/users", response_class=HTMLResponse)
def manage_users(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, username, role FROM users")
    users = c.fetchall()
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
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()

    return RedirectResponse("/admin/users", status_code=302)
# --------------------------------
# ENGINEERS
# --------------------------------

@app.get("/engineers", response_class=HTMLResponse)
def engineers_page(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT * FROM engineers")
    engineers = c.fetchall()
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
    c = conn.cursor()
    c.execute("DELETE FROM engineers WHERE id=?", (engineer_id,))
    conn.commit()
    conn.close()

    return RedirectResponse("/engineers", status_code=302)


# ADD ENGINEER ROUTE (MUST BE OUTSIDE)

@app.post("/add-engineer")
def add_engineer(name: str = Form(...), home_address: str = Form(...)):

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": home_address,
        "key": GOOGLE_API_KEY
    }

    r = requests.get(url, params=params)
    data = r.json()

    if not data.get("results"):
        return {"status": "error"}

    loc = data["results"][0]["geometry"]["location"]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        c.execute("""
            INSERT INTO engineers (name, home_address, lat, lng)
            VALUES (?, ?, ?, ?)
        """, (name, home_address, loc["lat"], loc["lng"]))

        conn.commit()
    except:
        conn.close()
        return {"status": "exists"}

    conn.close()

    return {"status": "success"}
# --------------------------------
# ROUTE CACHE
# --------------------------------

@app.get("/route-cache", response_class=HTMLResponse)
def route_cache_page(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT id, origin, destination, distance_km,
               duration_minutes, added_by, created_at
        FROM route_cache
        ORDER BY id DESC
    """)
    routes = c.fetchall()
    conn.close()

    return templates.TemplateResponse("route_cache.html", {
        "request": request,
        "routes": routes
    })
