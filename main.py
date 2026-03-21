import sqlite3
import re
from datetime import datetime
from fastapi import FastAPI, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from passlib.context import CryptContext
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

load_dotenv()

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="super-secret-change-this")

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# --------------------------------
# PASSWORD HASHING
# --------------------------------

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

def hash_password(password: str):
    return pwd_context.hash(password)

def verify_password(password: str, hashed: str):
    return pwd_context.verify(password, hashed)

# --------------------------------
# DATABASE INIT
# --------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        company_id INTEGER NOT NULL,
        external_rate REAL DEFAULT 0,
        internal_rate REAL DEFAULT 0,
        UNIQUE(name, company_id)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password_hash TEXT,
        role TEXT,
        company_id INTEGER,
        client_id INTEGER
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS engineers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        home_address TEXT,
        lat REAL,
        lng REAL,
        verified INTEGER DEFAULT 0
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS route_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        origin TEXT,
        destination TEXT,
        origin_label TEXT,
        destination_label TEXT,
        destination_address TEXT,
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
        status TEXT,
        task_done TEXT,
        internal_approved INTEGER DEFAULT 0,
        client_approved INTEGER DEFAULT 0,
        return_trip INTEGER DEFAULT 1
    )
    """)

    # Default company
    c.execute("INSERT OR IGNORE INTO companies (id, name) VALUES (1, 'Nkgwete')")

    # Default admin
    c.execute("SELECT * FROM users WHERE username='admin'")
    if not c.fetchone():
        c.execute("""
            INSERT INTO users (username, password_hash, role, company_id)
            VALUES (?, ?, ?, ?)
        """, ("admin", hash_password("admin123"), "admin", 1))

    conn.commit()
    conn.close()

init_db()
# --------------------------------
# UPLOAD PAGE
# --------------------------------

@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    return templates.TemplateResponse("upload.html", {
        "request": request
    })
# --------------------------------
# SAFE PARSERS
# --------------------------------

import re

def parse_distance(value):
    if not value:
        return 0.0

    value = str(value).lower().replace("km", "").strip()

    try:
        return float(value)
    except:
        return 0.0

    try:
        return float(value)
    except:
        numbers = re.findall(r"\d+\.?\d*", value)
        if numbers:
            return float(numbers[0])
        return 0.0

    number = float(numbers[0])

    # If meters (but not km)
    if "m" in value and "km" not in value:
        return round(number / 1000, 2)

    return number


def parse_minutes(value):
    if not value:
        return 0.0

    value = str(value).strip().lower()

    # Handle 00d:01h:38m:44s format
    if "d" in value and "h" in value and "m" in value:
        try:
            value = value.replace("d", "").replace("h", "").replace("m", "").replace("s", "")
            days, hours, minutes, seconds = value.split(":")

            total_minutes = (
                int(days) * 1440 +
                int(hours) * 60 +
                int(minutes) +
                int(seconds) / 60
            )

            return round(total_minutes, 2)

        except:
            return 0.0

    # Fallback if already numeric
    try:
        return float(value)
    except:
        return 0.0

# --------------------------------
# LOGIN
# --------------------------------

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, password_hash, role, company_id, client_id FROM users WHERE username=?", (username,))
    user = c.fetchone()
    conn.close()

    if not user or not verify_password(password, user[1]):
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})

    request.session.update({
        "user_id": user[0],
        "username": username,
        "role": user[2],
        "company_id": user[3],
        "client_id": user[4]
    })

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

    if not request.session.get("user_id"):
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT * FROM route_cache ORDER BY id DESC")
    jobs = c.fetchall()
    conn.close()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "jobs": jobs
    })

# --------------------------------
# CALCULATE ROUTE (Google only)
# --------------------------------

@app.post("/calculate-route")
async def calculate_route(
    request: Request,
    origin: str = Form(...),
    destination: str = Form(...),
    origin_label: str = Form(""),
    destination_label: str = Form("")
):

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        SELECT distance_km, duration_minutes
        FROM route_cache
        WHERE origin=? AND destination=?
    """, (origin, destination))

    cached = c.fetchone()

    if cached:
        conn.close()
        return {"status": "cached", "distance": cached[0], "duration": cached[1]}

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin,
        "destinations": destination,
        "mode": "driving",
        "units": "metric",
        "key": GOOGLE_API_KEY
    }

    response = requests.get(url, params=params).json()

    element = response["rows"][0]["elements"][0]

    if element["status"] != "OK":
        conn.close()
        return {"status": "error"}

    km = round(element["distance"]["value"] / 1000, 2)
    minutes = round(element["duration"]["value"] / 60, 1)

    c.execute("""
        INSERT INTO route_cache
        (origin, destination, origin_label, destination_label,
         distance_km, duration_minutes, added_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        origin,
        destination,
        origin_label or origin,
        destination_label or destination,
        km,
        minutes,
        request.session.get("username"),
        datetime.now().isoformat()
    ))

    conn.commit()
    conn.close()

    return {"status": "calculated", "distance": km, "duration": minutes}

# --------------------------------
# RE-CALCULATE UPLOAD ROW
# --------------------------------

@app.post("/recalculate-upload/{row_id}")
def recalculate_upload(row_id: int):

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT engineer, site_gps, return_trip FROM upload_rows WHERE id=?", (row_id,))
    row = c.fetchone()

    if not row:
        conn.close()
        return {"status": "error"}

    engineer, site_gps, return_trip = row

    # Check if engineer exists
    c.execute("SELECT id, lat, lng FROM engineers WHERE LOWER(name)=LOWER(?)", (engineer,))
    eng = c.fetchone()

    if not eng:
        print("Creating new engineer:", engineer)

        try:
            lat_str, lng_str = site_gps.split(",")
            lat = float(lat_str)
            lng = float(lng_str)
        except:
            lat = 0
            lng = 0

        c.execute("""
            INSERT INTO engineers (name, home_address, lat, lng)
            VALUES (?, ?, ?, ?)
        """, (engineer, "Auto-created from upload", lat, lng))

        conn.commit()

        c.execute("SELECT id, lat, lng FROM engineers WHERE LOWER(name)=LOWER(?)", (engineer,))
        eng = c.fetchone()

    origin = f"{eng[1]},{eng[2]}"
    destination = site_gps

    c.execute("""
        SELECT distance_km, duration_minutes
        FROM route_cache
        WHERE origin=? AND destination=?
    """, (origin, destination))

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

        response = requests.get(url, params=params).json()
        element = response["rows"][0]["elements"][0]

        km = round(element["distance"]["value"] / 1000, 2)
        minutes = round(element["duration"]["value"] / 60, 1)

    final_km = km * 2 if return_trip == 1 else km
    final_minutes = minutes * 2 if return_trip == 1 else minutes

    c.execute("""
        UPDATE upload_rows
        SET system_km=?, system_minutes=?,
            final_km=?, final_travel_minutes=?,
            status='OK'
        WHERE id=?
    """, (km, minutes, final_km, final_minutes, row_id))

    conn.commit()
    conn.close()

    return {"status": "recalculated"}
# --------------------------------
# ENGINEERS
# --------------------------------

@app.get("/engineers", response_class=HTMLResponse)
def engineers_page(request: Request):

    if not request.session.get("user_id"):
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
         SELECT id, name ,home_address, lat, lng, verified
         FROM engineers
         ORDER BY id DESC
    """)
    engineers = c.fetchall()
    conn.close()

    return templates.TemplateResponse("engineers.html", {
        "request": request,
        "engineers": engineers
    })


@app.post("/add-engineer")
def add_engineer(
    request: Request,
    name: str = Form(...),
    home_address: str = Form(...)
):

    if request.session.get("role") != "admin":
        return {"status": "unauthorized"}

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": home_address,
        "key": GOOGLE_API_KEY
    }

    response = requests.get(url, params=params).json()

    if not response.get("results"):
        return {"status": "geocode_failed"}

    location = response["results"][0]["geometry"]["location"]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        c.execute("""
            INSERT INTO engineers (name, home_address, lat, lng)
            VALUES (?, ?, ?, ?)
        """, (name.strip(), home_address, location["lat"], location["lng"]))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return {"status": "exists"}

    conn.close()
    return {"status": "success"}


@app.post("/admin/delete-engineer/{engineer_id}")
def delete_engineer(engineer_id: int, request: Request):

    if request.session.get("role") != "admin":
        return {"status": "unauthorized"}

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Check if engineer used in uploads
    c.execute("""
        SELECT COUNT(*) FROM upload_rows
        WHERE LOWER(engineer) IN (
            SELECT LOWER(name) FROM engineers WHERE id=?
        )
    """, (engineer_id,))

    count = c.fetchone()[0]

    if count > 0:
        conn.close()
        return {"status": "in_use"}

    c.execute("DELETE FROM engineers WHERE id=?", (engineer_id,))
    conn.commit()
    conn.close()

    return {"status": "deleted"}
# --------------------------------
# UPDATE ENGINEER (AUTO GEOCODE)
# --------------------------------

@app.post("/admin/update-engineer/{engineer_id}")
def update_engineer(
    engineer_id: int,
    home_address: str = Form(...),
    request: Request = None
):

    if request.session.get("role") != "admin":
        return {"status": "unauthorized"}

    # Geocode new address
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": home_address,
        "key": GOOGLE_API_KEY
    }

    response = requests.get(url, params=params).json()

    if not response.get("results"):
        return {"status": "geocode_failed"}

    location = response["results"][0]["geometry"]["location"]
    lat = location["lat"]
    lng = location["lng"]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        UPDATE engineers
        SET home_address=?, lat=?, lng=?, verified=1
        WHERE id=?
    """, (home_address.strip(), lat, lng, engineer_id))

    conn.commit()
    conn.close()

    return {"status": "success"}
# -------------
# UPLOAD PAGE
# -------------
@app.post("/upload")
async def handle_upload(request: Request, file: UploadFile = File(...)):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    df = pd.read_excel(file.file)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    

    for _, row in df.iterrows():

        engineer = str(row.get("User", "")).strip()
        reference = str(row.get("Name", "")).strip()
        site_name = str(row.get("Site", "")).strip()
        client_name = str(row.get("Client", "")).strip()
        site_gps = str(row.get("Task Location", "")).strip()
        site_address = str(row.get("Full Address", "")).strip()

        # ---- Date Handling ----
        task_raw = row.get("Task Done")
        if pd.isna(task_raw):
            task_done = None
        elif isinstance(task_raw, pd.Timestamp):
            task_done = task_raw.strftime("%Y-%m-%d")
        else:
            task_done = str(task_raw)

        # ---- Parse recorded values FIRST ----
        recorded_km = parse_distance(row.get("Actual Travel Distance"))
        recorded_travel = parse_minutes(row.get("Actual Travel Time"))
        recorded_onsite = parse_minutes(row.get("Work Duration to Task Done"))

        # --------------------------------------------
        # DETERMINE BILLING MODE
        # --------------------------------------------

        # Get engineer id
        c.execute("SELECT id FROM engineers WHERE LOWER(name)=LOWER(?)", (engineer,))
        eng_row = c.fetchone()
        if not eng_row:
            continue

        engineer_id = eng_row[0]

        # Get client id
        c.execute("SELECT id FROM clients WHERE LOWER(name)=LOWER(?)", (client_name,))
        client_row = c.fetchone()
        if not client_row:
            continue

        client_id = client_row[0]

        # Check contract rule
        c.execute("""
            SELECT billing_mode
            FROM engineer_contract_rules
            WHERE engineer_id=? AND client_id=?
        """, (engineer_id, client_id))

        rule = c.fetchone()

        # --------------------------------------------
        # 🚫 FIXED CONTRACT – NON BILLABLE
        # --------------------------------------------
        if rule and rule[0] == "fixed":

            c.execute("""
                INSERT INTO upload_rows
                (engineer, reference, site_name, client_name,
                 site_gps, recorded_km, recorded_travel_minutes,
                 recorded_onsite_minutes,
                 system_km, system_minutes,
                 visits, final_km, final_travel_minutes,
                 final_onsite_minutes,
                 task_done, status,
                 billable, line_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 
                        0, 0, 1, 0, 0, ?, ?, 
                        0, 'fixed_engineer')
            """, (
                engineer,
                reference,
                site_name,
                client_name,
                site_gps,
                recorded_km,
                recorded_travel,
                recorded_onsite,
                recorded_onsite,
                task_done,
                "FIXED CONTRACT – NON BILLABLE"
            ))

            continue  # 🔥 SKIP GOOGLE COMPLETELY

        # --------------------------------------------
        # NORMAL TIME & TRAVEL BILLING
        # --------------------------------------------

        if not site_gps or "," not in site_gps:
            continue

        try:
            lat_str, lng_str = site_gps.split(",")
            float(lat_str)
            float(lng_str)
        except:
            continue

        # Get engineer coordinates
        c.execute("SELECT lat, lng FROM engineers WHERE id=?", (engineer_id,))
        eng = c.fetchone()
        if not eng:
            continue

        origin = f"{eng[0]},{eng[1]}"
        destination = site_gps

        # Check cache
        c.execute("""
            SELECT distance_km, duration_minutes
            FROM route_cache
            WHERE origin=? AND destination=?
        """, (origin, destination))

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

            response = requests.get(url, params=params).json()
            element = response["rows"][0]["elements"][0]

            if element["status"] != "OK":
                continue

            km = round(element["distance"]["value"] / 1000, 2)
            minutes = round(element["duration"]["value"] / 60, 1)

            # Save cache
            c.execute("""
                INSERT INTO route_cache
                (origin, destination, origin_label, destination_label,
                 destination_address, distance_km, duration_minutes,
                 added_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                origin,
                destination,
                engineer,
                site_name,
                site_address,
                km,
                minutes,
                request.session.get("username"),
                datetime.now().isoformat()
            ))

        # Insert normal row
        c.execute("""
            INSERT INTO upload_rows
            (engineer, reference, site_name, client_name,
             site_gps, recorded_km, recorded_travel_minutes,
             recorded_onsite_minutes,
             system_km, system_minutes,
             visits, final_km, final_travel_minutes,
             final_onsite_minutes,
             task_done, status,
             billable, line_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 'OK', 1, 'time_travel')
        """, (
            engineer,
            reference,
            site_name,
            client_name,
            site_gps,
            recorded_km,
            recorded_travel,
            recorded_onsite,
            km,
            minutes,
            km,
            minutes,
            recorded_onsite,
            task_done
        ))

    conn.commit()
    conn.close()

    return RedirectResponse("/review-upload", status_code=302)
# --------------------------------
# REVIEW UPLOAD PAGE
# --------------------------------
@app.get("/review-upload", response_class=HTMLResponse)
def review_upload(request: Request,
                  start_date: str = None,
                  end_date: str = None):

    if not request.session.get("user_id"):
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    query = """
    SELECT u.*, e.verified, e.home_address, e.lat, e.lng
    FROM upload_rows u
    LEFT JOIN engineers e
        ON LOWER(u.engineer) = LOWER(e.name)
    """

    params = []

    if start_date and end_date:
        query += " WHERE date(u.task_done) BETWEEN date(?) AND date(?)"
        params.extend([start_date, end_date])

    query += " ORDER BY u.task_done DESC"

    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    return templates.TemplateResponse("review_upload.html", {
        "request": request,
        "rows": rows,
        "start_date": start_date,
        "end_date": end_date
    })
# --------------------------------
# ROUTE CACHE PAGE
# --------------------------------
@app.get("/route-cache", response_class=HTMLResponse)
def route_cache_page(request: Request):

    if not request.session.get("user_id"):
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        SELECT 
            r.id,
            r.origin,
            r.destination,
            r.origin_label,
            r.destination_label,
            r.distance_km,
            r.duration_minutes,
            r.added_by,
            r.created_at,
            r.destination_address,
            e.home_address
        FROM route_cache r
        LEFT JOIN engineers e
            ON LOWER(r.origin_label) = LOWER(e.name)
        ORDER BY r.id DESC
    """)

    routes = c.fetchall()
    conn.close()

    return templates.TemplateResponse("route_cache.html", {
        "request": request,
        "routes": routes
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

    # All users
    c.execute("""
        SELECT id, username, role, company_id, client_id
        FROM users
        ORDER BY id DESC
    """)
    users = c.fetchall()

    # All clients
    c.execute("SELECT id, name FROM clients ORDER BY name")
    clients = c.fetchall()

    conn.close()

    return templates.TemplateResponse("users.html", {
        "request": request,
        "users": users,
        "clients": clients
    })
# --------------------------------
# MANAGE CLIENTS
# --------------------------------

@app.get("/admin/clients", response_class=HTMLResponse)
def manage_clients(request: Request):

    if request.session.get("role") != "admin":
        return RedirectResponse("/login")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        SELECT id, name, company_id, external_rate, internal_rate
        FROM clients
        ORDER BY id DESC
    """)
    clients = c.fetchall()

    conn.close()

    return templates.TemplateResponse("clients.html", {
        "request": request,
        "clients": clients
    })
# --------------------------------
# ADD CLIENT
# --------------------------------

@app.post("/admin/add-client")
def add_client(
    request: Request,
    name: str = Form(...),
    external_rate: float = Form(...),
    internal_rate: float = Form(...)
):

    if request.session.get("role") != "admin":
        return {"status": "unauthorized"}

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()

    # Prevent duplicates (case-insensitive)
    c.execute("""
        SELECT id FROM clients
        WHERE LOWER(name)=LOWER(?) AND company_id=?
    """, (name.strip().lower(), request.session.get("company_id")))

    if c.fetchone():
        conn.close()
        return {"status": "exists"}

    c.execute("""
        INSERT INTO clients (name, company_id, external_rate, internal_rate)
        VALUES (?, ?, ?, ?)
    """, (
        name.strip(),
        request.session.get("company_id"),
        external_rate,
        internal_rate
    ))

    conn.commit()
    conn.close()

    return {"status": "success"}
# --------------------------------
# RECALCULATE ALL JOBS FOR ENGINEER
# --------------------------------

@app.post("/admin/recalculate-engineer/{engineer_id}")
def recalc_engineer(engineer_id: int, request: Request):

    if request.session.get("role") != "admin":
        return {"status": "unauthorized"}

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Get engineer info
    c.execute("SELECT name, lat, lng FROM engineers WHERE id=?", (engineer_id,))
    eng = c.fetchone()

    if not eng:
        conn.close()
        return {"status": "not_found"}

    name, lat, lng = eng
    origin = f"{lat},{lng}"

    # Get all upload rows for this engineer
    c.execute("""
        SELECT id, site_gps, return_trip
        FROM upload_rows
        WHERE engineer=?
    """, (name,))

    rows = c.fetchall()

    for row in rows:
        row_id, site_gps, return_trip = row

        if not site_gps:
            continue

        destination = site_gps

        # Check cache first
        c.execute("""
            SELECT distance_km, duration_minutes
            FROM route_cache
            WHERE origin=? AND destination=?
        """, (origin, destination))

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

            response = requests.get(url, params=params).json()
            element = response["rows"][0]["elements"][0]

            if element["status"] != "OK":
                continue

            km = round(element["distance"]["value"] / 1000, 2)
            minutes = round(element["duration"]["value"] / 60, 1)

            c.execute("""
                INSERT INTO route_cache
                (origin, destination, origin_label, destination_label,
                 distance_km, duration_minutes, added_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                origin,
                destination,
                name,
                destination,
                km,
                minutes,
                request.session.get("username"),
                datetime.now().isoformat()
            ))

        final_km = km * 2 if return_trip == 1 else km
        final_minutes = minutes * 2 if return_trip == 1 else minutes

        c.execute("""
            UPDATE upload_rows
            SET system_km=?, system_minutes=?,
                final_km=?, final_travel_minutes=?,
                status='OK'
            WHERE id=?
        """, (km, minutes, final_km, final_minutes, row_id))

    conn.commit()
    conn.close()

    return {"status": "done"}
#--------
#DELETEE ROUTE FROM CACHE
#--------
@app.post("/route-cache/delete/{route_id}")
def delete_route(route_id: int, request: Request):

    if request.session.get("role") != "admin":
        return {"status": "unauthorized"}

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("DELETE FROM route_cache WHERE id=?", (route_id,))

    conn.commit()
    conn.close()

    return {"status": "deleted"}
#--------
#verify engineer on the upload
#--------
@app.post("/admin/verify-engineer/{engineer_id}")
async def verify_engineer(engineer_id: int, request: Request):

    form = await request.form()

    address = form.get("home_address")
    lat = form.get("lat")
    lng = form.get("lng")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        UPDATE engineers
        SET home_address=?,
            lat=?,
            lng=?,
            verified=1
        WHERE id=?
    """, (address, lat, lng, engineer_id))

    conn.commit()
    conn.close()

    return {"status": "verified"}