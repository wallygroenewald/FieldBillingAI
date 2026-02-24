import sqlite3
from datetime import datetime
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import requests
import os
from dotenv import load_dotenv
from pathlib import Path
from starlette.middleware.sessions import SessionMiddleware
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="supersecretkey")

# ---------------------------------------------------
# BASIC SETUP
# ---------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
DB_PATH = BASE_DIR / "billing.db"

load_dotenv(dotenv_path=ENV_FILE, override=True)

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
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT,
            destination TEXT,
            distance_km REAL,
            duration_minutes REAL,
            created_at TEXT
        )
    """)

    conn.commit()
    conn.close()

init_db()

# ---------------------------------------------------
# HOME PAGE
# ---------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs ORDER BY id DESC")
    jobs = cursor.fetchall()
    conn.close()

    return templates.TemplateResponse(
        "index.html",
        {"request": request, "jobs": jobs}
    )

# ---------------------------------------------------
# CALCULATE DISTANCE
# ---------------------------------------------------

@app.post("/calculate-distance")
def calculate_distance(origin: str, destination: str):

    if not GOOGLE_API_KEY:
        return {"error": "Google API key missing"}

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

    element = data["rows"][0]["elements"][0]

    if element["status"] != "OK":
        return {"error": "Calculation failed"}

    return {
        "distance_km": round(element["distance"]["value"] / 1000, 2),
        "duration_minutes": round(element["duration"]["value"] / 60, 1),
        "status": "OK"
    }

# ---------------------------------------------------
# SAVE JOB
# ---------------------------------------------------

@app.post("/save-job")
def save_job(
    origin: str,
    destination: str,
    distance_km: float,
    duration_minutes: float
):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO jobs (origin, destination, distance_km, duration_minutes, created_at)
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
# DELETE SINGLE JOB
# ---------------------------------------------------

@app.get("/delete-job/{job_id}")
def delete_job(job_id: int, request: Request):

    if not request.session.get("admin"):
        return RedirectResponse("/admin")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()

    return RedirectResponse("/", status_code=302)


# ---------------------------------------------------
# CLEAR ALL JOBS
# ---------------------------------------------------

@app.post("/clear-jobs")
def clear_jobs():

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM jobs")
    conn.commit()
    conn.close()

    return RedirectResponse("/", status_code=303)

# ---------------------------------------------------
# EDIT JOB PAGE
# ---------------------------------------------------

@app.get("/edit-job/{job_id}")
def edit_job(job_id: int, request: Request):

    if not request.session.get("admin"):
        return RedirectResponse("/admin")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    job = cursor.fetchone()
    conn.close()

    return templates.TemplateResponse(
        "edit_job.html",
        {"request": request, "job": job}
    )


# ---------------------------------------------------
# UPDATE JOB
# ---------------------------------------------------

@app.post("/update-job/{job_id}")
def update_job(
    job_id: int,
    origin: str = Form(...),
    destination: str = Form(...),
    distance_km: float = Form(...),
    duration_minutes: float = Form(...)
):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE jobs
        SET origin = ?, destination = ?, distance_km = ?, duration_minutes = ?
        WHERE id = ?
    """, (
        origin,
        destination,
        distance_km,
        duration_minutes,
        job_id
    ))

    conn.commit()
    conn.close()

    return RedirectResponse("/", status_code=303)
@app.get("/admin")
def admin_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


    if not request.session.get("admin_logged_in"):
        return templates.TemplateResponse("login.html", {"request": request})


    # Load engineers
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM engineers")
    engineers = cursor.fetchall()
    conn.close()

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "engineers": engineers
        }
    )
from fastapi import Form

ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "password123"   # change later

@app.post("/admin/login")
def admin_login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        request.session["admin"] = True
        return RedirectResponse("/admin/dashboard", status_code=302)

    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": "Invalid credentials"}
    )

@app.get("/admin/dashboard")
def admin_dashboard(request: Request):

    if not request.session.get("admin"):
        return RedirectResponse("/admin")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs ORDER BY id DESC")
    jobs = cursor.fetchall()
    conn.close()

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "jobs": jobs
        }
    )


@app.get("/engineers")
def engineers_page(request: Request):
    if not request.session.get("admin"):
        return RedirectResponse("/admin")

    return templates.TemplateResponse("engineers.html", {"request": request})

@app.get("/sites")
def sites_page(request: Request):
    if not request.session.get("admin"):
        return RedirectResponse("/admin")

    return templates.TemplateResponse("sites.html", {"request": request})

@app.get("/route-cache")
def route_cache_page(request: Request):
    if not request.session.get("admin"):
        return RedirectResponse("/admin")

    return templates.TemplateResponse("route_cache.html", {"request": request})
@app.get("/admin/logout")
def admin_logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)

