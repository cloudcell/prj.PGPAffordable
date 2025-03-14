import os
import json
import duckdb
from fastapi import FastAPI, Query, HTTPException, Depends, status, Request
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import numpy as np
from typing import List, Dict
from pydantic import BaseModel
from hashlib import sha256
import hashlib
import secrets
from fastapi import FastAPI, HTTPException, Depends, status



# Directories
TABLE_IVPE_DIR = 'staging_area_03'

db_path = "bio_data.duck.db"
conn = duckdb.connect(db_path)

# Database Initialization
conn.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id BIGINT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL
    )
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS disease_management (
        id BIGINT PRIMARY KEY,
        disease_id TEXT NOT NULL,
        reference_drug_id TEXT NOT NULL,
        replacement_drug_id TEXT NOT NULL
    )
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS ivpe_table_selection (
        id BIGINT PRIMARY KEY,
        table_name TEXT UNIQUE NOT NULL,
        selected BOOLEAN NOT NULL DEFAULT 0
    )
""")

# OAuth2 authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
app = FastAPI(title="Affordable API", description="API for molecular similarity, target data, and management system", version="1.2")

# Static files and templates for admin UI
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Authentication Middleware
from fastapi import Header, HTTPException, Depends, Request

def get_current_user(request: Request):
    token = request.cookies.get("token")  # ✅ Get token from the cookie

    if not token:
        raise HTTPException(status_code=401, detail="No authentication token provided")

    # Validate token against stored tokens
    user = conn.execute("SELECT username FROM users WHERE token = ?", [token]).fetchone()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid token or user not authenticated")

    return user[0]  # ✅ Return the authenticated username




@app.get("/", response_class=RedirectResponse)
def root():
    return RedirectResponse(url="/login")


# Secure Token Generation
# Secure token generation
def generate_token(username):
    random_string = secrets.token_hex(16)  # Generate random 16-byte string
    hashed_token = hashlib.sha256(f"{username}{random_string}".encode()).hexdigest()
    return hashed_token

from fastapi.responses import JSONResponse

@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = conn.execute("SELECT * FROM users WHERE username=? AND password=?", 
                        [form_data.username, sha256(form_data.password.encode()).hexdigest()]).fetchone()

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    token = generate_token(user[1])  # ✅ Generate SHA-256 token

    # ✅ Store the token in the database for future validation
    conn.execute("UPDATE users SET token=? WHERE username=?", [token, user[1]])

    # ✅ Send token as an HttpOnly cookie
    response = JSONResponse(content={"message": "Login successful"})
    response.set_cookie(
        key="token", 
        value=token, 
        httponly=True, 
        secure=True, 
        samesite="Strict"
    )
    return response



@app.post("/register")
def register_user(username: str, password: str):
    try:
        hashed_password = sha256(password.encode()).hexdigest()  # ✅ Hash password before storing
        new_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM users").fetchone()[0]
        conn.execute("INSERT INTO users (id, username, password) VALUES (?, ?, ?)", 
                     [new_id, username, hashed_password])
    except duckdb.CatalogException:
        raise HTTPException(status_code=400, detail="Username already exists")
    return {"message": "User registered successfully"}


# Serve HTML Pages
@app.get("/admin", response_class=HTMLResponse, dependencies=[Depends(get_current_user)])
def admin_page(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.get("/management", response_class=HTMLResponse, dependencies=[Depends(get_current_user)])
def management_page(request: Request):
    return templates.TemplateResponse("management.html", {"request": request})

# Disease Management API
class DiseaseEntry(BaseModel):
    disease_id: str
    reference_drug_id: str
    replacement_drug_id: str

@app.get("/disease_management", response_model=List[Dict], dependencies=[Depends(get_current_user)])
def get_disease_entries():
    rows = conn.execute("SELECT * FROM disease_management").fetchall()
    print("Fetched Records:", rows)  # ✅ Debugging: Check if records exist in the backend
    return [{"id": r[0], "disease_id": r[1], "reference_drug_id": r[2], "replacement_drug_id": r[3]} for r in rows]


@app.post("/disease_management", dependencies=[Depends(get_current_user)])
def add_disease_entry(entry: DiseaseEntry):
    new_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM disease_management").fetchone()[0]
    conn.execute("""
        INSERT INTO disease_management (id, disease_id, reference_drug_id, replacement_drug_id)
        VALUES (?, ?, ?, ?)""", [new_id, entry.disease_id, entry.reference_drug_id, entry.replacement_drug_id])
    return {"message": "Entry added successfully"}

@app.delete("/disease_management/{entry_id}", dependencies=[Depends(get_current_user)])
def delete_disease_entry(entry_id: int):
    conn.execute("DELETE FROM disease_management WHERE id = ?", [entry_id])
    return {"message": "Entry deleted successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7334)
