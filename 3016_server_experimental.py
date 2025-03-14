import os
import json
import duckdb
from fastapi import FastAPI, Query, HTTPException, Depends, status, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import numpy as np
from typing import List, Dict
from pydantic import BaseModel
from hashlib import sha256

# Directories
TABLE_IVPE_DIR = 'staging_area_03'

db_path = "bio_data.duck.db"
conn = duckdb.connect(db_path)

# DuckDB for user management and table selection
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
app = FastAPI(title="Affordable API", description="API for molecular similarity, target data, and management system", version="1.1")

# Static files and templates for admin UI
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# User authentication
def verify_user(token: str = Depends(oauth2_scheme)):
    return True  # In production, implement proper user session verification

@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = conn.execute("SELECT * FROM users WHERE username=? AND password=?", 
                        [form_data.username, sha256(form_data.password.encode()).hexdigest()]).fetchone()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    return {"access_token": form_data.username, "token_type": "bearer"}

@app.post("/register")
async def register_user(data: dict):
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        raise HTTPException(status_code=400, detail="Username and password are required")

    try:
        new_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM users").fetchone()[0]
        conn.execute("INSERT INTO users (id, username, password) VALUES (?, ?, ?)", 
                     [new_id, username, sha256(password.encode()).hexdigest()])
    except duckdb.CatalogException:
        raise HTTPException(status_code=400, detail="Username already exists")
    
    return {"message": "User registered successfully"}


# Serve HTML Pages
@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    try:
        return templates.TemplateResponse("register.html", {"request": request})
    except Exception as e:
        print(f"Error serving register.html: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error - Cannot load register.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7334)
