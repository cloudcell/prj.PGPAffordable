import os
import json
import duckdb
import jwt  # 🔹 Import PyJWT
import datetime
from fastapi import FastAPI, HTTPException, Depends, status, Request, Header
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from hashlib import sha256
from typing import List, Dict

# 🔹 Secret Key for JWT
SECRET_KEY = "your_secret_key_here"  # Replace with a secure secret key
ALGORITHM = "HS256"  # JWT algorithm
TOKEN_EXPIRE_MINUTES = 60  # Token expiration time

# Database setup
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

# FastAPI app
app = FastAPI(title="Affordable API", description="API for molecular similarity, target data, and management system", version="1.2")

# Static files and templates for UI
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# OAuth2 token URL
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# 🔹 Function to create JWT Token
def create_access_token(data: dict, expires_delta: int = TOKEN_EXPIRE_MINUTES):
    to_encode = data.copy()
    expire = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=expires_delta)
    to_encode.update({"exp": expire})
    
    # ✅ Correctly using PyJWT's encode method
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


# 🔹 Function to decode JWT Token
# from fastapi import Cookie

from fastapi import Cookie, Request

def get_current_user(
    request: Request,
    token_cookie: str = Cookie(None)
):
    token = token_cookie  # ✅ Directly read from cookies

    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token")
        return username
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")



# 🔹 Root Redirect to Login Page
@app.get("/", response_class=RedirectResponse)
def root():
    return RedirectResponse(url="/login")

# 🔹 Login Endpoint with JWT Authentication
from fastapi.responses import JSONResponse

# from fastapi.responses import JSONResponse

@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    result = conn.execute(
        "SELECT username, password FROM users WHERE username=? AND password=?", 
        [form_data.username, sha256(form_data.password.encode()).hexdigest()]
    ).fetchone()

    if not result:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    username, _ = result  # Extract username

    # ✅ Generate JWT Token
    token = create_access_token(data={"sub": username})

    # ✅ Set token in HttpOnly cookie
    response = JSONResponse(content={"message": "Login successful"})
    response.set_cookie(
        key="token",
        value=token,
        httponly=True,
        secure=True,   # Set to False if testing locally on HTTP
        samesite="Lax"
    )

    return response



# 🔹 Register Endpoint
@app.post("/register")
def register_user(data: dict):
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        raise HTTPException(status_code=400, detail="Username and password are required")

    existing_user = conn.execute("SELECT username FROM users WHERE username = ?", [username]).fetchone()
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")

    new_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM users").fetchone()[0]
    conn.execute("INSERT INTO users (id, username, password) VALUES (?, ?, ?)", 
                 [new_id, username, sha256(password.encode()).hexdigest()])
    return {"message": "User registered successfully"}

# 🔹 Serve HTML Pages for Frontend
@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.get("/admin", response_class=HTMLResponse, dependencies=[Depends(get_current_user)])
def admin_page(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})

@app.get("/management", response_class=HTMLResponse, dependencies=[Depends(get_current_user)])
def management_page(request: Request):
    return templates.TemplateResponse("management.html", {"request": request})

# 🔹 Disease Management API
class DiseaseEntry(BaseModel):
    disease_id: str
    reference_drug_id: str
    replacement_drug_id: str

@app.get("/disease_management", response_model=List[Dict], dependencies=[Depends(get_current_user)])
def get_disease_entries():
    rows = conn.execute("SELECT * FROM disease_management").fetchall()
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
