from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.database import init_db
from app.routers import auth, users, records, meta


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create DB tables on startup."""
    await init_db()
    yield


app = FastAPI(
    title="VCDP FormCraft API",
    description=(
        "Backend API for the VCDP Transaction Tracking & 3FS Reporting System. "
        "Tracks VCDP expenditures 2013–2025, links to UN/IFAD 3FS framework."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ─── CORS ────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url, "http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Routers ──────────────────────────────────────────────────────────────────
app.include_router(auth.router)
app.include_router(users.router)
app.include_router(records.router)
app.include_router(meta.router)


@app.get("/api/health", tags=["health"])
async def health_check():
    return {"status": "ok", "service": "VCDP FormCraft API"}
