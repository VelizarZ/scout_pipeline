from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

app = FastAPI(title="Mock Scout API")

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)


@app.exception_handler(RateLimitExceeded)
def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    # Optional: provide a Retry-After hint (seconds).
    # slowapi doesn't always expose an exact retry time here, so we provide a safe default.
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded"},
        headers={"Retry-After": "60"},
    )


# Load CSV once
DATA_PATH = Path(__file__).resolve().parent / "players.csv"
df = pd.read_csv(DATA_PATH, encoding="latin1")

# Normalize IDs
df["id"] = pd.to_numeric(df["id"], errors="coerce")
df = df.dropna(subset=["id"]).copy()
df["id"] = df["id"].astype(int)

# Enforce deterministic behavior if duplicates exist
# Keep the first occurrence (or choose last, but be explicit).
df = df.drop_duplicates(subset=["id"], keep="first")

df.set_index("id", inplace=True)


@app.get("/player/{player_id}")
@limiter.limit("100/minute")
def get_player(player_id: int, request: Request):
    if player_id not in df.index:
        raise HTTPException(status_code=404, detail="Player not found")

    row = df.loc[player_id]

    # row is a Series due to drop_duplicates; safe to convert
    record = row.where(pd.notnull(row), None).to_dict()
    record["id"] = player_id
    return record


@app.get("/players/ids")
@limiter.limit("100/minute")  # optional, but consistent
def players_ids(request: Request):
    ids = df.index.tolist()
    return {"count": len(ids), "ids": ids}
    
@app.get("/health")
def health():
    return {"status": "ok", "players": int(df.shape[0])}
