from fastapi import FastAPI, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
import pandas as pd

app = FastAPI()

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

@app.exception_handler(RateLimitExceeded)
def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return HTTPException(status_code=429, detail="Rate limit exceeded")

# Load CSV once (using latin1 to avoid Unicode issues)
df = pd.read_csv("players.csv", encoding="latin1")
df.set_index("id", inplace=True)

@app.get("/player/{player_id}")
@limiter.limit("100/minute")
def get_player(player_id: str, request: Request):
  # <-- include request here
    if player_id not in df.index:
        raise HTTPException(status_code=404, detail="Player not found")
    return df.loc[player_id].to_dict()
