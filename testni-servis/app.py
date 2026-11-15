"""
Testni servis – glumi javni servis (npr. e-Građani) koji sonde provjeravaju.

Normalno na GET / vraća 200. Kroz /upravljanje/... ga možemo:
  - ugasiti   -> GET / vraća 503 (servis "ne radi")
  - usporiti  -> GET / odgovara tek nakon zadanog broja milisekundi
                 (dobro za testiranje isteka vremena, timeouta)
  - vratiti u normalu

Stanje se drži u memoriji procesa. To je ovdje namjerno: servis je samo
alat za testiranje, a ne dio našeg sustava.
"""

import asyncio
import os
from datetime import datetime, timezone

from fastapi import FastAPI, Query, Response

IME_SERVISA = os.getenv("IME_SERVISA", "Testni servis")

app = FastAPI(title=IME_SERVISA)

stanje = {
    "radi": True,
    "kasnjenje_ms": 0,
}


@app.get("/")
async def pocetna(response: Response):
    """Ovo provjeravaju sonde."""
    if stanje["kasnjenje_ms"] > 0:
        await asyncio.sleep(stanje["kasnjenje_ms"] / 1000)

    if not stanje["radi"]:
        response.status_code = 503
        return {"servis": IME_SERVISA, "poruka": "Servis trenutno nije dostupan"}

    return {
        "servis": IME_SERVISA,
        "poruka": "Sve radi",
        "vrijeme": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/upravljanje/stanje")
async def dohvati_stanje():
    return {"servis": IME_SERVISA, **stanje}


@app.post("/upravljanje/ugasi")
async def ugasi():
    stanje["radi"] = False
    return {"servis": IME_SERVISA, **stanje}


@app.post("/upravljanje/upali")
async def upali():
    stanje["radi"] = True
    stanje["kasnjenje_ms"] = 0
    return {"servis": IME_SERVISA, **stanje}


@app.post("/upravljanje/uspori")
async def uspori(ms: int = Query(..., ge=0, le=60000, description="Kašnjenje u milisekundama")):
    stanje["kasnjenje_ms"] = ms
    return {"servis": IME_SERVISA, **stanje}
