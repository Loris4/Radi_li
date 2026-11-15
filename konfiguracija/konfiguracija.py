"""
Usluga konfiguracije – vlasnik popisa servisa koje sustav prati.

Ova usluga je JEDINI pisac tablice servisi. Ostale komponente popis samo
čitaju preko REST-a (GET /servisi). Kasnije ovdje dolaze i pretplate.

Pravilo odgovornog praćenja (iz prijave): stvarni javni servisi smiju se
provjeravati najviše jednom u minuti (interval >= 60 s). Testni servisi
smiju češće (>= 10 s). Pravilo se provjerava i ovdje i u bazi (CHECK).

Dodavanje i izmjena servisa ide preko PUT /servisi/{id}, koji je
idempotentan: isti zahtjev poslan dvaput daje isti rezultat. Zato ga
klijent smije ponoviti nakon isteka vremena bez straha od duplikata.
"""

import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime

import asyncpg
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field, HttpUrl, model_validator

DATABASE_URL = os.environ["DATABASE_URL"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s [konfiguracija] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("konfiguracija")

ID_UZORAK = r"^[a-z0-9-]{2,40}$"
MIN_INTERVAL_TESTNI = 10
MIN_INTERVAL_JAVNI = 60

SHEMA = f"""
CREATE TABLE IF NOT EXISTS servisi (
    id           text        PRIMARY KEY CHECK (id ~ '{ID_UZORAK}'),
    naziv        text        NOT NULL,
    url          text        NOT NULL,
    interval_s   integer     NOT NULL CHECK (interval_s >= {MIN_INTERVAL_TESTNI}),
    testni       boolean     NOT NULL DEFAULT false,
    aktivan      boolean     NOT NULL DEFAULT true,
    stvoreno     timestamptz NOT NULL DEFAULT now(),
    izmijenjeno  timestamptz NOT NULL DEFAULT now(),
    -- javni servisi najviše jednom u minuti
    CONSTRAINT odgovorno_pracenje CHECK (testni OR interval_s >= {MIN_INTERVAL_JAVNI})
);
"""

# Početni popis. Testni servisi su aktivni; stvarni javni servisi su
# isključeni dok ih operater svjesno ne uključi.
POCETNI_SERVISI = [
    # id,        naziv,                  url,                               interval, testni, aktivan
    ("egradani", "e-Građani (testni)",  "http://testni-egradani:8000/",     10, True,  True),
    ("studomat", "Studomat (testni)",   "http://testni-studomat:8000/",     10, True,  True),
    ("merlin",   "Merlin (testni)",     "http://testni-merlin:8000/",       10, True,  True),
    ("gov-hr",   "e-Građani",           "https://gov.hr/",                  60, False, False),
    ("isvu",     "Studomat",            "https://www.isvu.hr/studomat/",    60, False, False),
    ("srce-merlin", "Merlin",           "https://moodle.srce.hr/",          60, False, False),
    ("fipu",     "FIPU",                "https://fipu.unipu.hr/",           60, False, False),
]

STUPCI = "id, naziv, url, interval_s, testni, aktivan, stvoreno, izmijenjeno"

stanje = {"pool": None}


# --- Modeli ---------------------------------------------------------------------

class ServisUlaz(BaseModel):
    """Tijelo zahtjeva za PUT /servisi/{id}."""
    naziv: str = Field(min_length=1, max_length=100)
    url: HttpUrl
    interval_s: int = Field(ge=MIN_INTERVAL_TESTNI, le=3600)
    testni: bool = False
    aktivan: bool = True

    @model_validator(mode="after")
    def odgovorno_pracenje(self):
        if not self.testni and self.interval_s < MIN_INTERVAL_JAVNI:
            raise ValueError(f"Javni servisi smiju se provjeravati najviše jednom u {MIN_INTERVAL_JAVNI} s")
        return self


class ServisIzmjena(BaseModel):
    """Tijelo zahtjeva za PATCH – sva polja su opcionalna."""
    naziv: str | None = Field(default=None, min_length=1, max_length=100)
    url: HttpUrl | None = None
    interval_s: int | None = Field(default=None, ge=MIN_INTERVAL_TESTNI, le=3600)
    aktivan: bool | None = None


class Servis(BaseModel):
    id: str
    naziv: str
    url: str
    interval_s: int
    testni: bool
    aktivan: bool
    stvoreno: datetime
    izmijenjeno: datetime


# --- Baza -------------------------------------------------------------------------

async def povezi_bazu() -> asyncpg.Pool:
    cekanje = 1
    while True:
        try:
            return await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        except Exception as e:
            log.warning("Baza nije dostupna (%s), novi pokušaj za %s s", e, cekanje)
            await asyncio.sleep(cekanje)
            cekanje = min(cekanje * 2, 30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await povezi_bazu()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(4243)")
            await conn.execute(SHEMA)
            # ON CONFLICT DO NOTHING: početni podaci upisuju se samo prvi put,
            # a kasnije izmjene operatera se ne pregaze pri ponovnom pokretanju.
            await conn.executemany(
                "INSERT INTO servisi (id, naziv, url, interval_s, testni, aktivan) "
                "VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING",
                POCETNI_SERVISI,
            )
    stanje["pool"] = pool
    log.info("Pokrenuta.")
    yield
    await pool.close()


app = FastAPI(title="Radi li? – usluga konfiguracije", lifespan=lifespan)


def provjeri_id(servis_id: str):
    if not re.fullmatch(ID_UZORAK, servis_id):
        raise HTTPException(422, "ID smije sadržavati samo mala slova, brojke i crticu (2–40 znakova)")


# --- REST -------------------------------------------------------------------------

@app.get("/servisi", response_model=list[Servis])
async def popis(aktivan: bool | None = None):
    """Popis servisa. Sonde (kasnije raspoređivač) traže ?aktivan=true."""
    async with stanje["pool"].acquire() as conn:
        if aktivan is None:
            redovi = await conn.fetch(f"SELECT {STUPCI} FROM servisi ORDER BY id")
        else:
            redovi = await conn.fetch(f"SELECT {STUPCI} FROM servisi WHERE aktivan = $1 ORDER BY id", aktivan)
    return [dict(r) for r in redovi]


@app.get("/servisi/{servis_id}", response_model=Servis)
async def dohvati(servis_id: str):
    async with stanje["pool"].acquire() as conn:
        red = await conn.fetchrow(f"SELECT {STUPCI} FROM servisi WHERE id = $1", servis_id)
    if red is None:
        raise HTTPException(404, "Servis ne postoji")
    return dict(red)


@app.put("/servisi/{servis_id}", response_model=Servis)
async def postavi(servis_id: str, ulaz: ServisUlaz, response: Response):
    """Dodaje servis ili ga zamjenjuje. Idempotentno: ponovljeni zahtjev daje isti ishod."""
    provjeri_id(servis_id)
    async with stanje["pool"].acquire() as conn:
        red = await conn.fetchrow(
            f"""
            INSERT INTO servisi (id, naziv, url, interval_s, testni, aktivan)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                naziv = EXCLUDED.naziv, url = EXCLUDED.url, interval_s = EXCLUDED.interval_s,
                testni = EXCLUDED.testni, aktivan = EXCLUDED.aktivan, izmijenjeno = now()
            RETURNING {STUPCI}, (xmax = 0) AS novi
            """,
            servis_id, ulaz.naziv, str(ulaz.url), ulaz.interval_s, ulaz.testni, ulaz.aktivan,
        )
    r = dict(red)
    if r.pop("novi"):
        response.status_code = 201
    log.info("Servis %s postavljen", servis_id)
    return r


@app.patch("/servisi/{servis_id}", response_model=Servis)
async def izmijeni(servis_id: str, izmjena: ServisIzmjena):
    """Djelomična izmjena, npr. {"aktivan": false} za privremeno isključivanje."""
    polja = izmjena.model_dump(exclude_none=True)
    if "url" in polja:
        polja["url"] = str(polja["url"])
    if not polja:
        return await dohvati(servis_id)

    postavke = ", ".join(f"{k} = ${i}" for i, k in enumerate(polja, start=2))
    async with stanje["pool"].acquire() as conn:
        try:
            red = await conn.fetchrow(
                f"UPDATE servisi SET {postavke}, izmijenjeno = now() WHERE id = $1 RETURNING {STUPCI}",
                servis_id, *polja.values(),
            )
        except asyncpg.CheckViolationError:
            raise HTTPException(422, f"Javni servisi smiju se provjeravati najviše jednom u {MIN_INTERVAL_JAVNI} s")
    if red is None:
        raise HTTPException(404, "Servis ne postoji")
    log.info("Servis %s izmijenjen: %s", servis_id, polja)
    return dict(red)


@app.delete("/servisi/{servis_id}", status_code=204)
async def obrisi(servis_id: str):
    """Briše servis. Idempotentno: brisanje nepostojećeg servisa također vraća 204."""
    async with stanje["pool"].acquire() as conn:
        await conn.execute("DELETE FROM servisi WHERE id = $1", servis_id)
    log.info("Servis %s obrisan", servis_id)


@app.get("/zdravlje")
async def zdravlje(response: Response):
    try:
        async with stanje["pool"].acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"baza": True}
    except Exception:
        response.status_code = 503
        return {"baza": False}
