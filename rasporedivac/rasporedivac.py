"""
Raspoređivač – odlučuje koja sonda provjerava koji servis.

Što radi:
  1. Prima otkucaje (heartbeat) sondi iz reda "otkucaji" i prati koja je živa.
     - bez otkucaja KASNI_S sekundi  -> sonda "kasni" (još joj ne oduzimamo posao)
     - bez otkucaja ISPALA_S sekundi -> sonda "ispala" (posao ide drugima)
  2. Od usluge konfiguracije dohvaća popis aktivnih servisa.
  3. Algoritmom iz raspodjela.py svaki servis dodjeljuje trima sondama
     s različitih lokacija, uz podjednako opterećenje.
  4. Svakoj sondi šalje njezin popis servisa kroz direct exchange "raspored"
     (routing key = sonda_id). Svaka sonda ima svoj red "raspored.<sonda_id>".
  5. Raspored sprema u tablicu "dodjele" (treba je detektor ispada za kvorum)
     i stanje sondi u tablicu "sonde" (za sučelje za upravljanje).

Raspoređivač radi kao JEDNA instanca. Ako padne:
  - sonde nastavljaju provjeravati po posljednjem primljenom rasporedu,
    pa praćenje ne staje;
  - nakon ponovnog pokretanja čita prethodni raspored iz baze, pričeka da se
    sonde jave i nastavlja bez nepotrebnog premještanja servisa.
Jedino što za vrijeme ispada ne radi je preraspodjela pri ispadu sonde.

Raspoređivač je jedini pisac tablica "dodjele" i "sonde".
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aio_pika
import asyncpg
import httpx
from fastapi import FastAPI, Response

from raspodjela import K, izracunaj_raspored, po_sondama

# --- Postavke ---------------------------------------------------------------

RABBITMQ_URL = os.environ["RABBITMQ_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
KONFIGURACIJA_URL = os.environ["KONFIGURACIJA_URL"]

OTKUCAJ_S = float(os.getenv("OTKUCAJ_S", "10"))     # koliko često sonde šalju otkucaj
KASNI_S = float(os.getenv("KASNI_S", "15"))         # nakon ovoliko bez otkucaja sonda "kasni"
ISPALA_S = float(os.getenv("ISPALA_S", "30"))       # nakon ovoliko (3 otkucaja) sonda je "ispala"
TAKT_S = float(os.getenv("TAKT_S", "2"))            # koliko često provjeravamo stanje
OSVJEZI_S = float(os.getenv("OSVJEZI_S", "15"))     # koliko često dohvaćamo popis servisa
PONOVI_S = float(os.getenv("PONOVI_S", "30"))       # koliko često svima ponovno šaljemo raspored
# Nakon pokretanja pričekaj da se sonde jave prije prve raspodjele. Inače bi
# prva sonda koja se javi dobila sve servise, a odmah zatim ih opet izgubila.
POCETNO_CEKANJE_S = float(os.getenv("POCETNO_CEKANJE_S", str(OTKUCAJ_S + 2)))

RED_OTKUCAJI = "otkucaji"
EXCHANGE_RASPORED = "raspored"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [rasporedivac] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rasporedivac")

# --- Stanje (samo u memoriji; trajni dio je u tablicama dodjele i sonde) -----

sonde: dict[str, dict] = {}          # sonda_id -> {lokacija, instanca, zadnji_otkucaj, ...}
servisi: dict[str, dict] = {}        # servis_id -> podaci iz konfiguracije
servisi_ucitani = False              # je li popis servisa ikad uspješno dohvaćen
raspored: dict[str, list[str]] = {}  # servis_id -> [sonda_id, ...]
poslano: dict[str, list] = {}        # sonda_id -> zadnji poslani popis (da ne šaljemo isto)
pokrenut = time.monotonic()
stanje = {"pool": None, "exchange": None, "veza": None, "zadaci": []}


# --- Baza -------------------------------------------------------------------------

SHEMA = """
CREATE TABLE IF NOT EXISTS sonde (
    sonda_id         text        PRIMARY KEY,
    lokacija         text        NOT NULL,
    instanca         text,
    stanje           text        NOT NULL,          -- aktivna | kasni | ispala
    zadnji_otkucaj   timestamptz NOT NULL,
    broj_servisa     integer     NOT NULL DEFAULT 0,
    izmijenjeno      timestamptz NOT NULL DEFAULT now()
);

-- Koja sonda provjerava koji servis. Čita ga detektor ispada (kvorum).
CREATE TABLE IF NOT EXISTS dodjele (
    servis_id   text        NOT NULL,
    sonda_id    text        NOT NULL,
    dodijeljeno timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (servis_id, sonda_id)
);
"""


async def povezi_bazu() -> asyncpg.Pool:
    cekanje = 1
    while True:
        try:
            return await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
        except Exception as e:
            log.warning("Baza nije dostupna (%s), novi pokušaj za %s s", e, cekanje)
            await asyncio.sleep(cekanje)
            cekanje = min(cekanje * 2, 30)


async def ucitaj_prethodni_raspored(pool: asyncpg.Pool) -> dict[str, list[str]]:
    async with pool.acquire() as conn:
        redovi = await conn.fetch("SELECT servis_id, sonda_id FROM dodjele")
    r: dict[str, list[str]] = {}
    for red in redovi:
        r.setdefault(red["servis_id"], []).append(red["sonda_id"])
    return r


async def spremi_raspored(pool: asyncpg.Pool, novi: dict[str, list[str]]):
    """Spremi raspored u jednoj transakciji. Postojeće dodjele zadržavaju vrijeme dodjele."""
    parovi = [(s, p) for s, lista in novi.items() for p in lista]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                DELETE FROM dodjele d
                WHERE NOT EXISTS (
                    SELECT 1 FROM unnest($1::text[], $2::text[]) AS n(servis_id, sonda_id)
                    WHERE n.servis_id = d.servis_id AND n.sonda_id = d.sonda_id
                )
                """,
                [s for s, _ in parovi], [p for _, p in parovi],
            )
            await conn.executemany(
                "INSERT INTO dodjele (servis_id, sonda_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                parovi,
            )


async def spremi_sonde(pool: asyncpg.Pool):
    broj = po_sondama(raspored, sonde)
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO sonde (sonda_id, lokacija, instanca, stanje, zadnji_otkucaj, broj_servisa)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (sonda_id) DO UPDATE SET
                lokacija = EXCLUDED.lokacija, instanca = EXCLUDED.instanca,
                stanje = EXCLUDED.stanje, zadnji_otkucaj = EXCLUDED.zadnji_otkucaj,
                broj_servisa = EXCLUDED.broj_servisa, izmijenjeno = now()
            """,
            [
                (sid, p["lokacija"], p["instanca"], p["stanje"], p["zadnji_otkucaj_utc"],
                 len(broj.get(sid, [])))
                for sid, p in sonde.items()
            ],
        )


# --- Otkucaji -------------------------------------------------------------------

async def primi_otkucaj(poruka: aio_pika.abc.AbstractIncomingMessage):
    async with poruka.process():
        try:
            o = json.loads(poruka.body)
            sid = str(o["sonda_id"])
            lokacija = str(o["lokacija"])
        except (ValueError, KeyError, TypeError):
            log.warning("Neispravan otkucaj odbačen: %r", poruka.body[:200])
            return

        prethodna = sonde.get(sid)
        if prethodna is None or prethodna["stanje"] == "ispala":
            log.info("Sonda %s (%s) se javila", sid, lokacija)
        elif prethodna["instanca"] != o.get("instanca"):
            log.info("Sonda %s ponovno pokrenuta (nova instanca)", sid)
            poslano.pop(sid, None)   # nova instanca – pošalji joj raspored ponovno
        elif prethodna["stanje"] == "kasni":
            log.info("Sonda %s se ponovno javlja", sid)

        sonde[sid] = {
            "lokacija": lokacija,
            "instanca": o.get("instanca"),
            "zadnji_otkucaj": time.monotonic(),
            "zadnji_otkucaj_utc": datetime.now(timezone.utc),
            "stanje": "aktivna",
        }


def azuriraj_stanja_sondi() -> bool:
    """Postavi kasni/ispala prema vremenu zadnjeg otkucaja. Vraća True ako je neka ispala."""
    sada = time.monotonic()
    ispala_neka = False
    for sid, p in sonde.items():
        tisina = sada - p["zadnji_otkucaj"]
        if tisina >= ISPALA_S and p["stanje"] != "ispala":
            p["stanje"] = "ispala"
            ispala_neka = True
            log.warning("Sonda %s ISPALA (bez otkucaja %.0f s), preraspodjeljujem njezine servise",
                        sid, tisina)
        elif KASNI_S <= tisina < ISPALA_S and p["stanje"] == "aktivna":
            p["stanje"] = "kasni"
            log.warning("Sonda %s kasni (bez otkucaja %.0f s)", sid, tisina)
    return ispala_neka


# --- Servisi iz konfiguracije -----------------------------------------------------

async def osvjezi_servise(klijent: httpx.AsyncClient):
    global servisi_ucitani
    try:
        odgovor = await klijent.get(f"{KONFIGURACIJA_URL}/servisi", params={"aktivan": "true"}, timeout=3)
        odgovor.raise_for_status()
        novi = {
            s["id"]: {k: s[k] for k in ("id", "naziv", "url", "interval_s")}
            for s in odgovor.json()
        }
    except (httpx.HTTPError, ValueError, KeyError) as e:
        log.warning("Konfiguracija nedostupna (%s), zadržavam posljednji popis (%d servisa)",
                    type(e).__name__, len(servisi))
        return
    if set(novi) != set(servisi) and servisi_ucitani:
        log.info("Popis servisa promijenjen: %s", ", ".join(sorted(novi)) or "(prazan)")
    servisi.clear()
    servisi.update(novi)
    servisi_ucitani = True


# --- Raspodjela i slanje ----------------------------------------------------------

async def rasporedi_i_posalji(svima: bool = False):
    global raspored
    zive = {sid: p["lokacija"] for sid, p in sonde.items() if p["stanje"] != "ispala"}
    novi = izracunaj_raspored(sorted(servisi), zive, raspored)

    if novi != raspored:
        if not zive and servisi:
            log.error("Nema nijedne žive sonde – servisi se trenutno ne provjeravaju!")
        else:
            opis = "; ".join(f"{sid}: {len(lista)}" for sid, lista in sorted(po_sondama(novi, zive).items()))
            log.info("Novi raspored (servisa po sondi) – %s", opis)
        await spremi_raspored(stanje["pool"], novi)
        raspored = novi

    # Svaka sonda dobiva puni popis svojih servisa (ne samo promjene), pa
    # je poruka idempotentna: primiti je dvaput ne mijenja ništa.
    za_sonde = po_sondama(raspored, sonde)
    for sid, ids in za_sonde.items():
        if sonde.get(sid, {}).get("stanje") == "ispala":
            ids = []   # ispaloj sondi pošalji prazan popis, za slučaj da se vrati
        popis = [servisi[i] for i in ids if i in servisi]
        if not svima and poslano.get(sid) == popis:
            continue
        await posalji_raspored(sid, popis)
        poslano[sid] = popis


async def posalji_raspored(sonda_id: str, popis: list[dict]):
    poruka = {
        "sonda_id": sonda_id,
        # Verzija raste s vremenom (i preko ponovnih pokretanja raspoređivača),
        # pa sonda prepoznaje i odbacuje stari raspored koji stigne kasno.
        "verzija": time.time_ns(),
        "servisi": popis,
    }
    try:
        await stanje["exchange"].publish(
            aio_pika.Message(
                body=json.dumps(poruka).encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=sonda_id,
        )
    except Exception as e:
        log.error("Slanje rasporeda sondi %s nije uspjelo: %s", sonda_id, e)
        poslano.pop(sonda_id, None)   # pokušaj ponovno u sljedećem taktu


async def glavna_petlja():
    async with httpx.AsyncClient() as klijent:
        zadnje_osvjezavanje = 0.0
        zadnje_ponavljanje = time.monotonic()
        while True:
            sada = time.monotonic()
            if sada - zadnje_osvjezavanje >= OSVJEZI_S:
                await osvjezi_servise(klijent)
                zadnje_osvjezavanje = sada

            azuriraj_stanja_sondi()

            if servisi_ucitani and sada - pokrenut >= POCETNO_CEKANJE_S:
                try:
                    svima = sada - zadnje_ponavljanje >= PONOVI_S
                    await rasporedi_i_posalji(svima=svima)
                    if svima:
                        zadnje_ponavljanje = sada
                    await spremi_sonde(stanje["pool"])
                except (asyncpg.PostgresError, OSError, asyncpg.InterfaceError) as e:
                    log.error("Baza nedostupna (%s), pokušavam u sljedećem taktu", e)
                except Exception:
                    log.exception("Neočekivana greška u raspodjeli")

            await asyncio.sleep(TAKT_S)


async def povezi_rabbitmq() -> aio_pika.abc.AbstractRobustConnection:
    cekanje = 1
    while True:
        try:
            return await aio_pika.connect_robust(RABBITMQ_URL)
        except Exception as e:
            log.warning("RabbitMQ nije dostupan (%s), novi pokušaj za %s s", e, cekanje)
            await asyncio.sleep(cekanje)
            cekanje = min(cekanje * 2, 30)


# --- FastAPI --------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global raspored
    pool = await povezi_bazu()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(4244)")
            await conn.execute(SHEMA)
    raspored = await ucitaj_prethodni_raspored(pool)
    if raspored:
        log.info("Učitan prethodni raspored (%d servisa); čekam %.0f s da se sonde jave",
                 len(raspored), POCETNO_CEKANJE_S)

    veza = await povezi_rabbitmq()
    kanal = await veza.channel()
    await kanal.set_qos(prefetch_count=50)
    exchange = await kanal.declare_exchange(EXCHANGE_RASPORED, aio_pika.ExchangeType.DIRECT, durable=True)
    red = await kanal.declare_queue(RED_OTKUCAJI, durable=True)
    await red.consume(primi_otkucaj)

    stanje.update(pool=pool, exchange=exchange, veza=veza)
    stanje["zadaci"] = [asyncio.create_task(glavna_petlja())]
    log.info("Pokrenut.")
    yield

    for z in stanje["zadaci"]:
        z.cancel()
    await veza.close()
    await pool.close()


app = FastAPI(title="Radi li? – raspoređivač", lifespan=lifespan)


@app.get("/sonde")
async def dohvati_sonde():
    """Stanje svih sondi koje su se ikad javile (za sučelje za upravljanje)."""
    sada = time.monotonic()
    broj = po_sondama(raspored, sonde)
    return [
        {
            "sonda_id": sid,
            "lokacija": p["lokacija"],
            "stanje": p["stanje"],
            "sekundi_od_otkucaja": round(sada - p["zadnji_otkucaj"], 1),
            "servisi": broj.get(sid, []),
        }
        for sid, p in sorted(sonde.items())
    ]


@app.get("/raspored")
async def dohvati_raspored():
    """Koje sonde provjeravaju koji servis."""
    return {
        "k": K,
        "servisi": {
            s: [{"sonda_id": p, "lokacija": sonde.get(p, {}).get("lokacija")} for p in lista]
            for s, lista in sorted(raspored.items())
        },
    }


@app.get("/zdravlje")
async def zdravlje(response: Response):
    rabbit_ok = stanje["veza"] is not None and not stanje["veza"].is_closed
    zive = sum(1 for p in sonde.values() if p["stanje"] != "ispala")
    if not rabbit_ok:
        response.status_code = 503
    return {"rabbitmq": rabbit_ok, "servisa": len(servisi), "zivih_sondi": zive}
