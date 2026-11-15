"""
Usluga za prihvat – čita rezultate provjera iz RabbitMQ-a i sprema ih u PostgreSQL.

Najvažnija pravila (bitno za obranu):

1. Poruka se potvrđuje (ack) tek NAKON što je rezultat upisan u bazu.
   Ako usluga padne prije upisa, RabbitMQ poruku vraća u red i
   preuzima je druga replika ili ista usluga nakon ponovnog pokretanja.
   Nijedan rezultat se ne gubi.

2. Upis je idempotentan: ključ je provjera_id (sonda + servis + krug).
   Ako ista poruka stigne dvaput (npr. ack nije stigao do RabbitMQ-a pa je
   poruka ponovno isporučena), drugi upis ne radi ništa (ON CONFLICT DO NOTHING).

3. Usluga nema vlastito stanje: sve je u bazi i u redu. Zato se može
   pokrenuti u više replika (docker compose up --scale prihvat=3) koje se
   natječu za poruke iz istog reda (natjecanje potrošača).

4. Rezultati se upisuju u serijama (do 100 odjednom ili svake sekunde),
   jer je jedan upis od 100 redaka puno jeftiniji od 100 zasebnih upisa.

Ova usluga je JEDINI pisac tablice rezultati (vlasnik tog stanja).
"""

import asyncio
import json
import logging
import os
import socket
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

import aio_pika
import asyncpg
from fastapi import FastAPI, Response

# --- Postavke ---------------------------------------------------------------

RABBITMQ_URL = os.environ["RABBITMQ_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
VELICINA_SERIJE = int(os.getenv("VELICINA_SERIJE", "100"))
MAX_CEKANJE_SERIJE_S = float(os.getenv("MAX_CEKANJE_SERIJE_S", "1"))
SAZETAK_S = int(os.getenv("SAZETAK_S", "60"))      # koliko često ispisati sažetak u log

# Isto kao u sondi. Deklaracije su idempotentne, pa nije bitno tko ih prvi napravi.
EXCHANGE_REZULTATI = "rezultati"
RED_PRIHVAT = "rezultati.prihvat"

# Ime kontejnera – kad ima više replika, vidi se koja je što napravila
INSTANCA = socket.gethostname()

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s %(levelname)-7s [prihvat/{INSTANCA[:12]}] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prihvat")

# Statistika ove replike (za sučelje za upravljanje, kasnije)
statistika = {
    "instanca": INSTANCA,
    "primljeno": 0,       # poruka preuzeto iz reda
    "upisano": 0,         # novih redaka u bazi
    "duplikata": 0,       # poruka koje su već bile upisane
    "odbaceno": 0,        # neispravnih poruka
    "serija": 0,
    "zadnji_upis": None,
}

stanje = {"pool": None, "veza": None, "zadaci": []}


# --- Baza ----------------------------------------------------------------------

SHEMA = """
-- Rezultati su particionirani po danu: svaki dan ima svoju tablicu.
-- Upiti za zadnjih nekoliko sati čitaju samo jednu malu particiju,
-- a stare dane kasnije možemo obrisati jednom naredbom (DROP TABLE).
CREATE TABLE IF NOT EXISTS rezultati (
    provjera_id  text        NOT NULL,
    sonda_id     text        NOT NULL,
    lokacija     text        NOT NULL,
    servis_id    text        NOT NULL,
    vrijeme      timestamptz NOT NULL,
    uspjeh       boolean     NOT NULL,
    http_status  integer,
    odziv_ms     integer     NOT NULL,
    greska       text,
    primljeno    timestamptz NOT NULL DEFAULT now(),
    -- Kod particionirane tablice ključ mora sadržavati stupac po kojem se dijeli.
    -- provjera_id već sadrži vrijeme kruga, pa je ključ i dalje jedinstven po provjeri.
    PRIMARY KEY (provjera_id, vrijeme)
) PARTITION BY RANGE (vrijeme);

-- Rezervna particija za retke kojima dan nije unaprijed napravljen
CREATE TABLE IF NOT EXISTS rezultati_ostalo PARTITION OF rezultati DEFAULT;

-- Najčešći upit: "zadnji rezultati za servis X"
CREATE INDEX IF NOT EXISTS rezultati_servis_vrijeme ON rezultati (servis_id, vrijeme DESC);
"""

UPIS_SERIJE = """
INSERT INTO rezultati
    (provjera_id, sonda_id, lokacija, servis_id, vrijeme, uspjeh, http_status, odziv_ms, greska)
SELECT * FROM unnest(
    $1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[],
    $6::boolean[], $7::integer[], $8::integer[], $9::text[]
)
ON CONFLICT (provjera_id, vrijeme) DO NOTHING
RETURNING provjera_id
"""


async def povezi_bazu() -> asyncpg.Pool:
    cekanje = 1
    while True:
        try:
            return await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        except Exception as e:
            log.warning("Baza nije dostupna (%s), novi pokušaj za %s s", e, cekanje)
            await asyncio.sleep(cekanje)
            cekanje = min(cekanje * 2, 30)


async def osiguraj_particije(pool: asyncpg.Pool):
    """Napravi particije za danas i sutra (ako ne postoje)."""
    danas = datetime.now(timezone.utc).date()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Više replika može ovo raditi istovremeno; zaključavanje osigurava
            # da tablicu stvara samo jedna, a ostale čekaju i vide da postoji.
            await conn.execute("SELECT pg_advisory_xact_lock(4242)")
            for d in (danas, danas + timedelta(days=1)):
                await conn.execute(_sql_particije(d))


def _sql_particije(d: date) -> str:
    sljedeci = d + timedelta(days=1)
    return (
        f"CREATE TABLE IF NOT EXISTS rezultati_{d:%Y%m%d} PARTITION OF rezultati "
        f"FOR VALUES FROM ('{d.isoformat()}') TO ('{sljedeci.isoformat()}')"
    )


async def particije_petlja(pool: asyncpg.Pool):
    """Jednom na sat provjeri postoje li particije za danas i sutra."""
    while True:
        await asyncio.sleep(3600)
        try:
            await osiguraj_particije(pool)
        except Exception as e:
            log.error("Stvaranje particija nije uspjelo: %s", e)


# --- Obrada poruka --------------------------------------------------------------

def procitaj(poruka: aio_pika.abc.AbstractIncomingMessage) -> dict | None:
    """Pretvori poruku u rezultat; vrati None ako poruka nije ispravna."""
    try:
        r = json.loads(poruka.body)
        return {
            "provjera_id": str(r["provjera_id"]),
            "sonda_id": str(r["sonda_id"]),
            "lokacija": str(r["lokacija"]),
            "servis_id": str(r["servis_id"]),
            "vrijeme": datetime.fromisoformat(r["vrijeme"]),
            "uspjeh": bool(r["uspjeh"]),
            "http_status": r.get("http_status"),
            "odziv_ms": int(r["odziv_ms"]),
            "greska": r.get("greska"),
        }
    except (ValueError, KeyError, TypeError) as e:
        log.warning("Neispravna poruka odbačena (%s): %r", e, poruka.body[:200])
        return None


async def upisi(pool: asyncpg.Pool, zapisi: list[dict]) -> int:
    """Upiše seriju jednim upitom; vraća broj NOVIH redaka."""
    stupci = ["provjera_id", "sonda_id", "lokacija", "servis_id", "vrijeme",
              "uspjeh", "http_status", "odziv_ms", "greska"]
    async with pool.acquire() as conn:
        novi = await conn.fetch(UPIS_SERIJE, *[[z[s] for z in zapisi] for s in stupci])
    return len(novi)


async def obradi_seriju(pool: asyncpg.Pool, serija: list):
    ispravne, zapisi = [], []
    for poruka in serija:
        z = procitaj(poruka)
        if z is None:
            # Neispravnu poruku nema smisla vraćati u red – nikad neće uspjeti.
            # (U kasnijem koraku ide u red neuspjelih poruka, dead-letter.)
            await poruka.reject(requeue=False)
            statistika["odbaceno"] += 1
        else:
            ispravne.append(poruka)
            zapisi.append(z)

    if not zapisi:
        return

    # Ako baza ne radi, pokušavamo ponovno dok ne uspije. Poruke za to vrijeme
    # ostaju nepotvrđene u RabbitMQ-u, pa se ništa ne gubi ni ako ova usluga padne.
    cekanje = 1
    while True:
        try:
            novih = await upisi(pool, zapisi)
            break
        except (asyncpg.PostgresError, OSError, asyncpg.InterfaceError) as e:
            log.error("Upis nije uspio (%s), novi pokušaj za %s s", e, cekanje)
            await asyncio.sleep(cekanje)
            cekanje = min(cekanje * 2, 30)

    # Tek sada potvrđujemo poruke.
    for poruka in ispravne:
        try:
            await poruka.ack()
        except Exception as e:
            # Veza s RabbitMQ-om je pukla prije potvrde. Poruka će biti
            # ponovno isporučena, ali upis je idempotentan pa duplikata neće biti.
            log.warning("Potvrda nije uspjela (%s); poruka će doći ponovno", e)

    statistika["upisano"] += novih
    statistika["duplikata"] += len(zapisi) - novih
    statistika["serija"] += 1
    statistika["zadnji_upis"] = datetime.now(timezone.utc).isoformat()
    log.info("Upisano %d novih rezultata (%d duplikata)", novih, len(zapisi) - novih)


async def sazetak_petlja():
    """Jednom u minuti ispiše sažetak (filtrira se sa: Select-String "SAZETAK")."""
    prije = dict(statistika)
    while True:
        await asyncio.sleep(SAZETAK_S)
        upisano = statistika["upisano"] - prije["upisano"]
        duplikata = statistika["duplikata"] - prije["duplikata"]
        odbaceno = statistika["odbaceno"] - prije["odbaceno"]
        log.info("SAZETAK zadnjih %d s: upisano %d, duplikata %d, odbačeno %d",
                 SAZETAK_S, upisano, duplikata, odbaceno)
        prije = dict(statistika)


async def radnik(pool: asyncpg.Pool, ulaz: asyncio.Queue):
    """Skuplja poruke u serije: do VELICINA_SERIJE ili MAX_CEKANJE_SERIJE_S."""
    petlja = asyncio.get_running_loop()
    while True:
        serija = [await ulaz.get()]
        rok = petlja.time() + MAX_CEKANJE_SERIJE_S
        while len(serija) < VELICINA_SERIJE:
            preostalo = rok - petlja.time()
            if preostalo <= 0:
                break
            try:
                serija.append(await asyncio.wait_for(ulaz.get(), preostalo))
            except asyncio.TimeoutError:
                break
        try:
            await obradi_seriju(pool, serija)
        except Exception:
            log.exception("Neočekivana greška pri obradi serije")


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
    pool = await povezi_bazu()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(4242)")
            await conn.execute(SHEMA)
    await osiguraj_particije(pool)

    veza = await povezi_rabbitmq()
    kanal = await veza.channel()
    # Koliko nepotvrđenih poruka ova replika smije držati odjednom.
    # Dovoljno za dvije serije; ostatak ostaje u redu za druge replike.
    await kanal.set_qos(prefetch_count=VELICINA_SERIJE * 2)
    exchange = await kanal.declare_exchange(
        EXCHANGE_REZULTATI, aio_pika.ExchangeType.FANOUT, durable=True
    )
    red = await kanal.declare_queue(RED_PRIHVAT, durable=True)
    await red.bind(exchange)

    ulaz: asyncio.Queue = asyncio.Queue()

    async def primi(poruka):
        statistika["primljeno"] += 1
        await ulaz.put(poruka)

    await red.consume(primi)

    stanje.update(pool=pool, veza=veza)
    stanje["zadaci"] = [
        asyncio.create_task(radnik(pool, ulaz)),
        asyncio.create_task(particije_petlja(pool)),
        asyncio.create_task(sazetak_petlja()),
    ]
    log.info("Pokrenuta, čitam iz reda %s", RED_PRIHVAT)

    yield

    # Gašenje: nepotvrđene poruke RabbitMQ sam vraća u red kad se veza zatvori.
    for z in stanje["zadaci"]:
        z.cancel()
    await veza.close()
    await pool.close()


app = FastAPI(title="Radi li? – usluga za prihvat", lifespan=lifespan)


@app.get("/zdravlje")
async def zdravlje(response: Response):
    """Je li replika spojena na bazu i RabbitMQ (za sučelje za upravljanje)."""
    baza_ok = False
    try:
        async with stanje["pool"].acquire() as conn:
            await conn.fetchval("SELECT 1")
        baza_ok = True
    except Exception:
        pass
    rabbit_ok = stanje["veza"] is not None and not stanje["veza"].is_closed

    if not (baza_ok and rabbit_ok):
        response.status_code = 503
    return {"instanca": INSTANCA, "baza": baza_ok, "rabbitmq": rabbit_ok}


@app.get("/statistika")
async def dohvati_statistiku():
    return statistika
