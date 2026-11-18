"""
Detektor ispada – primjenjuje pravilo kvoruma i otvara/zatvara incidente.

Što radi:
  1. Iz vlastitog reda "rezultati.detektor" (na fanout exchangeu "rezultati")
     prima KOPIJU svakog rezultata provjere i pamti zadnje rezultate po
     servisu i sondi.
  2. Iz baze čita koje su sonde dodijeljene kojem servisu (tablica dodjele,
     vlasnik je raspoređivač) i intervale servisa (tablica servisi).
  3. Svakih TAKT_S sekundi za svaki servis primijeni pravilo kvoruma
     (kvorum.py) i zapiše stanje u tablicu stanje_servisa.
  4. Kad kvorum kaže "ne radi", otvori incident; kad kaže "radi", zatvori ga.
     O svakom otvaranju i zatvaranju objavi događaj na exchange "incidenti"
     (za buduću uslugu obavijesti).

Detektor je JEDINI pisac tablica stanje_servisa i incidenti.

Radi kao jedna instanca, jer mora vidjeti SVE rezultate jednog servisa
(replike bi dijelile poruke iz reda i svaka bi vidjela samo dio glasova).
Ako padne:
  - rezultati za to vrijeme čekaju u njegovom redu i ništa se ne gubi;
  - nakon ponovnog pokretanja zadnje rezultate učita iz baze (tablica
    rezultati), otvorene incidente također, i nastavlja gdje je stao.

Baza jamči da servis nikad nema dva otvorena incidenta (jedinstveni
indeks), pa je otvaranje idempotentno i nakon ponovnog pokretanja.
"""

import asyncio
import json
import logging
import os
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import aio_pika
import asyncpg
from fastapi import FastAPI, HTTPException, Response

from kvorum import UZASTOPNO, Procjena, Rezultat, procijeni

# --- Postavke ---------------------------------------------------------------

RABBITMQ_URL = os.environ["RABBITMQ_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
TAKT_S = float(os.getenv("TAKT_S", "2"))
# Rezultat stariji od ovoliko intervala servisa ne glasa (+ ISTEK_S za sporu provjeru)
SVJEZE_INTERVALA = float(os.getenv("SVJEZE_INTERVALA", "3"))
ISTEK_S = float(os.getenv("ISTEK_S", "10"))
# Koliko rezultata po (servis, sonda) pamtimo u memoriji
PAMTI = int(os.getenv("PAMTI", "5"))

EXCHANGE_REZULTATI = "rezultati"
RED_DETEKTOR = "rezultati.detektor"
EXCHANGE_INCIDENTI = "incidenti"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [detektor] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("detektor")

# --- Stanje u memoriji ------------------------------------------------------

# (servis_id, sonda_id) -> zadnjih PAMTI rezultata: (vrijeme, uspjeh, provjera_id)
rezultati: dict[tuple[str, str], deque] = defaultdict(lambda: deque(maxlen=PAMTI))
dodjele: dict[str, list[str]] = {}       # servis_id -> [sonda_id, ...]
servisi: dict[str, dict] = {}            # servis_id -> {naziv, interval_s, aktivan}
procjene: dict[str, Procjena] = {}       # servis_id -> zadnja procjena
otvoreni: dict[str, int] = {}            # servis_id -> id otvorenog incidenta
lokalni: dict[str, set[str]] = {}        # servis_id -> sonde s lokalnim problemom (za log)
stanje = {"pool": None, "veza": None, "exchange": None, "zadaci": [], "spreman": False}


# --- Baza -------------------------------------------------------------------------

SHEMA = """
CREATE TABLE IF NOT EXISTS stanje_servisa (
    servis_id         text        PRIMARY KEY,
    stanje            text        NOT NULL,     -- radi | ne_radi | nepoznato | iskljucen
    od                timestamptz,              -- kad je stanje nastupilo (po kvorumu)
    kvorum            integer     NOT NULL,
    glasovi           jsonb       NOT NULL,     -- {"sonda-a": "radi", ...}
    lokalni_problemi  text[]      NOT NULL DEFAULT '{}',
    izmijenjeno       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS incidenti (
    id                 bigserial   PRIMARY KEY,
    servis_id          text        NOT NULL,
    pocetak            timestamptz NOT NULL,
    kraj               timestamptz,
    sonde_potvrdile    text[]      NOT NULL,   -- sonde koje su glasale "pao"
    razlog_zatvaranja  text                    -- oporavak | rucno | servis_iskljucen
);

-- Servis smije imati najviše JEDAN otvoren incident.
CREATE UNIQUE INDEX IF NOT EXISTS incidenti_jedan_otvoren ON incidenti (servis_id) WHERE kraj IS NULL;
CREATE INDEX IF NOT EXISTS incidenti_servis_pocetak ON incidenti (servis_id, pocetak DESC);
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


async def ucitaj_dodjele_i_servise(pool: asyncpg.Pool):
    """Tablice drugih usluga samo čitamo. Ako još ne postoje, pokušamo ponovno kasnije."""
    async with pool.acquire() as conn:
        try:
            redovi = await conn.fetch("SELECT servis_id, sonda_id FROM dodjele")
            novo: dict[str, list[str]] = {}
            for r in redovi:
                novo.setdefault(r["servis_id"], []).append(r["sonda_id"])
            dodjele.clear()
            dodjele.update(novo)
        except asyncpg.UndefinedTableError:
            pass
        try:
            redovi = await conn.fetch("SELECT id, naziv, interval_s, aktivan FROM servisi")
            servisi.clear()
            servisi.update({r["id"]: dict(r) for r in redovi})
        except asyncpg.UndefinedTableError:
            pass


async def ucitaj_iz_baze_nakon_pokretanja(pool: asyncpg.Pool):
    """Zadnji rezultati i otvoreni incidenti, da nastavimo gdje smo stali."""
    async with pool.acquire() as conn:
        for r in await conn.fetch("SELECT id, servis_id FROM incidenti WHERE kraj IS NULL"):
            otvoreni[r["servis_id"]] = r["id"]
        try:
            redovi = await conn.fetch(
                """
                SELECT servis_id, sonda_id, vrijeme, uspjeh, provjera_id FROM (
                    SELECT *, row_number() OVER (PARTITION BY servis_id, sonda_id ORDER BY vrijeme DESC) AS rb
                    FROM rezultati WHERE vrijeme > now() - interval '15 minutes'
                ) t WHERE rb <= $1 ORDER BY vrijeme
                """,
                PAMTI,
            )
        except asyncpg.UndefinedTableError:
            redovi = []
    for r in redovi:
        zapamti(r["servis_id"], r["sonda_id"], r["vrijeme"], r["uspjeh"], r["provjera_id"])
    log.info("Učitano iz baze: %d rezultata, %d otvorenih incidenata", len(redovi), len(otvoreni))


# --- Primanje rezultata ---------------------------------------------------------

def zapamti(servis_id, sonda_id, vrijeme, uspjeh, provjera_id):
    niz = rezultati[(servis_id, sonda_id)]
    # Duplikat (ista provjera stigla dvaput) ili rezultat stariji od
    # najnovijeg koji već imamo – ne mijenja ništa.
    if any(p == provjera_id for _, _, p in niz):
        return
    if niz and vrijeme < niz[-1][0]:
        # izvan redoslijeda: umetni na pravo mjesto
        stari = sorted([*niz, (vrijeme, uspjeh, provjera_id)])
        niz.clear()
        niz.extend(stari[-PAMTI:])
        return
    niz.append((vrijeme, uspjeh, provjera_id))


async def primi_rezultat(poruka: aio_pika.abc.AbstractIncomingMessage):
    # Obrada je samo upis u memoriju, pa poruku potvrđujemo odmah. Ako detektor
    # padne, zadnje rezultate ionako učita iz baze (koju puni prihvat).
    async with poruka.process():
        try:
            r = json.loads(poruka.body)
            zapamti(str(r["servis_id"]), str(r["sonda_id"]),
                    datetime.fromisoformat(r["vrijeme"]), bool(r["uspjeh"]), str(r["provjera_id"]))
        except (ValueError, KeyError, TypeError):
            log.warning("Neispravan rezultat odbačen: %r", poruka.body[:200])


# --- Procjena i incidenti -----------------------------------------------------------

async def objavi_dogadaj(vrsta: str, incident_id: int, servis_id: str, vrijeme: datetime):
    dogadaj = {
        "vrsta": vrsta,                 # pocetak | kraj
        "incident_id": incident_id,
        "servis_id": servis_id,
        "naziv": servisi.get(servis_id, {}).get("naziv", servis_id),
        "vrijeme": vrijeme.isoformat(),
    }
    try:
        await stanje["exchange"].publish(
            aio_pika.Message(
                body=json.dumps(dogadaj).encode(),
                content_type="application/json",
                # Isti incident i ista vrsta = ista poruka; po ovome će usluga
                # obavijesti prepoznati duplikat i ne poslati obavijest dvaput.
                message_id=f"{incident_id}:{vrsta}",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="",
        )
    except Exception as e:
        log.error("Objava događaja nije uspjela: %s", e)


async def otvori_incident(conn, servis_id: str, p: Procjena):
    potvrdile = [s for s, g in p.glasovi.items() if g == "pao"]
    red = await conn.fetchrow(
        """
        INSERT INTO incidenti (servis_id, pocetak, sonde_potvrdile)
        VALUES ($1, $2, $3)
        ON CONFLICT (servis_id) WHERE kraj IS NULL DO NOTHING
        RETURNING id
        """,
        servis_id, p.od, potvrdile,
    )
    if red is None:
        # Već postoji otvoren incident (npr. otvoren prije ponovnog pokretanja)
        red = await conn.fetchrow("SELECT id FROM incidenti WHERE servis_id = $1 AND kraj IS NULL", servis_id)
        otvoreni[servis_id] = red["id"]
        return
    otvoreni[servis_id] = red["id"]
    log.warning("INCIDENT #%d OTVOREN: %s ne radi (potvrdile sonde: %s)",
                red["id"], servis_id, ", ".join(potvrdile))
    await objavi_dogadaj("pocetak", red["id"], servis_id, p.od)


async def zatvori_incident(conn, servis_id: str, kraj: datetime, razlog: str):
    incident_id = otvoreni.pop(servis_id, None)
    if incident_id is None:
        return
    red = await conn.fetchrow(
        "UPDATE incidenti SET kraj = $2, razlog_zatvaranja = $3 WHERE id = $1 AND kraj IS NULL "
        "RETURNING pocetak",
        incident_id, kraj, razlog,
    )
    if red is None:
        return   # već zatvoren (npr. ručno)
    trajanje = kraj - red["pocetak"]
    log.warning("INCIDENT #%d ZATVOREN: %s (%s, trajao %s)",
                incident_id, servis_id, razlog, str(trajanje).split(".")[0])
    await objavi_dogadaj("kraj", incident_id, servis_id, kraj)


def zapisi_lokalne_probleme(servis_id: str, p: Procjena):
    novi = set(p.lokalni_problemi)
    stari = lokalni.get(servis_id, set())
    for s in sorted(novi - stari):
        log.warning("Lokalni problem: %s ne dohvaća %s, a ostale sonde da (nije incident)", s, servis_id)
    for s in sorted(stari - novi):
        log.info("Lokalni problem riješen: %s ponovno dohvaća %s", s, servis_id)
    lokalni[servis_id] = novi


async def procijeni_sve(pool: asyncpg.Pool):
    sada = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        for sid, servis in sorted(servisi.items()):
            if not servis["aktivan"]:
                p = Procjena("iskljucen", {}, kvorum=0)
                if sid in otvoreni:
                    await zatvori_incident(conn, sid, sada, "servis_iskljucen")
            else:
                max_starost = timedelta(seconds=servis["interval_s"] * SVJEZE_INTERVALA + ISTEK_S)
                po_sondi = {
                    sonda: [Rezultat(v, u) for v, u, _ in rezultati[(sid, sonda)]]
                    for sonda in dodjele.get(sid, [])
                }
                p = procijeni(po_sondi, dodjele.get(sid, []), sada, max_starost)

                if p.stanje == "ne_radi" and sid not in otvoreni:
                    await otvori_incident(conn, sid, p)
                elif p.stanje == "radi" and sid in otvoreni:
                    await zatvori_incident(conn, sid, p.od or sada, "oporavak")
                # "nepoznato" ne mijenja incident: otvoren ostaje otvoren dok
                # kvorum ne potvrdi oporavak, a zatvoren se ne otvara bez kvoruma.
                zapisi_lokalne_probleme(sid, p)

            prethodna = procjene.get(sid)
            if prethodna is None or (prethodna.stanje, prethodna.glasovi, prethodna.lokalni_problemi) != \
                    (p.stanje, p.glasovi, p.lokalni_problemi):
                if prethodna is not None and prethodna.stanje != p.stanje:
                    log.info("%s: %s -> %s  %s", sid, prethodna.stanje, p.stanje, p.glasovi)
                await conn.execute(
                    """
                    INSERT INTO stanje_servisa (servis_id, stanje, od, kvorum, glasovi, lokalni_problemi)
                    VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                    ON CONFLICT (servis_id) DO UPDATE SET
                        stanje = EXCLUDED.stanje, kvorum = EXCLUDED.kvorum,
                        glasovi = EXCLUDED.glasovi, lokalni_problemi = EXCLUDED.lokalni_problemi,
                        -- "od" se mijenja samo kad se promijeni stanje
                        od = CASE WHEN stanje_servisa.stanje = EXCLUDED.stanje
                                  THEN stanje_servisa.od ELSE EXCLUDED.od END,
                        izmijenjeno = now()
                    """,
                    sid, p.stanje, p.od, p.kvorum, json.dumps(p.glasovi), p.lokalni_problemi,
                )
            procjene[sid] = p

        # Obrisani servisi: makni im stanje i zatvori incident
        for sid in set(procjene) - set(servisi):
            if sid in otvoreni:
                await zatvori_incident(conn, sid, sada, "servis_iskljucen")
            await conn.execute("DELETE FROM stanje_servisa WHERE servis_id = $1", sid)
            procjene.pop(sid, None)


async def glavna_petlja():
    pool = stanje["pool"]
    while True:
        try:
            await ucitaj_dodjele_i_servise(pool)
            await procijeni_sve(pool)
        except (asyncpg.PostgresError, OSError, asyncpg.InterfaceError) as e:
            log.error("Baza nedostupna (%s), pokušavam u sljedećem taktu", e)
        except Exception:
            log.exception("Neočekivana greška u procjeni")
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
    pool = await povezi_bazu()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(4245)")
            await conn.execute(SHEMA)
    await ucitaj_iz_baze_nakon_pokretanja(pool)

    veza = await povezi_rabbitmq()
    kanal = await veza.channel()
    await kanal.set_qos(prefetch_count=200)
    exchange_incidenti = await kanal.declare_exchange(
        EXCHANGE_INCIDENTI, aio_pika.ExchangeType.FANOUT, durable=True
    )
    exchange_rezultati = await kanal.declare_exchange(
        EXCHANGE_REZULTATI, aio_pika.ExchangeType.FANOUT, durable=True
    )
    # Vlastiti red: fanout exchange detektoru daje KOPIJU svakog rezultata,
    # neovisno o usluzi za prihvat.
    red = await kanal.declare_queue(RED_DETEKTOR, durable=True)
    await red.bind(exchange_rezultati)

    stanje.update(pool=pool, veza=veza, exchange=exchange_incidenti)
    await red.consume(primi_rezultat)
    stanje["zadaci"] = [asyncio.create_task(glavna_petlja())]
    log.info("Pokrenut. Kvorum: većina dodijeljenih sondi, %d uzastopne provjere po sondi.", UZASTOPNO)
    yield

    for z in stanje["zadaci"]:
        z.cancel()
    await veza.close()
    await pool.close()


app = FastAPI(title="Radi li? – detektor ispada", lifespan=lifespan)


@app.get("/stanje")
async def dohvati_stanje():
    """Trenutno stanje svakog servisa po kvorumu, s glasovima sondi."""
    return {
        sid: {
            "stanje": p.stanje,
            "od": p.od.isoformat() if p.od else None,
            "kvorum": p.kvorum,
            "glasovi": p.glasovi,
            "lokalni_problemi": p.lokalni_problemi,
            "otvoren_incident": otvoreni.get(sid),
        }
        for sid, p in sorted(procjene.items())
    }


@app.get("/incidenti")
async def dohvati_incidente(servis_id: str | None = None, dana: int = 30):
    async with stanje["pool"].acquire() as conn:
        redovi = await conn.fetch(
            """
            SELECT id, servis_id, pocetak, kraj, sonde_potvrdile, razlog_zatvaranja,
                   EXTRACT(EPOCH FROM (coalesce(kraj, now()) - pocetak))::int AS trajanje_s
            FROM incidenti
            WHERE pocetak > now() - make_interval(days => $1)
              AND ($2::text IS NULL OR servis_id = $2)
            ORDER BY pocetak DESC
            """,
            dana, servis_id,
        )
    return [dict(r) for r in redovi]


@app.post("/incidenti/{incident_id}/zatvori")
async def zatvori_rucno(incident_id: int):
    """
    Operater ručno zatvara incident. Ima smisla kad je servis zapravo
    proradio, a kvorum to još ne može potvrditi (npr. premalo živih sondi).
    Ako kvorum i dalje kaže da servis ne radi, u sljedećem taktu otvorit će se novi incident.
    """
    async with stanje["pool"].acquire() as conn:
        red = await conn.fetchrow("SELECT servis_id FROM incidenti WHERE id = $1 AND kraj IS NULL", incident_id)
        if red is None:
            raise HTTPException(404, "Nema otvorenog incidenta s tim ID-jem")
        if otvoreni.get(red["servis_id"]) != incident_id:
            otvoreni[red["servis_id"]] = incident_id
        await zatvori_incident(conn, red["servis_id"], datetime.now(timezone.utc), "rucno")
    return {"zatvoren": incident_id}


@app.get("/zdravlje")
async def zdravlje(response: Response):
    rabbit_ok = stanje["veza"] is not None and not stanje["veza"].is_closed
    if not rabbit_ok:
        response.status_code = 503
    return {"rabbitmq": rabbit_ok, "servisa": len(procjene), "otvorenih_incidenata": len(otvoreni)}
