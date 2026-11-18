"""
Sonda – periodički provjerava servise i objavljuje rezultate u RabbitMQ.

Svaka sonda glumi jednu lokaciju (npr. Pula, Rijeka, Zagreb). Ne zna ništa
o bazi ni o drugim sondama: samo provjerava i objavljuje. O tome je li
servis stvarno pao odlučuje kasnije detektor ispada (kvorum 2 od 3).

Komunikacija sonde (sve asinkrono, kroz RabbitMQ):
  - svakih OTKUCAJ_S sekundi šalje otkucaj (heartbeat) u red "otkucaji",
    da raspoređivač zna da je živa;
  - iz svog reda "raspored.<SONDA_ID>" prima popis servisa koje provjerava;
  - rezultate objavljuje na fanout exchange "rezultati".

Ako raspoređivač ne radi, sonda nastavlja po posljednjem primljenom
rasporedu – praćenje ne staje.
"""

import asyncio
import json
import logging
import os
import signal
import socket
import time
from datetime import datetime, timezone

import aio_pika
import httpx

# --- Postavke (iz varijabli okruženja) -------------------------------------

SONDA_ID = os.environ["SONDA_ID"]
LOKACIJA = os.getenv("LOKACIJA", "nepoznato")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]
TAKT_S = float(os.getenv("TAKT_S", "1"))           # koliko često sonda gleda kome je došao red
ISTEK_S = float(os.getenv("ISTEK_S", "10"))        # timeout jedne provjere
OTKUCAJ_S = float(os.getenv("OTKUCAJ_S", "10"))    # koliko često šalje otkucaj

EXCHANGE_REZULTATI = "rezultati"
RED_PRIHVAT = "rezultati.prihvat"
RED_OTKUCAJI = "otkucaji"
EXCHANGE_RASPORED = "raspored"
RED_RASPORED = f"raspored.{SONDA_ID}"

# Svako pokretanje kontejnera je nova instanca; raspoređivač po tome
# prepoznaje da je sonda ponovno pokrenuta.
INSTANCA = f"{socket.gethostname()}-{int(time.time())}"

KORISNICKI_AGENT = "RadiLi-sonda/0.3 (studentski projekt, FIPU)"

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s %(levelname)-7s [{SONDA_ID}/{LOKACIJA}] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sonda")

# Stanje sonde – samo lokalno, ništa od ovoga nije zajedničko s drugima
servisi: dict[str, dict] = {}          # servis_id -> {"id", "naziv", "url", "interval_s", ...}
zadnja_provjera: dict[str, float] = {}  # servis_id -> monotonic vrijeme zadnje provjere
u_tijeku: set[str] = set()             # servisi čija provjera još traje
zadnje_stanje: dict[str, bool] = {}    # servis_id -> je li zadnja provjera uspjela (za oznaku PROMJENA)
verzija_rasporeda = 0                  # verzija posljednjeg primljenog rasporeda


# --- Raspored (od raspoređivača) ----------------------------------------------

async def primi_raspored(poruka: aio_pika.abc.AbstractIncomingMessage):
    global verzija_rasporeda
    async with poruka.process():
        try:
            r = json.loads(poruka.body)
            verzija = int(r["verzija"])
            novi = {s["id"]: s for s in r["servisi"]}
        except (ValueError, KeyError, TypeError):
            log.warning("Neispravan raspored odbačen: %r", poruka.body[:200])
            return

        # Raspored koji stigne kasnije od novijeg se zanemaruje.
        if verzija <= verzija_rasporeda:
            return
        verzija_rasporeda = verzija

        if set(novi) != set(servisi):
            log.info("Novi raspored: %s", ", ".join(sorted(novi)) or "(bez servisa)")
        # Zaboravi servise koji više nisu naši
        for sid in set(servisi) - set(novi):
            zadnja_provjera.pop(sid, None)
            zadnje_stanje.pop(sid, None)
        servisi.clear()
        servisi.update(novi)


# --- Otkucaji -------------------------------------------------------------------

async def otkucaji_petlja(kanal: aio_pika.abc.AbstractChannel, stop: asyncio.Event):
    while not stop.is_set():
        otkucaj = {
            "sonda_id": SONDA_ID,
            "lokacija": LOKACIJA,
            "instanca": INSTANCA,
            "vrijeme": datetime.now(timezone.utc).isoformat(),
            "broj_servisa": len(servisi),
        }
        try:
            await kanal.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(otkucaj).encode(),
                    content_type="application/json",
                    # Otkucaj stariji od 3 intervala nema smisla – ako ga
                    # raspoređivač dotad ne pročita, RabbitMQ ga sam briše.
                    expiration=OTKUCAJ_S * 3,
                ),
                routing_key=RED_OTKUCAJI,
            )
        except Exception as e:
            log.error("Otkucaj nije poslan: %s", e)
        try:
            await asyncio.wait_for(stop.wait(), timeout=OTKUCAJ_S)
        except asyncio.TimeoutError:
            pass


# --- Provjera jednog servisa -----------------------------------------------

async def provjeri(klijent: httpx.AsyncClient, servis: dict) -> dict:
    """Pošalje GET na servis i vrati rezultat. Nikad ne baca iznimku."""
    krug = datetime.now(timezone.utc).replace(microsecond=0)
    pocetak = time.perf_counter()
    http_status = None
    greska = None

    try:
        odgovor = await klijent.get(servis["url"], timeout=ISTEK_S)
        http_status = odgovor.status_code
        uspjeh = 200 <= http_status < 400
        if not uspjeh:
            greska = f"http_{http_status}"
    except httpx.TimeoutException:
        uspjeh = False
        greska = "istek_vremena"
    except httpx.HTTPError as e:
        # npr. servis ne postoji, odbijena veza, DNS greška
        uspjeh = False
        greska = type(e).__name__

    odziv_ms = round((time.perf_counter() - pocetak) * 1000)

    return {
        # Jedinstveni ključ provjere: ista sonda, isti servis, isti trenutak.
        # Usluga za prihvat po njemu prepoznaje duplikate (idempotentnost).
        "provjera_id": f"{SONDA_ID}:{servis['id']}:{int(krug.timestamp())}",
        "sonda_id": SONDA_ID,
        "lokacija": LOKACIJA,
        "servis_id": servis["id"],
        "url": servis["url"],
        "vrijeme": krug.isoformat(),
        "uspjeh": uspjeh,
        "http_status": http_status,
        "odziv_ms": odziv_ms,
        "greska": greska,
    }


async def provjeri_i_objavi(klijent, exchange, servis: dict):
    try:
        r = await provjeri(klijent, servis)
        ispisi(servis, r)
        try:
            await exchange.publish(
                aio_pika.Message(
                    body=json.dumps(r).encode(),
                    content_type="application/json",
                    message_id=r["provjera_id"],
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="",
            )
        except Exception as e:
            # Veza je pukla; connect_robust je obnavlja u pozadini.
            # Ovaj rezultat se gubi, sljedeća provjera ide normalno.
            log.error("Objava nije uspjela: %s", e)
    finally:
        u_tijeku.discard(servis["id"])


# --- Spajanje na RabbitMQ ----------------------------------------------------

async def povezi_rabbitmq() -> aio_pika.abc.AbstractRobustConnection:
    """Spaja se na RabbitMQ; dok nije dostupan, pokušava ponovno s rastućim razmakom."""
    cekanje = 1
    while True:
        try:
            return await aio_pika.connect_robust(RABBITMQ_URL)
        except Exception as e:
            log.warning("RabbitMQ nije dostupan (%s), novi pokušaj za %s s", e, cekanje)
            await asyncio.sleep(cekanje)
            cekanje = min(cekanje * 2, 30)


# --- Glavna petlja -------------------------------------------------------------

async def main():
    stop = asyncio.Event()
    petlja = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # docker compose stop šalje SIGTERM – završi uredno
        petlja.add_signal_handler(sig, stop.set)

    veza = await povezi_rabbitmq()
    kanal = await veza.channel(publisher_confirms=True)
    exchange = await kanal.declare_exchange(
        EXCHANGE_REZULTATI, aio_pika.ExchangeType.FANOUT, durable=True
    )
    red = await kanal.declare_queue(RED_PRIHVAT, durable=True)
    await red.bind(exchange)

    # Otkucaji idu u zajednički red koji čita raspoređivač.
    await kanal.declare_queue(RED_OTKUCAJI, durable=True)

    # Vlastiti red za raspored. Čuva samo NAJNOVIJU poruku (x-max-length 1,
    # starije se odbacuju): sondu zanima samo trenutni raspored. Red je
    # trajan, pa ponovno pokrenuta sonda odmah dobije zadnji raspored.
    raspored_exchange = await kanal.declare_exchange(
        EXCHANGE_RASPORED, aio_pika.ExchangeType.DIRECT, durable=True
    )
    red_raspored = await kanal.declare_queue(
        RED_RASPORED, durable=True,
        arguments={"x-max-length": 1, "x-overflow": "drop-head"},
    )
    await red_raspored.bind(raspored_exchange, routing_key=SONDA_ID)
    await red_raspored.consume(primi_raspored)

    async with httpx.AsyncClient(
        headers={"User-Agent": KORISNICKI_AGENT},
        follow_redirects=True,
    ) as klijent:
        log.info("Pokrenuta, čekam raspored.")
        # Red za raspored postoji prije prvog otkucaja, pa raspored ne može promašiti sondu.
        zadaci = {asyncio.create_task(otkucaji_petlja(kanal, stop))}

        while not stop.is_set():
            sada = time.monotonic()
            for servis in list(servisi.values()):
                sid = servis["id"]
                dosao_red = sada - zadnja_provjera.get(sid, 0) >= servis["interval_s"]
                # Ako prethodna provjera istog servisa još traje (spor servis),
                # ne pokrećemo novu – ne želimo ga dodatno opterećivati.
                if dosao_red and sid not in u_tijeku:
                    zadnja_provjera[sid] = sada
                    u_tijeku.add(sid)
                    # Svaka provjera je zaseban zadatak: spor servis
                    # ne zadržava provjere ostalih.
                    z = asyncio.create_task(provjeri_i_objavi(klijent, exchange, servis))
                    zadaci.add(z)
                    z.add_done_callback(zadaci.discard)
            try:
                await asyncio.wait_for(stop.wait(), timeout=TAKT_S)
            except asyncio.TimeoutError:
                pass

        log.info("Zaustavljam se.")
        for z in zadaci:
            z.cancel()
    await veza.close()


def ispisi(servis: dict, r: dict):
    """
    Zapisuje SVAKU provjeru. Filtriranje se radi u terminalu, npr.:
      docker compose logs -f sonda-a | Select-String "PROMJENA"
    Uspjela provjera ide na razini INFO, neuspjela na razini WARNING.
    Kad se stanje servisa promijeni (pad ili povratak), redak počinje s PROMJENA.
    """
    sid = r["servis_id"]
    prethodno = zadnje_stanje.get(sid)
    zadnje_stanje[sid] = r["uspjeh"]
    promjena = "PROMJENA " if prethodno is not None and prethodno != r["uspjeh"] else ""

    naziv = servis.get("naziv", sid)
    if r["uspjeh"]:
        log.info("%s%-22s RADI     %s  %5d ms", promjena, naziv, r["http_status"], r["odziv_ms"])
    else:
        log.warning("%s%-22s NE RADI  %s  %5d ms", promjena, naziv, r["greska"], r["odziv_ms"])


if __name__ == "__main__":
    asyncio.run(main())
