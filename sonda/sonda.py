"""
Sonda – periodički provjerava servise i objavljuje rezultate u RabbitMQ.

Svaka sonda glumi jednu lokaciju (npr. Pula, Rijeka, Zagreb). Ne zna ništa
o bazi ni o drugim sondama: samo provjerava i objavljuje. O tome je li
servis stvarno pao odlučuje kasnije detektor ispada (kvorum 2 od 3).

U ovom koraku popis servisa dolazi iz varijable okruženja SERVISI.
U koraku 5 popis će slati raspoređivač.
"""

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import aio_pika
import httpx

# --- Postavke (iz varijabli okruženja) -------------------------------------

SONDA_ID = os.environ["SONDA_ID"]
LOKACIJA = os.getenv("LOKACIJA", "nepoznato")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]
INTERVAL_S = float(os.getenv("INTERVAL_S", "30"))   # koliko često provjeravamo
ISTEK_S = float(os.getenv("ISTEK_S", "10"))         # timeout jedne provjere
SERVISI = json.loads(os.environ["SERVISI"])         # [{"id", "naziv", "url"}, ...]

if ISTEK_S >= INTERVAL_S:
    raise SystemExit("ISTEK_S mora biti manji od INTERVAL_S, inače se krugovi provjera preklapaju")

# Rezultati se objavljuju na fanout exchange (objavi–pretplati se):
# svaka usluga koja ih treba (prihvat, detektor ispada) dobiva svoju kopiju.
EXCHANGE_REZULTATI = "rezultati"
# Red iz kojega će usluga za prihvat (korak 3) čitati rezultate.
# Deklariramo ga i ovdje da se rezultati ne gube dok prihvat još ne postoji.
# Deklaracija je idempotentna: ako red već postoji, ništa se ne mijenja.
RED_PRIHVAT = "rezultati.prihvat"

KORISNICKI_AGENT = "RadiLi-sonda/0.1 (studentski projekt, FIPU)"

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{SONDA_ID}/{LOKACIJA}] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sonda")


# --- Provjera jednog servisa -----------------------------------------------

async def provjeri(klijent: httpx.AsyncClient, servis: dict, krug: datetime) -> dict:
    """Pošalje GET na servis i vrati rezultat. Nikad ne baca iznimku."""
    pocetak = time.perf_counter()
    http_status = None
    greska = None

    try:
        odgovor = await klijent.get(servis["url"])
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
        # Jedinstveni ključ provjere: ista sonda, isti servis, isti krug.
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


# --- Spajanje na RabbitMQ ----------------------------------------------------

async def povezi_rabbitmq() -> aio_pika.abc.AbstractRobustConnection:
    """Spaja se na RabbitMQ; dok nije dostupan, pokušava ponovno s rastućim razmakom."""
    cekanje = 1
    while True:
        try:
            # connect_robust se sam ponovno spaja ako veza kasnije pukne
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

    log.info("Pokrenuta. Provjeravam %d servisa svakih %.0f s.", len(SERVISI), INTERVAL_S)

    async with httpx.AsyncClient(
        timeout=ISTEK_S,
        headers={"User-Agent": KORISNICKI_AGENT},
        follow_redirects=True,
    ) as klijent:
        sljedeci_krug = time.monotonic()

        while not stop.is_set():
            krug = datetime.now(timezone.utc).replace(microsecond=0)

            # Svi servisi provjeravaju se istovremeno: spor servis
            # ne zadržava provjere ostalih.
            rezultati = await asyncio.gather(
                *(provjeri(klijent, s, krug) for s in SERVISI)
            )

            for r in rezultati:
                ispisi(r)
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
                    # Ovaj rezultat se gubi, sljedeći krug ide normalno.
                    log.error("Objava nije uspjela: %s", e)

            # Fiksni ritam: krugovi počinju svakih INTERVAL_S sekundi,
            # bez obzira na to koliko je trajala sama provjera.
            sljedeci_krug += INTERVAL_S
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0, sljedeci_krug - time.monotonic()))
            except asyncio.TimeoutError:
                pass

    log.info("Zaustavljam se.")
    await veza.close()


def ispisi(r: dict):
    naziv = next((s["naziv"] for s in SERVISI if s["id"] == r["servis_id"]), r["servis_id"])
    if r["uspjeh"]:
        log.info("%-22s OK    %s  %5d ms", naziv, r["http_status"], r["odziv_ms"])
    else:
        log.info("%-22s PAO   %s  %5d ms", naziv, r["greska"], r["odziv_ms"])


if __name__ == "__main__":
    asyncio.run(main())
