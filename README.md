# Radi li?

Raspodijeljeno praćenje dostupnosti javnih servisa. Sonde na više lokacija provjeravaju servise, a ispad se proglašava tek kad ga potvrdi većina sondi (kvorum 2 od 3).

Projekt iz kolegija Raspodijeljeni sustavi, FIPU. Autor: Loris Lukić.

## Struktura

```
Radi_li/
├── docker-compose.yml   # sve komponente i infrastruktura
├── .env.example         # predložak postavki (kopirati u .env)
├── testni-servis/       # lažni javni servis za testiranje (može se ugasiti/usporiti)
├── konfiguracija/       # vlasnik popisa servisa (REST, PostgreSQL)
├── rasporedivac/        # prati otkucaje sondi i dodjeljuje servise sondama
├── sonda/               # provjerava servise i objavljuje rezultate u RabbitMQ
├── prihvat/             # čita rezultate iz reda i sprema ih u PostgreSQL
└── detektor/            # kvorum 2 od 3, otvara i zatvara incidente
```

Ostale komponente (API, sučelje) dodaju se korak po korak.

## Tko s kim razgovara

| Veza | Način |
| --- | --- |
| Sonde → raspoređivač | red `otkucaji` (otkucaj svakih 10 s) |
| Raspoređivač → sonde | direct exchange `raspored`, red `raspored.<sonda_id>` po sondi |
| Raspoređivač → konfiguracija | REST `GET /servisi?aktivan=true` |
| Sonde → prihvat | fanout exchange `rezultati`, red `rezultati.prihvat` |
| Sonde → detektor | isti fanout exchange `rezultati`, vlastiti red `rezultati.detektor` (kopija svakog rezultata) |
| Detektor → (buduće obavijesti) | fanout exchange `incidenti`, događaji `pocetak` i `kraj` |

## Detektor ispada

Odlučuje je li servis stvarno pao. Stanje: http://localhost:8501/stanje, incidenti: http://localhost:8501/incidenti, ručno zatvaranje: http://localhost:8501/docs

**Pravilo kvoruma** (`detektor/kvorum.py`):

- Svaka dodijeljena sonda glasa: `pao` ako su joj **dvije uzastopne** provjere neuspjele, `radi` ako su dvije uzastopne uspjele, inače `nesiguran`. Rezultati stariji od 3 intervala (+ istek) se ne broje (`nepoznato`).
- **Kvorum je većina** dodijeljenih sondi (2 od 3). Stanje je `ne_radi` ako kvorum glasa `pao`, `radi` ako kvorum glasa `radi`, inače `nepoznato`.
- Sonda koja glasa `pao` dok servis po kvorumu radi ima **lokalni problem** (npr. njezina mreža). Incident se ne otvara.

**Incidenti:** otvaraju se kad stanje postane `ne_radi`, a zatvaraju kad postane `radi`. Početak i kraj su vremena prve neuspjele odnosno uspjele provjere u nizu, a ne trenutak odluke. Jedinstveni indeks u bazi jamči najviše jedan otvoren incident po servisu.

- Detektor radi kao jedna instanca. Ako padne, rezultati čekaju u njegovom redu, a nakon ponovnog pokretanja zadnje rezultate i otvorene incidente učita iz baze.
- Jedini je pisac tablica `stanje_servisa` i `incidenti`.

Testovi pravila kvoruma:

```
docker compose run --rm --no-deps detektor python test_kvorum.py
```

### Simulacija lokalnog problema sonde

Testni servisi su u zasebnoj Docker mreži `internet`, na koju su spojene samo sonde. Odspajanjem jedne sonde s te mreže glumi se da njezina lokacija nema pristup internetu, a veza s RabbitMQ-om joj i dalje radi:

```
docker network disconnect radi_li_internet radi_li-sonda-b-1
docker network connect    radi_li_internet radi_li-sonda-b-1
```

Detektor tada javlja lokalni problem sonde-b, ali incident se ne otvara. (Točna imena mreže i kontejnera vide se naredbama `docker network ls` i `docker compose ps`.)

## Usluga konfiguracije

Jedini pisac tablice `servisi`. Sučelje za isprobavanje: http://localhost:8301/docs

| Zahtjev | Opis |
| --- | --- |
| `GET /servisi?aktivan=true` | popis servisa (ovo čita raspoređivač) |
| `PUT /servisi/{id}` | dodaje ili zamjenjuje servis (idempotentno) |
| `PATCH /servisi/{id}` | djelomična izmjena, npr. `{"aktivan": false}` |
| `DELETE /servisi/{id}` | briše servis (idempotentno) |

Pravilo odgovornog praćenja: stvarni javni servisi (`testni: false`) imaju interval od najmanje 60 s, testni od najmanje 10 s. Pravilo provjeravaju i usluga i baza. Stvarni javni servisi (gov.hr, Studomat, Merlin, FIPU) upisani su kao isključeni dok ih operater ne uključi.

## Raspoređivač

Odlučuje koja sonda provjerava koji servis. Stanje sondi: http://localhost:8401/sonde, raspored: http://localhost:8401/raspored

- Prati otkucaje sondi: bez otkucaja 15 s sonda **kasni**, nakon 30 s (3 propuštena otkucaja) je **ispala** i njezini servisi idu drugima.
- Svaki servis dodjeljuje trima sondama, i to s **različitih lokacija** kad god je moguće, uz podjednako opterećenje. Pri promjeni mijenja što manje dodjela (stabilnost).
- Svakoj sondi šalje **puni** popis njezinih servisa (idempotentna poruka), samo kad se promijeni i dodatno svakih 30 s.
- Raspored sprema u tablicu `dodjele` (za kvorum u detektoru ispada), a stanje sondi u tablicu `sonde`.
- Radi kao jedna instanca. Ako padne, sonde nastavljaju po posljednjem rasporedu; nakon ponovnog pokretanja čita stari raspored iz baze i pričeka 12 s da se sonde jave.

Algoritam je u `rasporedivac/raspodjela.py` i ima vlastite testove (uključujući stotine nasumičnih scenarija ispada i dolazaka sondi):

```
docker compose run --rm --no-deps rasporedivac python test_raspodjela.py
```

## Sonde

Tri sonde (`sonda-a` Pula, `sonda-b` Rijeka, `sonda-c` Zagreb) svakih 10 s šalju otkucaj raspoređivaču, od njega primaju popis servisa, provjeravaju svaki servis prema njegovom intervalu i rezultate objavljuju na fanout exchange `rezultati`. Odatle idu u red `rezultati.prihvat`, iz kojega čita usluga za prihvat.

Red `raspored.<sonda_id>` čuva samo najnoviji raspored (`x-max-length: 1`), pa ponovno pokrenuta sonda odmah dobije svoj posao. Ako raspoređivač ne radi, sonda nastavlja po posljednjem rasporedu.

Četvrta sonda (`sonda-d`, Osijek) ne pokreće se sama, nego služi za demonstraciju dolaska nove sonde: `docker compose --profile dodatne up -d sonda-d`.

Svaki rezultat je JSON poruka:

```json
{
  "provjera_id": "sonda-a:egradani:1763208000",
  "sonda_id": "sonda-a",
  "lokacija": "Pula",
  "servis_id": "egradani",
  "url": "http://testni-egradani:8000/",
  "vrijeme": "2026-11-15T12:00:00+00:00",
  "uspjeh": true,
  "http_status": 200,
  "odziv_ms": 12,
  "greska": null
}
```

`provjera_id` je jedinstven za sondu, servis i krug provjere, pa se po njemu prepoznaju duplikati.

## Usluga za prihvat

Čita red `rezultati.prihvat` i upisuje rezultate u tablicu `rezultati` (particioniranu po danu), u serijama do 100 rezultata ili svake sekunde.

- Poruka se potvrđuje (ack) tek nakon upisa u bazu, pa se pri padu usluge ništa ne gubi.
- Upis je idempotentan (`ON CONFLICT DO NOTHING` po `provjera_id`), pa ponovno isporučena poruka ne stvara duplikat.
- Nema vlastito stanje i skalira se horizontalno: `docker compose up -d --scale prihvat=3`.

| Zahtjev | Opis |
| --- | --- |
| `GET /zdravlje` | je li replika spojena na bazu i RabbitMQ |
| `GET /statistika` | koliko je poruka primila, upisala, prepoznala kao duplikat ili odbacila |

Docker svakoj replici sam dodjeljuje port na računalu. Portove vidiš naredbom `docker compose ps prihvat` (stupac PORTS, npr. `0.0.0.0:55012->8000/tcp` znači http://localhost:55012/statistika).

Pregled zadnjih rezultata u bazi:

```
docker compose exec baza psql -U radili -c "SELECT sonda_id, servis_id, vrijeme, uspjeh, greska FROM rezultati ORDER BY vrijeme DESC LIMIT 9;"
```

## Pokretanje

1. Kopiraj `.env.example` u `.env`.
2. Pokreni:
   ```
   docker compose up -d --build
   ```
3. Provjeri:
   - RabbitMQ sučelje: http://localhost:15672 (korisnik i lozinka iz `.env`)
   - Testni servisi: http://localhost:8101, http://localhost:8102, http://localhost:8103
   - Dokumentacija testnog servisa (gumbi za gašenje i usporavanje): http://localhost:8101/docs

Gašenje: `docker compose down`. Brisanje svih podataka (baza, redovi): `docker compose down -v`.

## Testni servis

| Zahtjev | Učinak |
| --- | --- |
| `GET /` | 200 ako radi, 503 ako je ugašen |
| `POST /upravljanje/ugasi` | servis počinje vraćati 503 |
| `POST /upravljanje/upali` | vraća servis u normalu |
| `POST /upravljanje/uspori?ms=15000` | odgovor kasni zadani broj milisekundi |
| `GET /upravljanje/stanje` | trenutno stanje |

Javni servisi (e-Građani, Studomat…) provjeravaju se najviše jednom u minuti. Svi testovi opterećenja rade se isključivo na testnim servisima.
