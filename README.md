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
├── sonda/               # provjerava servise i objavljuje rezultate u RabbitMQ
└── prihvat/             # čita rezultate iz reda i sprema ih u PostgreSQL
```

Ostale komponente (raspoređivač, detektor ispada, API, sučelje) dodaju se korak po korak.

## Usluga konfiguracije

Jedini pisac tablice `servisi`. Sučelje za isprobavanje: http://localhost:8301/docs

| Zahtjev | Opis |
| --- | --- |
| `GET /servisi?aktivan=true` | popis servisa (ovo čitaju sonde) |
| `PUT /servisi/{id}` | dodaje ili zamjenjuje servis (idempotentno) |
| `PATCH /servisi/{id}` | djelomična izmjena, npr. `{"aktivan": false}` |
| `DELETE /servisi/{id}` | briše servis (idempotentno) |

Pravilo odgovornog praćenja: stvarni javni servisi (`testni: false`) imaju interval od najmanje 60 s, testni od najmanje 10 s. Pravilo provjeravaju i usluga i baza. Stvarni javni servisi (gov.hr, Studomat, Merlin, FIPU) upisani su kao isključeni dok ih operater ne uključi.

## Sonde

Tri sonde (`sonda-a` Pula, `sonda-b` Rijeka, `sonda-c` Zagreb) svakih 15 s dohvaćaju popis aktivnih servisa od usluge konfiguracije, provjeravaju svaki servis prema njegovom intervalu i rezultate objavljuju na fanout exchange `rezultati`. Odatle idu u red `rezultati.prihvat`, iz kojega će čitati usluga za prihvat.

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
   docker compose up --build
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
