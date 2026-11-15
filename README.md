# Radi li?

Raspodijeljeno praćenje dostupnosti javnih servisa. Sonde na više lokacija provjeravaju servise, a ispad se proglašava tek kad ga potvrdi većina sondi (kvorum 2 od 3).

Projekt iz kolegija Raspodijeljeni sustavi, FIPU. Autor: Loris Lukić.

## Struktura

```
Radi_li/
├── docker-compose.yml   # sve komponente i infrastruktura
├── .env.example         # predložak postavki (kopirati u .env)
└── testni-servis/       # lažni javni servis za testiranje (može se ugasiti/usporiti)
```

Ostale komponente (sonde, usluga za prihvat, raspoređivač, detektor ispada, API, sučelje) dodaju se korak po korak.

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
