# Radi li?

Raspodijeljeno praćenje dostupnosti javnih servisa. Sonde na više lokacija provjeravaju servise, a ispad se proglašava tek kad ga potvrdi većina sondi (kvorum 2 od 3).

Projekt iz kolegija Raspodijeljeni sustavi, FIPU. Autor: Loris Lukić.

## Struktura

```
Radi_li/
├── docker-compose.yml   # sve komponente i infrastruktura
├── .env.example         # predložak postavki (kopirati u .env)
├── testni-servis/       # lažni javni servis za testiranje (može se ugasiti/usporiti)
└── sonda/               # provjerava servise i objavljuje rezultate u RabbitMQ
```

Ostale komponente (usluga za prihvat, raspoređivač, detektor ispada, API, sučelje) dodaju se korak po korak.

## Sonde

Tri sonde (`sonda-a` Pula, `sonda-b` Rijeka, `sonda-c` Zagreb) svakih 10 s provjeravaju testne servise i rezultate objavljuju na fanout exchange `rezultati`. Odatle idu u red `rezultati.prihvat`, iz kojega će čitati usluga za prihvat.

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
