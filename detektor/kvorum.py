"""
Pravilo kvoruma – je li servis pao, po glasovima dodijeljenih sondi.

Čista funkcija bez mreže i baze, pa se lako testira (vidi test_kvorum.py).

Glas jedne sonde (gleda se samo njezinih UZASTOPNO zadnjih svježih rezultata):
  "pao"       – sve su neuspjele
  "radi"      – sve su uspjele
  "nesiguran" – miješano (servis "titra" ili se upravo mijenja)
  "nepoznato" – nema dovoljno svježih rezultata (sonda tek počela, kasni ili je ispala)

Stanje servisa (kvorum = većina dodijeljenih sondi, za 3 sonde to je 2):
  "ne_radi"   – barem kvorum sondi glasa "pao"
  "radi"      – barem kvorum sondi glasa "radi"
  "nepoznato" – nijedna strana nema kvorum

Sonda koja glasa "pao" dok servis po kvorumu radi ima LOKALNI PROBLEM
(npr. njezina mreža), a ne servis. Zbog toga nema lažne uzbune.

Traženje dvije uzastopne provjere (a ne jedne) sprječava da jedan
izgubljeni paket ili kratka smetnja otvori incident.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

UZASTOPNO = 2


@dataclass
class Rezultat:
    vrijeme: datetime
    uspjeh: bool


@dataclass
class Procjena:
    stanje: str                                    # radi | ne_radi | nepoznato
    glasovi: dict[str, str]                        # sonda_id -> pao | radi | nesiguran | nepoznato
    kvorum: int                                    # koliko istih glasova treba
    od: datetime | None = None                     # kad je stanje (po kvorumu) nastupilo
    lokalni_problemi: list[str] = field(default_factory=list)


def glas_sonde(rezultati: list[Rezultat], sada: datetime, max_starost: timedelta,
               uzastopno: int = UZASTOPNO) -> tuple[str, datetime | None]:
    """Vraća (glas, vrijeme prvog rezultata u nizu koji je odlučio glas)."""
    svjezi = sorted((r for r in rezultati if sada - r.vrijeme <= max_starost), key=lambda r: r.vrijeme)
    zadnji = svjezi[-uzastopno:]
    if len(zadnji) < uzastopno:
        return "nepoznato", None
    if all(not r.uspjeh for r in zadnji):
        return "pao", zadnji[0].vrijeme
    if all(r.uspjeh for r in zadnji):
        return "radi", zadnji[0].vrijeme
    return "nesiguran", None


def procijeni(
    rezultati_po_sondi: dict[str, list[Rezultat]],
    dodijeljene: list[str],
    sada: datetime,
    max_starost: timedelta,
    uzastopno: int = UZASTOPNO,
) -> Procjena:
    """
    rezultati_po_sondi – zadnji rezultati za OVAJ servis, po sondi
    dodijeljene        – sonde kojima je raspoređivač dodijelio servis
    max_starost        – rezultat stariji od ovoga se ne broji
    """
    if not dodijeljene:
        return Procjena("nepoznato", {}, kvorum=0)

    kvorum = len(dodijeljene) // 2 + 1
    glasovi: dict[str, str] = {}
    pocetci: dict[str, datetime | None] = {}
    for p in sorted(dodijeljene):
        glasovi[p], pocetci[p] = glas_sonde(rezultati_po_sondi.get(p, []), sada, max_starost, uzastopno)

    pali = [p for p, g in glasovi.items() if g == "pao"]
    rade = [p for p, g in glasovi.items() if g == "radi"]

    if len(pali) >= kvorum:
        # Ispad je počeo kad je kvorum-ta sonda vidjela prvi neuspjeh u svom nizu
        od = sorted(pocetci[p] for p in pali)[kvorum - 1]
        return Procjena("ne_radi", glasovi, kvorum, od=od)
    if len(rade) >= kvorum:
        od = sorted(pocetci[p] for p in rade)[kvorum - 1]
        return Procjena("radi", glasovi, kvorum, od=od, lokalni_problemi=pali)
    return Procjena("nepoznato", glasovi, kvorum, lokalni_problemi=[])
