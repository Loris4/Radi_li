"""
Testovi pravila kvoruma. Pokretanje:
    docker compose run --rm --no-deps detektor python test_kvorum.py
"""

from datetime import datetime, timedelta, timezone

from kvorum import Rezultat, procijeni

T0 = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)
INTERVAL = timedelta(seconds=10)
MAX = timedelta(seconds=35)
TRI = ["sonda-a", "sonda-b", "sonda-c"]


def niz(*uspjesi, kraj=T0):
    """Rezultati u razmacima od 10 s, zadnji u trenutku `kraj`."""
    n = len(uspjesi)
    return [Rezultat(kraj - (n - 1 - i) * INTERVAL, u) for i, u in enumerate(uspjesi)]


def P(a, b, c, sada=T0):
    return procijeni({"sonda-a": a, "sonda-b": b, "sonda-c": c}, TRI, sada, MAX)


def test_sve_radi():
    p = P(niz(1, 1), niz(1, 1), niz(1, 1))
    assert p.stanje == "radi" and p.lokalni_problemi == []


def test_sve_sonde_vide_pad():
    p = P(niz(1, 0, 0), niz(1, 0, 0), niz(1, 0, 0))
    assert p.stanje == "ne_radi"
    assert p.od == T0 - INTERVAL          # prvi neuspjeh u nizu


def test_dvije_od_tri_dovoljno():
    assert P(niz(0, 0), niz(0, 0), niz(1, 1)).stanje == "ne_radi"


def test_jedna_sonda_lokalni_problem():
    """Samo sonda-b ne dohvaća servis: nema incidenta, a sonda-b ima lokalni problem."""
    p = P(niz(1, 1), niz(0, 0), niz(1, 1))
    assert p.stanje == "radi"
    assert p.lokalni_problemi == ["sonda-b"]


def test_jedan_neuspjeh_nije_dovoljan():
    """Jedna neuspjela provjera (npr. izgubljen paket) ne smije otvoriti incident."""
    p = P(niz(1, 0), niz(1, 0), niz(1, 0))
    assert p.stanje == "nepoznato"
    assert set(p.glasovi.values()) == {"nesiguran"}


def test_stari_rezultati_se_ne_broje():
    """Sonda koja je prestala slati rezultate ne glasa."""
    staro = niz(0, 0, kraj=T0 - timedelta(minutes=5))
    p = P(staro, niz(1, 1), niz(1, 1))
    assert p.glasovi["sonda-a"] == "nepoznato"
    assert p.stanje == "radi"


def test_premalo_svjezih_glasova():
    staro = niz(1, 1, kraj=T0 - timedelta(minutes=5))
    p = P(staro, staro, niz(0, 0))
    assert p.stanje == "nepoznato"


def test_oporavak():
    p = P(niz(0, 0, 1, 1), niz(0, 0, 1, 1), niz(0, 0, 0, 0))
    assert p.stanje == "radi"
    assert p.od == T0 - INTERVAL           # prvi uspjeh u nizu
    assert p.lokalni_problemi == ["sonda-c"]


def test_dvije_sonde_moraju_se_sloziti():
    """Ako su servisu dodijeljene samo 2 sonde, kvorum je 2."""
    p = procijeni({"a": niz(0, 0), "b": niz(1, 1)}, ["a", "b"], T0, MAX)
    assert p.kvorum == 2 and p.stanje == "nepoznato"
    p = procijeni({"a": niz(0, 0), "b": niz(0, 0)}, ["a", "b"], T0, MAX)
    assert p.stanje == "ne_radi"


def test_jedna_sonda():
    p = procijeni({"a": niz(0, 0)}, ["a"], T0, MAX)
    assert p.kvorum == 1 and p.stanje == "ne_radi"


def test_nema_dodijeljenih_sondi():
    assert procijeni({}, [], T0, MAX).stanje == "nepoznato"


def test_nedodijeljena_sonda_ne_glasa():
    """Rezultat sonde kojoj servis više nije dodijeljen ne utječe na odluku."""
    p = procijeni({"a": niz(0, 0), "b": niz(0, 0), "x": niz(0, 0)}, ["a", "b", "c"], T0, MAX)
    assert "x" not in p.glasovi
    assert p.stanje == "ne_radi"
    p = procijeni({"x": niz(0, 0), "y": niz(0, 0), "a": niz(1, 1), "b": niz(1, 1)}, ["a", "b", "c"], T0, MAX)
    assert p.stanje == "radi"


def test_redoslijed_rezultata_nije_bitan():
    """Rezultati mogu stići izvan redoslijeda; sortiraju se po vremenu."""
    a = list(reversed(niz(1, 0, 0)))
    assert P(a, niz(0, 0), niz(1, 1)).stanje == "ne_radi"


if __name__ == "__main__":
    for ime, f in list(globals().items()):
        if ime.startswith("test_"):
            f()
            print("OK ", ime)
