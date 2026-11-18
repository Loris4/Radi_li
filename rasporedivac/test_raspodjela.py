"""
Testovi algoritma raspodjele. Pokretanje (unutar mape rasporedivac):
    python -m pytest test_raspodjela.py -v
ili bez pytesta:
    python test_raspodjela.py
"""

from raspodjela import izracunaj_raspored, po_sondama

TRI = {"sonda-a": "Pula", "sonda-b": "Rijeka", "sonda-c": "Zagreb"}
SERVISI = ["egradani", "merlin", "studomat"]


def opterecenja(raspored, sonde):
    return {p: len(s) for p, s in po_sondama(raspored, sonde).items()}


def provjeri_osnovno(raspored, servisi, sonde, k=3):
    k = min(k, len(sonde))
    for s in servisi:
        assert len(raspored[s]) == k, f"{s} ima {len(raspored[s])} sondi umjesto {k}"
        assert len(set(raspored[s])) == k, f"{s} ima istu sondu dvaput"
    o = opterecenja(raspored, sonde).values()
    assert max(o) - min(o) <= 1, f"neujednačeno opterećenje: {o}"


def test_tri_sonde_tri_servisa():
    r = izracunaj_raspored(SERVISI, TRI)
    assert all(r[s] == ["sonda-a", "sonda-b", "sonda-c"] for s in SERVISI)


def test_ispadne_jedna_sonda():
    prije = izracunaj_raspored(SERVISI, TRI)
    dvije = {"sonda-a": "Pula", "sonda-c": "Zagreb"}
    r = izracunaj_raspored(SERVISI, dvije, prije)
    provjeri_osnovno(r, SERVISI, dvije)
    assert all("sonda-b" not in r[s] for s in SERVISI)


def test_nema_sondi():
    assert izracunaj_raspored(SERVISI, {}) == {s: [] for s in SERVISI}


def test_nema_servisa():
    assert izracunaj_raspored([], TRI) == {}


def test_nova_sonda_dobiva_posao():
    prije = izracunaj_raspored(SERVISI, TRI)
    cetiri = {**TRI, "sonda-d": "Osijek"}
    r = izracunaj_raspored(SERVISI, cetiri, prije)
    provjeri_osnovno(r, SERVISI, cetiri)
    assert opterecenja(r, cetiri)["sonda-d"] >= 2


def test_stabilnost():
    """Isti ulaz dvaput zaredom ne smije ništa promijeniti."""
    cetiri = {**TRI, "sonda-d": "Osijek"}
    servisi = [f"s{i}" for i in range(10)]
    r1 = izracunaj_raspored(servisi, cetiri)
    r2 = izracunaj_raspored(servisi, cetiri, r1)
    r3 = izracunaj_raspored(servisi, cetiri, r2)
    assert r1 == r2 == r3


def test_malo_promjena_pri_ispadu():
    """
    Kad jedna od 6 sondi ispadne, većina dodjela ostaje ista.
    Izgube se dodjele te sonde (15 od 90) i nekoliko se premjesti radi
    ujednačavanja opterećenja, ali nikad ne cijeli raspored.
    """
    sonde = {f"p{i}": f"L{i}" for i in range(6)}
    servisi = [f"s{i}" for i in range(30)]
    prije = izracunaj_raspored(servisi, sonde)
    bez_p2 = {p: l for p, l in sonde.items() if p != "p2"}
    poslije = izracunaj_raspored(servisi, bez_p2, prije)
    provjeri_osnovno(poslije, servisi, bez_p2)

    zadrzano = sum(len(set(prije[s]) & set(poslije[s])) for s in servisi)
    assert zadrzano >= 0.75 * 90, f"zadržano samo {zadrzano} od 90"
    # sonde koje nisu ispale ne gube posao bez razloga
    for s in servisi:
        if "p2" not in prije[s]:
            assert len(set(prije[s]) & set(poslije[s])) >= 2


def test_stabilnost_uz_ponovljene_lokacije():
    """Dvije lokacije, četiri sonde: raspored se ne smije mijenjati sam od sebe."""
    sonde = {"p0": "Pula", "p1": "Pula", "p2": "Pula", "p3": "Rijeka"}
    servisi = [f"s{i}" for i in range(8)]
    r1 = izracunaj_raspored(servisi, sonde)
    r2 = izracunaj_raspored(servisi, sonde, r1)
    assert r1 == r2


def test_nasumicni_ispadi_i_dolasci():
    """Stotine nasumičnih scenarija: pravila uvijek vrijede, a raspored je stabilan."""
    import random
    rnd = random.Random(1)
    for _ in range(300):
        sonde = {f"p{i}": f"L{rnd.randint(0, 6)}" for i in range(rnd.randint(1, 9))}
        servisi = [f"s{i}" for i in range(rnd.randint(0, 40))]
        r = izracunaj_raspored(servisi, sonde)
        for _ in range(6):
            sonde = dict(sonde)
            if sonde and rnd.random() < 0.5:
                sonde.pop(rnd.choice(sorted(sonde)))
            else:
                sonde[f"n{rnd.randint(0, 99)}"] = f"L{rnd.randint(0, 6)}"
            r = izracunaj_raspored(servisi, sonde, r)
            if sonde and servisi:
                k = min(3, len(sonde))
                broj_lokacija = len(set(sonde.values()))
                for s in servisi:
                    assert len(set(r[s])) == k
                    # cilj 2: uvijek najviše moguće različitih lokacija
                    assert len({sonde[p] for p in r[s]}) == min(k, broj_lokacija)
                # cilj 3 mora vrijediti kad su lokacije ravnomjerne (svaka sonda na svojoj)
                if broj_lokacija == len(sonde):
                    provjeri_osnovno(r, servisi, sonde)
            assert izracunaj_raspored(servisi, sonde, r) == r


def test_razlicite_lokacije():
    """
    Dvije sonde u Puli, po jedna u Rijeci i Zagrebu. Svaki servis mora imati
    sve tri lokacije, pa riječka i zagrebačka sonda nose sve servise, a pulske
    dijele posao među sobom (različite lokacije važnije su od ravnoteže).
    """
    sonde = {"a1": "Pula", "a2": "Pula", "b1": "Rijeka", "c1": "Zagreb"}
    r = izracunaj_raspored(SERVISI, sonde)
    for s in SERVISI:
        lokacije = [sonde[p] for p in r[s]]
        assert len(set(lokacije)) == 3, f"{s}: {lokacije}"
    o = opterecenja(r, sonde)
    assert o["b1"] == o["c1"] == 3
    assert abs(o["a1"] - o["a2"]) <= 1


def test_manje_lokacija_nego_k():
    """Ako postoje samo dvije lokacije, treća sonda smije biti s ponovljene lokacije."""
    sonde = {"a1": "Pula", "a2": "Pula", "b1": "Rijeka"}
    r = izracunaj_raspored(SERVISI, sonde)
    provjeri_osnovno(r, SERVISI, sonde)


def test_velik_sustav():
    sonde = {f"p{i}": f"L{i % 5}" for i in range(12)}
    servisi = [f"s{i}" for i in range(500)]
    r = izracunaj_raspored(servisi, sonde)
    provjeri_osnovno(r, servisi, sonde)
    for s in servisi:
        assert len({sonde[p] for p in r[s]}) == 3


if __name__ == "__main__":
    for ime, f in list(globals().items()):
        if ime.startswith("test_"):
            f()
            print("OK ", ime)
