"""
Algoritam raspodjele servisa po sondama.

Čista funkcija bez mreže i baze, pa se lako testira (vidi test_raspodjela.py).

Ciljevi, redom po važnosti:
  1. Svaki servis provjerava K sondi (3), ili sve sonde ako ih je manje od K.
  2. Sonde istog servisa su s RAZLIČITIH lokacija kad god je to moguće
     (inače kvorum ne bi razlikovao pad servisa od kvara jedne lokacije).
  3. Opterećenje je podjednako: najopterećenija i najmanje opterećena sonda
     razlikuju se za najviše jedan servis – koliko god to cilj 2 dopušta.
     (Ako je npr. u Rijeci samo jedna sonda, a u Puli pet, riječka sonda
     mora provjeravati više servisa da bi svaki servis imao obje lokacije.)
  4. Što manje promjena u odnosu na prethodni raspored (stabilnost):
     sonda zadržava svoje servise dok god može. Bez toga bi se pri svakoj
     sitnici sve ispremiješalo, a detektor ispada bi gubio povijest
     rezultata po sondi.
"""

K = 3


def izracunaj_raspored(
    servisi: list[str],
    sonde: dict[str, str],
    prethodno: dict[str, list[str]] | None = None,
    k: int = K,
) -> dict[str, list[str]]:
    """
    servisi   – ID-jevi aktivnih servisa
    sonde     – živuće sonde: sonda_id -> lokacija
    prethodno – dosadašnji raspored: servis_id -> [sonda_id, ...]
    vraća     – novi raspored: servis_id -> [sonda_id, ...] (sortirano)
    """
    prethodno = prethodno or {}
    if not sonde:
        return {s: [] for s in servisi}

    k = min(k, len(sonde))

    # Ako je lokacija manje od k, neke se lokacije u servisu moraju ponoviti.
    dopusteno_ponavljanja = k - min(k, len(set(sonde.values())))

    opterecenje = {p: 0 for p in sonde}
    novo: dict[str, list[str]] = {}

    # 1) Zadrži dosadašnje dodjele živih sondi. Najprije se zadržavaju sonde
    #    s različitih lokacija, a sonda s ponovljenom lokacijom samo kad nove
    #    lokacije ionako nema. (Opterećenje se ovdje ne gleda – njime se
    #    bavi korak 3, pa se postojeće dodjele ne miču bez potrebe.)
    for s in sorted(servisi):
        zadrzano: list[str] = []
        ponavljanja = 0
        stare = [p for p in sorted(prethodno.get(s, [])) if p in sonde]
        for samo_nove_lokacije in (True, False):
            for p in stare:
                if p in zadrzano or len(zadrzano) >= k:
                    continue
                nova_lokacija = sonde[p] not in {sonde[q] for q in zadrzano}
                if samo_nove_lokacije and not nova_lokacija:
                    continue
                if not nova_lokacija:
                    if ponavljanja >= dopusteno_ponavljanja:
                        continue
                    ponavljanja += 1
                zadrzano.append(p)
                opterecenje[p] += 1
        novo[s] = zadrzano

    # 2) Popuni servise kojima nedostaju sonde: prednost ima nova lokacija,
    #    zatim najmanje opterećena sonda, zatim abecedno (determinizam).
    for s in sorted(servisi):
        while len(novo[s]) < k:
            lokacije = {sonde[p] for p in novo[s]}
            kandidati = [p for p in sonde if p not in novo[s]]
            p = min(kandidati, key=lambda p: (sonde[p] in lokacije, opterecenje[p], p))
            novo[s].append(p)
            opterecenje[p] += 1

    # 3) Ujednači opterećenje: dok god je razlika veća od 1, premjesti jedan
    #    servis s preopterećene na manje opterećenu sondu. Ovo je korak u
    #    kojem nova sonda dobiva dio posla. Premještanje je dopušteno samo
    #    ako servis ne izgubi lokaciju (cilj 2 je važniji od cilja 3).
    def lokacije_bez(s, p):
        return [sonde[q] for q in novo[s] if q != p]

    # Petlja sigurno završava: svako premještanje s opterećenja a na b (a - b >= 2)
    # strogo smanjuje zbroj kvadrata opterećenja, a on ne može padati unedogled.
    while True:
        premjesteno = False
        # od najopterećenije prema manje opterećenima, a cilj je najmanje opterećena
        for maks in sorted(sonde, key=lambda p: (-opterecenje[p], p)):
            for mini in sorted(sonde, key=lambda p: (opterecenje[p], p)):
                if opterecenje[maks] - opterecenje[mini] <= 1:
                    break
                moguci = [
                    s for s in sorted(novo)
                    if maks in novo[s] and mini not in novo[s]
                    # broj različitih lokacija servisa ne smije pasti
                    and len(set(lokacije_bez(s, maks) + [sonde[mini]])) >= len({sonde[q] for q in novo[s]})
                ]
                if not moguci:
                    continue
                # Prednost: dodjele tek nastale u koraku 2 (stabilnost), zatim
                # servisi koji su do sada izgubili najmanje starih sondi, da
                # nijedan servis ne ostane s većinom sondi bez povijesti rezultata.
                izbor = min(moguci, key=lambda s: (
                    maks in prethodno.get(s, []),
                    len(set(novo[s]) - set(prethodno.get(s, []))),
                    s,
                ))
                novo[izbor].remove(maks)
                novo[izbor].append(mini)
                opterecenje[maks] -= 1
                opterecenje[mini] += 1
                premjesteno = True
                break
            if premjesteno:
                break
        if not premjesteno:
            break

    return {s: sorted(p) for s, p in novo.items()}


def po_sondama(raspored: dict[str, list[str]], sonde) -> dict[str, list[str]]:
    """Okreće raspored: sonda_id -> [servis_id, ...] (i za sonde bez ijednog servisa)."""
    rezultat = {p: [] for p in sonde}
    for s, lista in raspored.items():
        for p in lista:
            rezultat.setdefault(p, []).append(s)
    return {p: sorted(lista) for p, lista in rezultat.items()}
