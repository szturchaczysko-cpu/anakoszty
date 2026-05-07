"""DASHBOARD RENTOWNOŚCI DSG — Streamlit Cloud (all-in-one + Vertex AI query)"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from decimal import Decimal
import json
import re
import random
import firebase_admin
from firebase_admin import credentials, firestore

st.set_page_config(page_title="Rentowność DSG", layout="wide", page_icon="📊", initial_sidebar_state="expanded")

@st.cache_resource
def init_firebase():
    if not firebase_admin._apps:
        creds_dict = json.loads(st.secrets["FIREBASE_CREDS"])
        cred = credentials.Certificate(creds_dict)
        firebase_admin.initialize_app(cred)
    return firestore.client()

db = init_firebase()

if "auth_ok" not in st.session_state:
    st.session_state.auth_ok = False

def check_auth():
    if st.session_state.auth_ok:
        return True
    st.header("🔒 Dashboard Rentowności DSG")
    pwd = st.text_input("Hasło:", type="password")
    if st.button("Zaloguj", type="primary"):
        if pwd == st.secrets["ADMIN_PASSWORD"]:
            st.session_state.auth_ok = True
            st.rerun()
        else:
            st.error("Błędne hasło.")
    return False

if not check_auth():
    st.stop()


# ==========================================
# DOMYŚLNY SŁOWNIK POJĘĆ DOMENOWYCH
# (użytkownik może go edytować w sidebarze)
# ==========================================
DOMYSLNY_SLOWNIK = """
KONTEKST BIZNESOWY:
Firma Autos regeneruje używane skrzynie biegów DSG. Każda skrzynia ma swój numer partii (proNrPartii).
Twórca (pracownik) wykonuje zadania produkcyjne na skrzyni, pobiera części z magazynu, skrzynia jest sprzedawana przez eBay/Allegro/shopEU/shopPL.

MAPOWANIE POJĘĆ NA TABELE/KOLUMNY (dla PostgreSQL, schema 'public' i 'rapdb'):

### SKRZYNIA / PARTIA PRODUKCYJNA:
- "skrzynia", "numer partii", "nr partii" → kolumna "proNrPartii" w widoku "rapdb"."VwSkrzynieStareNoweKolekiNoweTworcyVer1" (unikalny identyfikator konkretnej wytworzonej skrzyni, np. "29PT", "29QH", "AAA1000")
- "index skrzyni", "indeks skrzyni", "indeks artykułu" → kolumna "artIndeks" (indeks magazynowy modelu/wariantu skrzyni, przykłady: "105FSI5GRUP2", "207DQ500GRUP5_BM", "156TL4GRUP12_USZK")
- "typ skrzyni", "grupa skrzyń", "model skrzyni" → kolumna "grupaIgo" (grupa igorowa)
- "nazwa skrzyni" → kolumna "artNazwa" (np. "Skrzynia biegów 1.4")
- UWAGA: "zadprElementNrPartii" w "wmsZleceniaZadaniaProdukcyjne" to kod znakowania nabijany na skrzynię przez twórcę (typu "1XU7") — nie używaj tego jako index skrzyni!

### TWÓRCA (PRACOWNIK) — KLUCZOWE:
- "twórca", "twórca skrzyniowy", "pracownik produkcji", "montażysta" → kolumna **"tworca_maggo"** w widoku **"rapdb"."VwSkrzynieStareNoweKolekiNoweTworcyVer1"** (login pracownika, np. "adrian_l", "tomasz_m", "kamil_l", "andrzej_o")
- ⚠️ NIE MYL: "pakUserTw" w "wmsPakunkiHist" to KTO OTWORZYŁ PAKUNEK w systemie (to często magazynier "lisu" lub sam twórca gdy zamawia — ALE NIE JEST TO TWÓRCA SKRZYNI!)
- ⚠️ NIE MYL: "zadprUserTw" w "wmsZleceniaZadaniaProdukcyjne" to kto założył zlecenie produkcyjne w systemie (zwykle szef produkcji rafal, czarek, SYS) — a nie prawdziwy twórca!
- Role w systemie: "lisu"/"damian_z"/"dawid_p" = magazynierzy (szykują i wydają części), "adrian_l"/"tomasz_m"/"kamil_l"/itd. = prawdziwi twórcy skrzyń

### WIDOK VwSkrzynieStareNoweKolekiNoweTworcyVer1 (KLUCZOWY — ŹRÓDŁO PRAWDY):
- Źródło prawdy o twórcach skrzyń i numerach partii
- Kolumna "typ_rap" ma wartości:
  - "skrzyniaStara" = pobrana do regeneracji (austausch — używana), może nie mieć twórcy
  - "skrzyniaNowa" = WYTWORZONA (zregenerowana) — TUTAJ JEST PRAWDZIWY TWÓRCA w "tworca_maggo"
  - "kolekNowy" = koleka nowa (kolektor)
- Kolumny: typ_rap, proDataTw (kiedy wytworzono), proNrPartii, prokwident, artIndeks, tartNazwa, tworca_maggo

### CZĘŚCI POBRANE DO REGENERACJI SKRZYNI:
- "pobrane części", "części pobrane z magazynu", "wydane części", "części użyte do skrzyni" → tabele "public"."wmsPakunkiHist" i "public"."wmsPakunkiPozycjeHist"
- wmsPakunkiHist = pakunek (jednostka pobrania)
- wmsPakunkiPozycjeHist = konkretne pozycje (artykuły) w pakunku, łączymy przez "pakID" (UWAGA: ID wielkimi literami!)

⚠️ KLUCZOWE POWIĄZANIA (ZWERYFIKOWANE EMPIRYCZNIE):

1. **PAKUNEK → SKRZYNIA**: JOIN "wmsPakunkiHist"."zadprNrPartii" = "VwSkrzynieStareNoweKolekiNoweTworcyVer1"."proNrPartii"
   (To NUMER PARTII identyczny w obu miejscach)
   
2. **TYP PAKUNKU — BARDZO WAŻNE**: W "wmsPakunkiHist" kolumna "pakRodzaj" rozróżnia:
   - **pakRodzaj = 1** — pakunek WYSYŁKOWY do klienta (olej, sprzęgło, akcesoria wysyłane z gotową skrzynią) — POMIJAJ jeśli pytasz o regenerację!
   - **pakRodzaj = 3** — POBRANIE CZĘŚCI DO REGENERACJI — to jest to czego szukasz przy pytaniach "co pobrano do skrzyni"

3. **NAZWA CZĘŚCI**: JOIN do "wmsArtykuly" przez "artID", kolumna "artNazwa"

4. **ILOŚĆ**: "pakpIlosc" w "wmsPakunkiPozycjeHist"

5. **DATA POBRANIA**: "pakDataWyd" (timestamp) — używaj ::date

⚠️ KONWENCJA NAZEWNICZA W TEJ BAZIE (bardzo ważne!):
- Identyfikatory kończące się na "ID" są PISANE WIELKIMI LITERAMI: "pakID", "artID", "zlecID", "zlecprID"
- NIE myl z "pakId" / "artId" - takie kolumny NIE istnieją!
- Inne kolumny zachowują camelCase: "pakDataWyd", "pakUserTw", "proNrPartii", "zadprNrPartii"
- Zawsze używaj cudzysłowów dla WSZYSTKICH takich nazw
- W "wmsPakunkiHist" JEST kolumna "zlecID" ale w "wmsZleceniaProdukcyjne*" NIE MA — jest "zlecprID"

### 📋 SZABLON WZORCOWY — raport "kto zrobił jaką skrzynię i co do niej pobrano":

WITH wytworzone_skrzynie AS (
    SELECT
        sk."tworca_maggo" AS "Twórca",
        sk."artIndeks" AS "Index skrzyni",
        sk."tartNazwa" AS "Nazwa skrzyni",
        sk."proNrPartii" AS "Nr partii"
    FROM "rapdb"."VwSkrzynieStareNoweKolekiNoweTworcyVer1" AS sk
    WHERE sk."typ_rap" = 'skrzyniaNowa'
      AND sk."proDataTw"::date = '2026-04-23'
      AND sk."tworca_maggo" IS NOT NULL
),
pobrania_do_partii AS (
    SELECT
        pak."zadprNrPartii" AS "Nr partii",
        STRING_AGG(
            DISTINCT art."artIndeks" || ' — ' || art."artNazwa" || ' (' || poz."pakpIlosc"::text || ' szt)',
            ', '
            ORDER BY art."artIndeks" || ' — ' || art."artNazwa" || ' (' || poz."pakpIlosc"::text || ' szt)'
        ) AS "Lista części"
    FROM "public"."wmsPakunkiHist" AS pak
    JOIN "public"."wmsPakunkiPozycjeHist" AS poz ON poz."pakID" = pak."pakID"
    JOIN "public"."wmsArtykuly" AS art ON art."artID" = poz."artID"
    WHERE pak."zadprNrPartii" IS NOT NULL
      AND pak."pakRodzaj" = 3
    GROUP BY pak."zadprNrPartii"
)
SELECT ws."Twórca", ws."Index skrzyni", ws."Nr partii", ws."Nazwa skrzyni",
       COALESCE(pd."Lista części", '(brak zarejestrowanych pobrań)') AS "Lista pobranych części"
FROM wytworzone_skrzynie AS ws
LEFT JOIN pobrania_do_partii AS pd ON pd."Nr partii" = ws."Nr partii"
ORDER BY ws."Twórca", ws."Index skrzyni"
LIMIT 1000

NIE MYLIĆ z tabelami "wmsKompletacje" (to kompletacja wysyłek do klienta, nie produkcja!)

### 💰 CENY I KOSZTY CZĘŚCI (dla rentowności produkcji):

**Skąd brać cenę zakupu części:**
- **`wmsArtykulySrCenyZakHist`** — historyczna średnia cena zakupu (WYLICZONA AUTOMATYCZNIE przez SYS) ⭐ GŁÓWNE ŹRÓDŁO
  - `asczhSrCenaZakPo` = aktualna średnia cena zakupu (netto)
  - `asczhSrCenaZakPoWalID` = waluta (26 = PLN)
  - `aschDataTw` = data zapisu (weź najnowszą)
  - `artID` = łączymy z `wmsArtykuly`
- **`wmsArtykulyKontrahenci`** — ceny od poszczególnych dostawców (TIMKEN, FERSA, SNR, NTN, FBJ itd.)
  - `artkCena` = cena netto od tego dostawcy
  - `artkCenaBrutto` = cena z VATem
  - `artkWiodacy` = True jeśli główny dostawca
  - `konID` = ID kontrahenta
- **`wmsArtykulyCeny`** — tabela na sprzedażową (zwykle pusta, nie używaj dla kosztu zakupu)

**Stany magazynowe:**
- **`wmsMagazynMiejsca`** — gdzie fizycznie leży i ile jest
  - `magmIlosc` = ile sztuk w tej lokalizacji
  - `magmIloscRezerw` = ile zarezerwowane
  - `magmRegal`, `magmPoziom`, `magmMiejsce` = lokalizacja fizyczna (R/P/M)
  - `artID` = łączymy z `wmsArtykuly`
- ⚠️ **ZAWSZE przy pytaniach o "stany magazynowe" DOŁĄCZ WARTOŚĆ** — czyli JOIN do `wmsArtykulySrCenyZakHist` i dodaj kolumny "Cena jedn." oraz "Wartość" (ilość × cena). Użytkownik pytając o stany prawie zawsze chce też wiedzieć ile to warte!

**UWAGA:** W tabeli `wmsPakunkiPozycjeHist` NIE MA kolumny z ceną — nie jest zapisywana w momencie pobrania. Koszt pobranej części trzeba wyliczyć jako `ilość × aktualna_średnia_cena_zakupu`.

### 📋 SZABLON RENTOWNOŚCI — koszt części per wytworzoną skrzynię:

WITH najnowsza_cena AS (
    -- średnia cena zakupu per artykuł (najświeższy zapis)
    SELECT DISTINCT ON (sc."artID")
        sc."artID",
        sc."asczhSrCenaZakPo" AS "cenaSr",
        sc."asczhSrCenaZakPoWalID" AS "walID"
    FROM "public"."wmsArtykulySrCenyZakHist" AS sc
    ORDER BY sc."artID", sc."aschDataTw" DESC NULLS LAST
),
wytworzone_skrzynie AS (
    SELECT
        sk."tworca_maggo" AS "Twórca",
        sk."artIndeks" AS "Index skrzyni",
        sk."tartNazwa" AS "Nazwa skrzyni",
        sk."proNrPartii" AS "Nr partii"
    FROM "rapdb"."VwSkrzynieStareNoweKolekiNoweTworcyVer1" AS sk
    WHERE sk."typ_rap" = 'skrzyniaNowa'
      AND sk."proDataTw"::date = '2026-04-23'
      AND sk."tworca_maggo" IS NOT NULL
),
pobrania_z_cenami AS (
    SELECT
        pak."zadprNrPartii" AS "Nr partii",
        SUM(poz."pakpIlosc" * COALESCE(cen."cenaSr", 0)) AS "Koszt łączny",
        COUNT(DISTINCT art."artID") AS "Liczba różnych części"
    FROM "public"."wmsPakunkiHist" AS pak
    JOIN "public"."wmsPakunkiPozycjeHist" AS poz ON poz."pakID" = pak."pakID"
    JOIN "public"."wmsArtykuly" AS art ON art."artID" = poz."artID"
    LEFT JOIN najnowsza_cena AS cen ON cen."artID" = art."artID"
    WHERE pak."zadprNrPartii" IS NOT NULL
      AND pak."pakRodzaj" = 3
    GROUP BY pak."zadprNrPartii"
)
### 📋 SZABLON WYCENY MAGAZYNU — "jakie mamy stany i ile to warte":

WITH najnowsza_cena AS (
    SELECT DISTINCT ON (sc."artID")
        sc."artID",
        sc."asczhSrCenaZakPo" AS "cenaSr"
    FROM "public"."wmsArtykulySrCenyZakHist" AS sc
    ORDER BY sc."artID", sc."aschDataTw" DESC NULLS LAST
)
SELECT
    art."artIndeks" AS "Indeks",
    art."artNazwa" AS "Nazwa artykułu",
    SUM(mm."magmIlosc") AS "Ilość na stanie",
    SUM(mm."magmIloscRezerw") AS "Ilość zarezerwowana",
    SUM(mm."magmIlosc" - mm."magmIloscRezerw") AS "Ilość dostępna",
    MAX(cen."cenaSr")::numeric(10,2) AS "Cena jedn. (zł)",
    (SUM(mm."magmIlosc") * MAX(cen."cenaSr"))::numeric(12,2) AS "Wartość całkowita (zł)",
    (SUM(mm."magmIlosc" - mm."magmIloscRezerw") * MAX(cen."cenaSr"))::numeric(12,2) AS "Wartość dostępna (zł)"
FROM "public"."wmsArtykuly" AS art
JOIN "public"."wmsMagazynMiejsca" AS mm ON mm."artID" = art."artID"
LEFT JOIN najnowsza_cena AS cen ON cen."artID" = art."artID"
WHERE art."artAktywny" = true
  AND mm."magmAktywne" = true
  AND mm."magmIlosc" > 0
GROUP BY art."artID", art."artIndeks", art."artNazwa"
ORDER BY "Wartość całkowita (zł)" DESC NULLS LAST
LIMIT 1000

⚠️ KONWENCJA NAZEWNICZA W TEJ BAZIE (bardzo ważne!):
- Identyfikatory kończące się na "ID" są PISANE WIELKIMI LITERAMI: "pakID", "artID", "zlecID", "poziD"
- NIE myl z "pakId" / "artId" - takie kolumny NIE istnieją, mała 'd' to błąd!
- Inne kolumny zachowują camelCase: "pakDataWyd", "pakUserCreateLogin", "proNrPartii"
- Zawsze używaj cudzysłowów dla WSZYSTKICH takich nazw

### ARTYKUŁY (CZĘŚCI):
- "nazwa części", "artykuł" → kolumna "artNazwa" w "public"."wmsArtykuly"
- "indeks części", "indeks magazynowy" → "artIndeks" lub "IndeksMag"
- Powiązanie pakunków z nazwami artykułów: JOIN przez "artId"

### ZLECENIA PRODUKCYJNE:
- "zlecenie produkcyjne", "zlecenie regeneracji" → tabele "public"."wmsZleceniaProdukcyjne" i "wmsZleceniaProdukcyjneHist"
- identyfikator zlecenia = "zlecId" (w tabelach pakunków jako "pakZlecId")
- numer zlecenia = "zlecprNumer"

### CZYNNOŚCI PRODUKCYJNE (NIE to samo co części!):
- "czynność produkcyjna", "zadanie" → tabele "wmsZleceniaZadaniaProdukcyjne" oraz widoki "rapdb"."VwZlecZadProdAktualHistCzynnosci*"
- UWAGA: to są OPERACJE na skrzyni (np. "Gwintowanie skrzyni", "Regeneracja", "Kontrola") — nie mylić z pobranymi częściami!

### DATY:
- "data produkcji", "kiedy wyprodukowano" → "DataProduktu", "pakDataWyd", "dataTwZlecProd"
- "dzisiaj" → użyj CURRENT_DATE lub DATE '2026-04-23'

### PUNKTY / WYNAGRODZENIE:
- "punkty twórcy", "zarobki twórcy" → widoki "rapdb"."VwNowyRaportMaggoPktTworc*" (kolumny "PunktyProd", "Maggo_razem")

### REKLAMACJE:
- "reklamacje" → kolumna "TypRap" = 'reklaNowa' lub widoki z "ZReklami" w nazwie

### SPRZEDAŻ / KANAŁY:
- "eBay", "Allegro", "shopEU", "shopPL" → kolumny w widokach rapdb.VwNowyRaportMaggo*

### PRZYKŁADY POPRAWNYCH ZAPYTAŃ:

Pytanie: "pokaż jakie części dziś pobrano do skrzyń z podziałem na twórcę i skrzynię"
SQL: SELECT pak."pakUserCreateLogin" AS "Twórca", zlecp."proNrPartii" AS "Skrzynia", art."artNazwa" AS "Część", poz."ilosc" AS "Ilość" FROM "public"."wmsPakunkiHist" AS pak JOIN "public"."wmsPakunkiPozycjeHist" AS poz ON poz."pakId" = pak."pakId" JOIN "public"."wmsArtykuly" AS art ON art."artId" = poz."artId" JOIN "public"."wmsZleceniaProdukcyjneHist" AS zlecp ON zlecp."zlecId" = pak."pakZlecId" WHERE pak."pakDataWyd"::date = CURRENT_DATE LIMIT 1000

Pytanie: "top 10 twórców po punktach produkcji w ostatnim tygodniu"
SQL: SELECT "TworcaMaggo", SUM("PunktyProd") AS "Suma punktów" FROM "rapdb"."VwNowyRaportMaggoPktTworcBezUjemnychVer2" WHERE "DataProduktu"::date >= CURRENT_DATE - INTERVAL '7 days' GROUP BY "TworcaMaggo" ORDER BY "Suma punktów" DESC LIMIT 10

==========================================================================
🔵 MS SQL SERVER — DANE SPRZEDAŻOWE, OPERACYJNE, KADROWE (dialekt T-SQL!)
==========================================================================

✅ UPRAWNIENIA: Użytkownik artur_ro ma SELECT na WSZYSTKIE 20 baz MSSQL!

**Mapa baz — gdzie czego szukać:**

📦 **ebayApiDB** (134 tabele) — sprzedaż eBay, transakcje, aukcje, regulatory cen (arc_*)
🏢 **STEEPC** (291 tabel) ⭐ GŁÓWNA — klienci (scKLIENT), zamówienia (scZAMKLINAG/scZAMKLISZCZEG), zwroty/austausche (scZwrotNag, scAustauch, v_austachStatus), faktury (scFAKTNAG), płatności, adresy kurierskie, skrzynie (scSkrzynia), kolektory (scKolektor)
🛒 **SHOP_PMG** (88 tabel) — sklep własny shopPL/shopEU (shop_order, shop_Items, shop_orderSzczeg)
🎯 **SZTURCHACZ** — panel obsługi austauchów (etapy, drabiny eskalacji, ustalenia z klientami, kotwice)
💰 **AUSTAUCH-ROZLICZENIE** — rozliczenia finansowe austauchów, kaucje, zwroty pieniędzy
💵 **WYPLATOMAT** — wynagrodzenia twórców, kalkulacje wypłat, punkty produkcyjne
💲 **EdytorCen** — historia zmian cen, kto edytował kiedy
📊 **RAPDB** — gotowe widoki raportowe MSSQL (analogia do Postgresowego rapdb)
🔬 **EKSPERTYZY-HELPER** — ekspertyzy techniczne pojazdów
📑 **GENERATOR-TYTULOW** — generowanie tytułów aukcji
🏷️ **STEEPW** — UWAGA! NIE UŻYWAJ do zapytań — widoki STEEPC czasem odwołują się do STEEPW i rzucają błąd
👥 **KADRY** — dane pracowników firmy
📅 **CALENDAR** — kalendarz firmowy, urlopy
📸 **GALERIE360 / GalerieCV** — zdjęcia produktów (galerie 360 stopni + CV)
🖥️ **PULPIT** — aplikacja pulpit, ustawienia użytkowników
💼 **FORFOR, JOBAGENT, RFID, WWMSDB** — specjalistyczne (przyjęcia RFID, kolejki zadań, WMS)

⚠️ JAK WYBRAĆ WŁAŚCIWĄ BAZĘ:
- "austausch" / "zwrot używanej skrzyni" → **STEEPC** (scZwrotNag, scAustauch) LUB **SZTURCHACZ** (pełny pipeline obsługi)
- "rozliczenie austauchu" / "kaucja" → **AUSTAUCH-ROZLICZENIE**
- "zarobki twórcy" / "wypłata" → **WYPLATOMAT**
- "historia zmiany ceny" → **EdytorCen**
- "zamówienie klienta" / "wysyłka" → **STEEPC** (scZAMKLINAG)
- "sprzedaż eBay" / "aukcja" → **ebayApiDB**
- "sklep własny" / "shopPL/shopEU" → **SHOP_PMG**
- "gotowy raport" → **RAPDB**
- "ekspertyza" → **EKSPERTYZY-HELPER**

⭐ KLUCZOWE TABELE/WIDOKI STEEPC (odkryte empirycznie):
- `scKLIENT` (15 kol) — klienci
- `scZAMKLINAG` (59 kol) + `scZAMKLISZCZEG` (35 kol) — zamówienia + pozycje
- `scZwrotNag` (21 kol) + `scZwrotSzczeg` (22 kol) — zwroty z typami (zwnTyp: 7=austausch, 4=reklamacja, 3=skup)
- `scAustauch` (13 kol) — OSOBNA TABELA dedykowana austauschom! Warto sprawdzić strukturę
- `v_austachStatus` (58 kol) — GOTOWY WIDOK statusu austauchów (prostsza wersja: v_austachStatusProsty 41 kol)
- `scSkrzynia` (34) + `scSkrzyniaHist` (36) — dane skrzyń
- `scKolektor` (6) + `scKolektor_hist` (8) — dane kolektorów
- `scFAKTNAG` (56) — faktury
- `scPospZwrotka` (45) — pospieszne zwrotki
- `ups_austauch_potw_listy` — potwierdzenia listów UPS dla austauchów

⭐⭐⭐ KLUCZOWE WIDOKI RAPDB (odkryte z PBIT — TAM JEST WIĘKSZOŚĆ TEGO CZEGO POTRZEBA!):

**RAPDB to baza raportowa** — Krzysiek już dawno przygotował widoki które agregują dane z STEEPC, ebayApiDB i innych. **JEŚLI PYTANIE DOTYCZY KOSZTÓW/SPRZEDAŻY/AUSTAUSCHÓW/PREMII — NAJPIERW SZUKAJ W RAPDB, NIE W STEEPC**.

Mapa "co gdzie szukać":

🔴 **KOSZTY FIRMOWE** (faktury, kontrahenci):
- `[RAPDB].[dbo].[VwKosztyVer1]` (36 kol) ⭐⭐⭐ MAIN — wszystkie koszty firmowe
  - Kluczowe kolumny: Data, Kontrachent, KontrachentNazwa, Kwota, Rozchod, Przychod, Kaucja
  - Tagi kategorii: TagNazwaP1, TagNazwaP2, TagNazwaP3 (3 poziomy taksonomii)
  - RodzajDokumentu, NrDokumentu, KomentarzDoDok, klNazwa
  - Plik (link do skanu faktury), TerminPlatnosci
- `[RAPDB].[dbo].[VwKosztyCsv]` (18 kol) — wersja eksportu CSV
- `[RAPDB].[dbo].[TblKoszty]` (28 kol) — tabela bazowa
- `[RAPDB].[dbo].[TblTagiP1/P2/P3]` — słowniki tagów kosztowych

🟡 **SPRZEDAŻ + UPS + AUSTAUSCHE per zamówienie**:
- `[RAPDB].[dbo].[VwZknFvUpsZwnVer1]` (23 kol) ⭐⭐⭐ MAIN
  - Per zamówienie: ZamDataTw, ZamUser, UserGrupa, IsoOdbiorcy, **TypArt** (skrzynia/kolektor)
  - Sumy: SumFvTotalAC (sprzedaż), SumUpsAC (koszt UPS), SumFvZwrRekAC
  - **Austausche: IlAust, IlAustZamkn, IleAustZamkZZoltkami** (specjalny status)
  - ProcUdzUpsWiersz (% udziału UPS), IlReklamacji, IlZwrot
  - Grupowania: ZamDataTwOkres, ZamDataTwWeekNum, ZamDataTw_Grup1mc, ZamDataTw_Grup3mc

🟢 **AUSTAUSCHE NIEZWRÓCONE** (klient nie oddał):
- `[RAPDB].[dbo].[VwAustauschNiezwrocnePmgTechVer2]` (39 kol)
- `[RAPDB].[dbo].[VwAustauschNiezwrocneWszystkieSpolkiVer2]` (41 kol) — wszystkie spółki
- `[RAPDB].[dbo].[VwEbayKomentarzeSkrzynieAustauschoweBezReklamacji]` — komentarze klientów

💰 **SKUPY** (kupowanie używanych skrzyń):
- `[RAPDB].[dbo].[VwSkupyPrzyjeteFvKsiegKasetkiKlientVer1]` (42 kol) — pełne info skupy + faktury + klient
- `[RAPDB].[dbo].[VwKosztySkupyPrzyjeteKlientVer1]` (15 kol) — koszty skupów per klient
- `[RAPDB].[dbo].[VwKasetkiPrzyjeciaKlient]` (34 kol)
- `[RAPDB].[dbo].[VwSkupyPrzyjeteFvKsiegKasKliNiezmatchPotencjalVer1]` (44 kol) — niezmatchowane

📊 **PREMIE/RENTOWNOŚĆ TEAM**:
- `[RAPDB].[dbo].[Vw_PremTeam_Fv_Main_Ver1]` (52 kol) ⭐ — pełna kalkulacja premii zespołowych
- `[RAPDB].[dbo].[Vw_PremTeam_Help_SumAustSumAustZamk]` — sumy austauschów zamkniętych do premii
- `[RAPDB].[dbo].[Vw_PremTeam_Fv_Help_ProcUpsGrupaArt3Mce]` — % UPS per grupa artykułów (3 miesiące)

🛒 **FAKTURY KLIENCKIE Z POZYCJAMI**:
- `[RAPDB].[dbo].[VwZknFvKliItemSzczegVer1]` (47 kol) — szczegóły faktur klienckich
- `[RAPDB].[dbo].[VwFvZamKliItemVer2]` (57 kol) — pełne pozycje
- `[RAPDB].[dbo].[VwFvMastFvZamKliTArtTDokVer1]` (28 kol) — master + zamówienia + artykuły

📦 **CSV / DOKUMENTY UPS+FAKTURY**:
- `[RAPDB].[dbo].[VwCsvFvUpsZamZPodzialemArtMaxZZksVer1]` (66 kol)
- `[RAPDB].[dbo].[VwTblCsvPolaczoneVer2]` (68 kol)

🛍️ **WSTAŻKI SPRZEDAŻOWE** (śledzenie statusu zamówienia od A do Z):
- `[RAPDB].[dbo].[VwWstazkiSprzed]` (67 kol)
- `[RAPDB].[dbo].[VwWstazkiSprzedPodsum]` (66 kol) — podsumowanie
- `[RAPDB].[dbo].[VwWstazkiSprzedPodsumOtwarte]` / `Zamkniete` — w toku / zakończone

💵 **PŁATNOŚCI BANKOWE** (do dopasowywania faktur):
- `[RAPDB].[dbo].[TblBankBV/DB/PKO/PayU/PayPal]` — historie z banków/PayU/PayPal
- `[RAPDB].[dbo].[VwPkoVer1]` (53 kol)

📅 **KALENDARZ DNI ROBOCZYCH**:
- `[RAPDB].[dbo].[TblDniRoboczeWMcu]` — ile dni roboczych w danym miesiącu (przydatne do średnich)

⭐ INNE WIDOKI (mniej priorytetowe):
- `[RAPDB].[dbo].[VwSprzedazRapPmgTech]` (8 kol) — sprzedaż per spółka tech
- `[RAPDB].[dbo].[VwArtykulyNieposiadane]` (11 kol) — artykuły bez stanu

🔮 KIEDY UŻYĆ JAKIEGO WIDOKU — REGUŁA KCIUKA:
- "Koszty firmowe / faktury kontrahenckie" → **VwKosztyVer1** (RAPDB)
- "Sprzedaż per zamówienie / per operator / per kraj" → **VwZknFvUpsZwnVer1** (RAPDB)
- "Austausche zamknięte/żółtki/% austauchu" → **VwZknFvUpsZwnVer1** (RAPDB)
- "Skupy używanych skrzyń" → **VwSkupyPrzyjeteFvKsiegKasetkiKlientVer1** (RAPDB)
- "Niezwrócone austausche" → **VwAustauschNiezwrocneWszystkieSpolkiVer2** (RAPDB)
- "Premie zespołów" → **Vw_PremTeam_Fv_Main_Ver1** (RAPDB)
- "Koszt części produkcji per skrzynia" → wmsPakunkiHist + wmsArtykulySrCenyZakHist (Postgres) — to robimy lokalnie, RAPDB tego nie ma

⭐ KLUCZOWE TABELE/WIDOKI STEEPC (odkryte empirycznie):

⚠️ DIALEKT T-SQL — NIE UŻYWAJ SKŁADNI POSTGRESA:
  ❌ LIMIT 100                  → ✅ SELECT TOP 100
  ❌ CURRENT_DATE               → ✅ CAST(GETDATE() AS DATE)
  ❌ '2026-04-23'::date         → ✅ CAST('2026-04-23' AS DATE)
  ❌ INTERVAL '7 days'          → ✅ DATEADD(day, -7, GETDATE())
  ❌ ILIKE                      → ✅ LIKE (case-insensitive domyślnie w SQL Server)
  ❌ "public"."tabela"          → ✅ [dbo].[tabela] lub [BAZA].[dbo].[tabela]

⚠️ NAZWY BAZ Z MYŚLNIKAMI — MUSZĄ być w [nawiasach]:
   [AUSTAUCH-ROZLICZENIE], [EKSPERTYZY-HELPER], [GENERATOR-TYTULOW]

⚠️ JEŚLI CHCESZ SIĘGAĆ DO WIDOKU V_AUSTACHSTATUS:
Uwaga — widok może cross-joinować do bazy STEEPW. Jeśli rzuci błąd 916, próbuj v_austachStatusProsty zamiast.

POPRAWNE PRZYKŁADY:
-- Indeksy w zamówieniach:
SELECT TOP 100 * FROM [STEEPC].[dbo].[scZAMKLISZCZEG] WHERE [zksIndex] LIKE '206222%'

-- Austausche w toku dla konkretnego zamówienia:
SELECT * FROM [STEEPC].[dbo].[scZwrotNag] WHERE [zwnZamNr] = 374937 AND [zwnStatus] = 1

-- Dashboard austauchów (gotowy widok):
SELECT TOP 100 * FROM [STEEPC].[dbo].[v_austachStatusProsty] ORDER BY 1 DESC

-- Lista tabel w SZTURCHACZ (rozpoznanie):
SELECT TABLE_NAME FROM [SZTURCHACZ].INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME

⚠️ GDY PYTANIE DOTYCZY OBU BAZ (Postgres+MSSQL):
Nie da się zrobić jednego zapytania łączącego oba silniki.
- Pytanie o koszt części → Postgres (wmsArtykulySrCenyZakHist)
- Pytanie o cenę sprzedaży → MSSQL (ebayApiDB / SHOP_PMG / STEEPC)
- Pytanie o zysk = koszt - cena → niemożliwe jednym zapytaniem; zrób dwa i połącz po ID

Pytanie: "pokaż tabele w bazie SZTURCHACZ"
SQL: SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM [SZTURCHACZ].INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME

Pytanie: "ile austauchów jest w toku dla skrzyń z indeksem 206222"
SQL: SELECT COUNT(DISTINCT n.[zwnID]) FROM [STEEPC].[dbo].[scZwrotNag] n JOIN [STEEPC].[dbo].[scZwrotSzczeg] s ON s.[zwsZwnID]=n.[zwnID] WHERE n.[zwnTyp]=7 AND n.[zwnStatus]=1 AND s.[zwsOpis]='Program Austauch SK' AND n.[zwnZamNr] IN (SELECT DISTINCT [zksZamNr] FROM [STEEPC].[dbo].[scZAMKLISZCZEG] WHERE [zksIndex] LIKE '206222%')
"""


# ==========================================
# VERTEX AI — INICJALIZACJA
# ==========================================
@st.cache_resource
def init_vertex_ai():
    """Inicjalizuje Vertex AI, losuje projekt GCP."""
    try:
        import vertexai
        from google.oauth2 import service_account
    except ImportError:
        return None, "Brak biblioteki google-cloud-aiplatform"

    try:
        gcp_projects = st.secrets.get("GCP_PROJECT_IDS", [])
        if isinstance(gcp_projects, str):
            gcp_projects = [gcp_projects]
        gcp_projects = list(gcp_projects)

        if not gcp_projects:
            return None, "Brak GCP_PROJECT_IDS w secrets"

        # Losowy projekt (load balancing)
        project_id = random.choice(gcp_projects)
        location = st.secrets.get("GCP_LOCATION", "us-central1")

        creds_info = json.loads(st.secrets["FIREBASE_CREDS"])
        creds = service_account.Credentials.from_service_account_info(creds_info)

        vertexai.init(project=project_id, location=location, credentials=creds)
        return project_id, None
    except Exception as e:
        return None, f"Błąd Vertex AI: {e}"


# ==========================================
# POŁĄCZENIE SSH + PG
# ==========================================
def otworz_tunel():
    from sshtunnel import SSHTunnelForwarder
    return SSHTunnelForwarder(
        (st.secrets["SSH_HOST"], int(st.secrets["SSH_PORT"])),
        ssh_username=st.secrets["SSH_USER"],
        ssh_password=st.secrets["SSH_PASSWORD"],
        remote_bind_address=(st.secrets["PG_HOST"], int(st.secrets["PG_PORT"])),
        local_bind_address=('127.0.0.1', 0),
    )


def pg_connect(tunnel, dbname):
    import psycopg2
    return psycopg2.connect(
        host='127.0.0.1', port=tunnel.local_bind_port,
        user=st.secrets["PG_USER"], password=st.secrets["PG_PASSWORD"],
        dbname=dbname, options="-c statement_timeout=300000", connect_timeout=15,
    )


# ==========================================
# SQL SERVER (druga gałąź - sprzedaż)
# ==========================================
def otworz_tunel_mssql():
    """Drugi tunel SSH do SQL Server na porcie 1433."""
    from sshtunnel import SSHTunnelForwarder
    return SSHTunnelForwarder(
        (st.secrets["SSH_HOST"], int(st.secrets["SSH_PORT"])),
        ssh_username=st.secrets["SSH_USER"],
        ssh_password=st.secrets["SSH_PASSWORD"],
        remote_bind_address=(st.secrets.get("MSSQL_HOST", "192.168.3.131"), int(st.secrets.get("MSSQL_PORT", 1433))),
        local_bind_address=('127.0.0.1', 0),
    )


def mssql_connect(tunnel, database):
    """Połączenie z bazą SQL Server przez tunel.
    Wymusza TDS 7.4 (zweryfikowane jako działające dla MSSQL 2017 Express).
    Serwer 192.168.3.138:1433 — inny niż Postgres!"""
    import pymssql
    return pymssql.connect(
        server='127.0.0.1',
        port=tunnel.local_bind_port,
        user=st.secrets["MSSQL_USER"],
        password=st.secrets["MSSQL_PASSWORD"],
        database=database,
        timeout=300,        # 5 minut na wykonanie zapytania (duże raporty)
        login_timeout=20,   # 20s na samo logowanie
        charset='UTF-8',
        tds_version='7.4',
    )


def diagnostyka_mssql():
    """Sprawdza krok po kroku co działa, a co nie. Zwraca listę kroków z wynikami."""
    kroki = []

    # KROK 1: Biblioteka
    try:
        import pymssql
        import socket
        kroki.append({"krok": "1️⃣ Biblioteki", "status": "✅", "info": f"pymssql {pymssql.__version__}"})
    except ImportError as e:
        kroki.append({"krok": "1️⃣ Biblioteki", "status": "❌", "info": f"Brak pymssql: {e}"})
        return kroki

    # KROK 2: Tunel SSH
    try:
        from sshtunnel import SSHTunnelForwarder
        tunnel = SSHTunnelForwarder(
            (st.secrets["SSH_HOST"], int(st.secrets["SSH_PORT"])),
            ssh_username=st.secrets["SSH_USER"],
            ssh_password=st.secrets["SSH_PASSWORD"],
            remote_bind_address=(st.secrets.get("MSSQL_HOST", "192.168.3.131"), 1433),
            local_bind_address=('127.0.0.1', 0),
        )
        tunnel.start()
        kroki.append({"krok": "2️⃣ Tunel SSH do 1433", "status": "✅",
                      "info": f"Otwarty na lokalnym porcie {tunnel.local_bind_port}"})
    except Exception as e:
        kroki.append({"krok": "2️⃣ Tunel SSH do 1433", "status": "❌", "info": str(e)[:300]})
        return kroki

    # KROK 3: Test socketu - czy coś nasłuchuje
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect(('127.0.0.1', tunnel.local_bind_port))
        s.close()
        kroki.append({"krok": "3️⃣ Socket TCP na 1433", "status": "✅",
                      "info": "Serwer nasłuchuje i akceptuje połączenia TCP"})
    except Exception as e:
        kroki.append({"krok": "3️⃣ Socket TCP na 1433", "status": "❌",
                      "info": f"Serwer nie odpowiada: {e}"})
        tunnel.stop()
        return kroki

    # KROK 4: Różne wersje TDS — spróbuj wszystkich
    databases = st.secrets.get("MSSQL_DATABASES", ["master"])
    test_db = "master"  # master jest zawsze; bazy użytkownika mogą nie być
    wersje_do_testu = ["7.4", "7.3", "7.2", "7.1", "7.0", "8.0"]
    zaladowana_wersja = None

    for wersja in wersje_do_testu:
        try:
            conn = pymssql.connect(
                server='127.0.0.1',
                port=tunnel.local_bind_port,
                user=st.secrets["MSSQL_USER"],
                password=st.secrets["MSSQL_PASSWORD"],
                database=test_db,
                timeout=10,
                login_timeout=5,
                charset='UTF-8',
                tds_version=wersja,
            )
            cur = conn.cursor()
            cur.execute("SELECT @@VERSION")
            sql_version = cur.fetchone()[0]
            conn.close()
            kroki.append({"krok": f"4️⃣ Login SQL z TDS {wersja}", "status": "✅",
                          "info": f"Wersja serwera: {str(sql_version)[:150]}"})
            zaladowana_wersja = wersja
            break
        except Exception as e:
            err_msg = str(e)[:200]
            kroki.append({"krok": f"4️⃣ Login SQL z TDS {wersja}", "status": "❌",
                          "info": err_msg})

    # KROK 4b: Surowy test handshake — co serwer mówi na plain TCP?
    if not zaladowana_wersja:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect(('127.0.0.1', tunnel.local_bind_port))
            # Wyślij TDS pre-login packet (handshake)
            # Header: 0x12 (Pre-Login), 0x01 (EOM), 0x00 0x2F (length), 0x00 0x00 0x00 0x00 (SPID/PackID), 0x00 0x00
            prelogin = bytes.fromhex('12010026000000000000001500000601000b000103000e000402000f00010000')
            s.sendall(prelogin)
            response = s.recv(1024)
            s.close()
            if response:
                kroki.append({"krok": "4b️⃣ Surowy TDS pre-login", "status": "⚠️",
                              "info": f"Serwer odpowiedział {len(response)} bajtów: {response[:50].hex()}..."})
            else:
                kroki.append({"krok": "4b️⃣ Surowy TDS pre-login", "status": "❌",
                              "info": "Serwer zamknął połączenie bez odpowiedzi — to NIE jest serwer TDS/MSSQL!"})
        except Exception as e:
            kroki.append({"krok": "4b️⃣ Surowy TDS pre-login", "status": "❌",
                          "info": f"Socket: {e}"})

    # KROK 4c: Może to nie SQL Server a co innego — pozdrowienie banera?
    if not zaladowana_wersja:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            s.connect(('127.0.0.1', tunnel.local_bind_port))
            # Poczekaj czy serwer sam się przywita
            s.settimeout(2)
            try:
                banner = s.recv(256)
                if banner:
                    try:
                        banner_text = banner.decode('utf-8', errors='replace')[:200]
                    except:
                        banner_text = banner[:50].hex()
                    kroki.append({"krok": "4c️⃣ Baner serwera", "status": "⚠️",
                                  "info": f"Serwer się przywitał (to może być Postgres/SSH/inne!): {banner_text}"})
                else:
                    kroki.append({"krok": "4c️⃣ Baner serwera", "status": "✅",
                                  "info": "Serwer czeka na dane od klienta (typowe dla MSSQL)"})
            except socket.timeout:
                kroki.append({"krok": "4c️⃣ Baner serwera", "status": "✅",
                              "info": "Serwer czeka na dane od klienta (typowe dla MSSQL)"})
            s.close()
        except Exception as e:
            kroki.append({"krok": "4c️⃣ Baner serwera", "status": "❌", "info": str(e)[:200]})

    if not zaladowana_wersja:
        tunnel.stop()
        kroki.append({"krok": "💡 Wniosek", "status": "❌",
                      "info": "TDS nie nawiązuje się ani w jednej wersji. Możliwe przyczyny: "
                              "(1) Port 1433 na serwerze prowadzi gdzie indziej (nie do MSSQL). "
                              "(2) MSSQL wymaga SSL/TLS już na handshake. "
                              "(3) Blokada na poziomie serwera. "
                              "Najpewniejszy krok: zapytaj Krzyśka czy tunel/port faktycznie dociera do MSSQL, "
                              "poproś żeby sprawdził `netstat -an | grep 1433` na serwerze."})
        return kroki

    # KROK 5: Sprawdź jakie bazy ma user
    try:
        conn = pymssql.connect(
            server='127.0.0.1',
            port=tunnel.local_bind_port,
            user=st.secrets["MSSQL_USER"],
            password=st.secrets["MSSQL_PASSWORD"],
            database="master",
            timeout=10,
            login_timeout=5,
            charset='UTF-8',
            tds_version=zaladowana_wersja,
        )
        cur = conn.cursor()
        cur.execute("SELECT name FROM sys.databases ORDER BY name")
        widoczne_bazy = [r[0] for r in cur.fetchall()]
        conn.close()
        kroki.append({"krok": "5️⃣ Lista baz widocznych dla usera", "status": "✅",
                      "info": ", ".join(widoczne_bazy)})

        # Sprawdź czy konfigurowane bazy są dostępne
        zadane = st.secrets.get("MSSQL_DATABASES", [])
        brakujace = [b for b in zadane if b not in widoczne_bazy]
        if brakujace:
            kroki.append({"krok": "⚠️ Uwaga", "status": "⚠️",
                          "info": f"Zadane bazy nie są widoczne: {brakujace}. Może Krzysiek jeszcze nie nadał uprawnień."})
    except Exception as e:
        kroki.append({"krok": "5️⃣ Lista baz", "status": "❌", "info": str(e)[:200]})

    # KROK 6: Próba pobrania pierwszych tabel z każdej bazy
    for db_name in st.secrets.get("MSSQL_DATABASES", []):
        try:
            conn = pymssql.connect(
                server='127.0.0.1',
                port=tunnel.local_bind_port,
                user=st.secrets["MSSQL_USER"],
                password=st.secrets["MSSQL_PASSWORD"],
                database=db_name,
                timeout=10,
                login_timeout=5,
                charset='UTF-8',
                tds_version=zaladowana_wersja,
            )
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")
            ile = cur.fetchone()[0]
            cur.execute("SELECT TOP 5 TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME")
            sample = cur.fetchall()
            conn.close()
            kroki.append({
                "krok": f"6️⃣ Baza {db_name}",
                "status": "✅",
                "info": f"{ile} tabel/widoków. Przykłady: " + ", ".join([f"{s}.{t}" for s, t in sample])
            })
        except Exception as e:
            kroki.append({"krok": f"6️⃣ Baza {db_name}", "status": "❌", "info": str(e)[:200]})

    tunnel.stop()

    kroki.append({
        "krok": "💡 Wniosek",
        "status": "✅",
        "info": f"Połączenie działa z TDS {zaladowana_wersja}. Jeśli chcesz — zaktualizuję kod żeby wymuszał tę wersję."
    })
    return kroki


@st.cache_data(ttl=3600)
def pobierz_schema_mssql():
    """Pobiera schemat wszystkich tabel/widoków we WSZYSTKICH bazach SQL Server.
    Zwraca dict z kluczami '[MSSQL] db.schema.table'.
    Odporne na padające bazy — jedna baza nie wyłącza reszty."""
    try:
        import pymssql
    except ImportError:
        return None

    databases = st.secrets.get("MSSQL_DATABASES", ["ebayApiDB", "STEEPC", "SHOP_PMG"])
    schema = {}
    problemy = []

    try:
        with otworz_tunel_mssql() as tunnel:
            for db_name in databases:
                try:
                    conn = mssql_connect(tunnel, db_name)
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                        ORDER BY TABLE_SCHEMA, TABLE_NAME
                    """)
                    tables = cur.fetchall()
                    count_db = 0
                    for tschema, tname, ttype in tables:
                        cur.execute("""
                            SELECT COLUMN_NAME, DATA_TYPE
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                            ORDER BY ORDINAL_POSITION
                        """, (tschema, tname))
                        cols = cur.fetchall()
                        key = f"[MSSQL] {db_name}.{tschema}.{tname}"
                        schema[key] = {
                            "type": ttype,
                            "columns": [{"name": c[0], "type": c[1]} for c in cols],
                            "database": db_name,
                            "db_schema": tschema,
                            "table_name": tname,
                            "engine": "mssql",
                        }
                        count_db += 1
                    conn.close()
                except Exception as e:
                    problemy.append(f"{db_name}: {str(e)[:100]}")
                    continue
    except Exception as e:
        st.error(f"Błąd tunelu SQL Server: {e}")
        return None

    if problemy:
        st.session_state["_mssql_problemy_bazy"] = problemy

    # === PERSYSTENCJA: zapisuję do Firestore żeby przeżyło restart apki ===
    if schema:
        try:
            zapisz_schema_mssql_firestore(schema)
        except Exception as e:
            st.warning(f"⚠️ Schemat pobrany ale nie zapisany w Firestore: {str(e)[:100]}")

    return schema


def zapisz_schema_mssql_firestore(schema):
    """Zapisuje schemat MSSQL do Firestore żeby przeżył restart apki.
    Firestore ma limit 1MB per dokument, więc dzielimy na chunki po 200 tabel."""
    if not schema or not db:
        return
    from datetime import datetime
    
    keys = sorted(schema.keys())
    chunk_size = 200
    chunks = [keys[i:i+chunk_size] for i in range(0, len(keys), chunk_size)]
    
    # Wyczyść stare chunki
    old_docs = db.collection("schema_mssql_cache").stream()
    for d in old_docs:
        d.reference.delete()
    
    # Zapisz nowe chunki
    for idx, chunk_keys in enumerate(chunks):
        chunk_data = {k: schema[k] for k in chunk_keys}
        db.collection("schema_mssql_cache").document(f"chunk_{idx:03d}").set({
            "data": chunk_data,
            "chunk_idx": idx,
            "total_chunks": len(chunks),
            "saved_at": datetime.utcnow().isoformat(),
            "total_keys": len(keys),
        })
    
    # Meta info
    db.collection("schema_mssql_cache").document("_meta").set({
        "total_chunks": len(chunks),
        "total_keys": len(keys),
        "saved_at": datetime.utcnow().isoformat(),
    })


def wczytaj_schema_mssql_firestore():
    """Wczytuje schemat MSSQL z Firestore (jeśli istnieje).
    Zwraca dict albo None."""
    if not db:
        return None
    try:
        meta = db.collection("schema_mssql_cache").document("_meta").get()
        if not meta.exists:
            return None
        meta_data = meta.to_dict()
        total_chunks = meta_data.get("total_chunks", 0)
        if total_chunks == 0:
            return None
        
        schema = {}
        for idx in range(total_chunks):
            doc = db.collection("schema_mssql_cache").document(f"chunk_{idx:03d}").get()
            if doc.exists:
                chunk = doc.to_dict().get("data", {})
                schema.update(chunk)
        
        return schema if schema else None
    except Exception as e:
        return None


def odpal_zapytanie_mssql(sql, database="ebayApiDB"):
    """Odpala zapytanie na SQL Server w danej bazie."""
    try:
        import pymssql
    except ImportError:
        return None, "Brak biblioteki pymssql. Dodaj do requirements.txt"

    try:
        with otworz_tunel_mssql() as tunnel:
            conn = mssql_connect(tunnel, database)
            df = pd.read_sql(sql, conn)
            conn.close()
            return df, None
    except Exception as e:
        return None, f"SQL Server: {str(e)}"


def wykryj_silnik_z_sql(sql):
    """Heurystyka: czy zapytanie dotyczy Postgresa czy SQL Server.
    Zwraca 'postgres' lub ('mssql', <database>)."""
    sql_lower = sql.lower()
    mssql_databases = st.secrets.get("MSSQL_DATABASES", ["ebayApiDB", "STEEPC", "SHOP_PMG"])

    # 0) NAJSILNIEJSZY SYGNAŁ: [nawiasy] z identyfikatorem MSSQL-style
    # np. [ebayApiDB], [AUSTAUCH-ROZLICZENIE], [dbo].[Orders]
    if re.search(r'\[[a-zA-Z_][\w\-]*\]\s*\.', sql):
        # Znaleźliśmy [coś]. — to prawie na pewno MSSQL
        # Spróbuj znaleźć nazwę bazy
        for db in mssql_databases:
            if re.search(rf'\[{re.escape(db)}\]', sql, re.IGNORECASE):
                return ("mssql", db)
        # Nie znaleziono konkretnej bazy ale jest [dbo]. — default ebayApiDB
        if re.search(r'\[dbo\]\s*\.', sql, re.IGNORECASE):
            return ("mssql", "ebayApiDB")
        # Jakiś [nawias]. ale nie rozpoznana baza — domyślnie MSSQL
        return ("mssql", "ebayApiDB")

    # 1) Jawne odwołanie do bazy MSSQL w zapytaniu bez nawiasów
    for db in mssql_databases:
        if re.search(rf'\b{re.escape(db)}\b', sql, re.IGNORECASE):
            return ("mssql", db)

    # 2) Specyficzne słowa MSSQL (T-SQL) — liczymy score
    mssql_sygnaly = [
        r'\btop\s+\d+\b',        # TOP N
        r'\bgetdate\s*\(\s*\)',  # GETDATE()
        r'\bisnull\s*\(',        # ISNULL
        r'\bdateadd\s*\(',       # DATEADD
        r'\bdatediff\s*\(',      # DATEDIFF
        r'\bnolock\b',           # NOLOCK hint
    ]
    mssql_score = sum(1 for p in mssql_sygnaly if re.search(p, sql_lower))

    # 3) Sygnały Postgresa
    pg_sygnaly = [
        '::',                   # PG cast
        'current_date',
        "interval '",
        '"public"',
        '"rapdb"',
        ' ilike ',
    ]
    pg_score = sum(1 for p in pg_sygnaly if p in sql_lower)

    # LIMIT na końcu (PG) a nie TOP (MSSQL) — gdy sam LIMIT i brak TOP
    if re.search(r'\blimit\s+\d+\s*$', sql_lower.strip(), re.IGNORECASE) and not re.search(r'\btop\s+\d+\b', sql_lower):
        pg_score += 1

    if mssql_score > pg_score:
        return ("mssql", "ebayApiDB")

    return "postgres"


def diagnostyka():
    wyniki = {"bazy": [], "widoki_rentownosci": {}, "wszystkie_schematy": {}}
    try:
        with otworz_tunel() as tunnel:
            st.info("✅ Tunel SSH otwarty")
            for sys_db in ["postgres", "template1", "maggo"]:
                try:
                    conn = pg_connect(tunnel, sys_db)
                    cur = conn.cursor()
                    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
                    wyniki["bazy"] = [r[0] for r in cur.fetchall()]
                    conn.close()
                    break
                except:
                    continue
            if not wyniki["bazy"]:
                return wyniki
            st.success(f"📋 Bazy: {', '.join(wyniki['bazy'])}")
            for db_name in wyniki["bazy"]:
                if db_name in ("postgres", "template0", "template1"):
                    continue
                try:
                    conn = pg_connect(tunnel, db_name)
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT schemaname, viewname FROM pg_views 
                        WHERE (viewname ILIKE '%raport%' OR viewname ILIKE '%rentow%' OR viewname ILIKE '%rpt%')
                        AND schemaname NOT IN ('pg_catalog', 'information_schema')
                        ORDER BY schemaname, viewname
                    """)
                    widoki = cur.fetchall()
                    if widoki:
                        wyniki["widoki_rentownosci"][db_name] = widoki
                    cur.execute("""
                        SELECT schema_name FROM information_schema.schemata 
                        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                        ORDER BY schema_name
                    """)
                    wyniki["wszystkie_schematy"][db_name] = [r[0] for r in cur.fetchall()]
                    conn.close()
                except Exception as e:
                    st.warning(f"Baza {db_name}: {str(e)[:100]}")
                    continue
    except Exception as e:
        st.error(f"❌ Tunel: {e}")

    try:
        db.collection("rentownosc_raporty").document("_diagnostyka").set({
            "wynik": wyniki,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    except:
        pass
    return wyniki


def wczytaj_diagnostyke_z_firestore():
    try:
        doc = db.collection("rentownosc_raporty").document("_diagnostyka").get()
        if doc.exists:
            d = doc.to_dict()
            return d.get("wynik"), d.get("updated_at")
    except:
        pass
    return None, None


# ==========================================
# INTROSPEKCJA BAZY (dla AI)
# ==========================================
@st.cache_data(ttl=3600)
def pobierz_schema_bazy():
    try:
        from sshtunnel import SSHTunnelForwarder
        import psycopg2
    except ImportError:
        return None

    try:
        with otworz_tunel() as tunnel:
            conn = pg_connect(tunnel, "maggo")
            cur = conn.cursor()
            cur.execute("""
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema IN ('public', 'rapdb')
                ORDER BY table_schema, table_name
            """)
            tables = cur.fetchall()

            schema = {}
            for schema_name, table_name, table_type in tables:
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema_name, table_name))
                cols = cur.fetchall()
                key = f"{schema_name}.{table_name}"
                schema[key] = {
                    "type": table_type,
                    "columns": [{"name": c[0], "type": c[1]} for c in cols]
                }
            conn.close()
            return schema
    except Exception as e:
        st.error(f"Błąd introspekcji: {e}")
        return None


def schema_do_tekstu_dla_ai(schema, schema_mssql=None, max_tables=100):
    if not schema:
        schema = {}
    if schema_mssql is None:
        schema_mssql = {}

    lines = []
    priority_keywords = ["rapdb", "wms", "Vw", "Maggo", "Raport"]

    # Kluczowe tabele dla produkcji DSG — pokazujemy WSZYSTKIE kolumny bez skracania
    KLUCZOWE_TABELE = {
        "public.wmsPakunkiHist",
        "public.wmsPakunkiPozycjeHist",
        "public.wmsPakunki",
        "public.wmsPakunkiPozycje",
        "public.wmsZleceniaProdukcyjne",
        "public.wmsZleceniaProdukcyjneHist",
        "public.wmsZleceniaProdukcyjnePozycje",
        "public.wmsZleceniaProdukcyjnePozycjeHist",
        "public.wmsZleceniaZadaniaProdukcyjne",
        "public.wmsZleceniaZadaniaProdukcyjneHist",
        "public.wmsZleceniaZadaniaProdukcyjnePracownicy",
        "public.wmsZleceniaZadaniaProdukcyjnePracownicyHist",
        "public.wmsArtykuly",
        "public.wmsArtykulySrCenyZakHist",
        "public.wmsArtykulyKontrahenci",
        "public.wmsUzytkownicy",
        "public.wmsMagazynMiejsca",
        "public.wmsNumeryPartii",
        "rapdb.VwNowyRaportMaggoBezUjemnychVer1",
        "rapdb.VwNowyRaportMaggoBezUjemnychZReklamiVer1",
        "rapdb.VwNowyRaportMaggoKolektorowBezUjemnychVer1",
        "rapdb.VwNowyRaportMaggoPktTworcBezUjemnychVer2",
        "rapdb.VwNowyRaportMaggoPktTworcBezUjemnychZReklamiVer2",
        "rapdb.VwSkrzynieStareNoweKolekiNoweTworcyVer1",
    }

    def priority(key):
        if key in KLUCZOWE_TABELE:
            return -1  # kluczowe zawsze na górze
        for i, kw in enumerate(priority_keywords):
            if kw.lower() in key.lower():
                return i
        return len(priority_keywords)

    # === POSTGRES ===
    lines.append("# === POSTGRESQL (baza 'maggo', schematy 'public' i 'rapdb') ===")
    lines.append("# Produkcja, pobory części, punkty twórców, skrzynie wytworzone")
    sorted_keys = sorted(schema.keys(), key=priority)[:max_tables]
    for key in sorted_keys:
        info = schema[key]
        type_label = "VIEW" if info["type"] == "VIEW" else "TABLE"
        limit_cols = len(info["columns"]) if key in KLUCZOWE_TABELE else 30
        cols_str = ", ".join([f'{c["name"]} ({c["type"]})' for c in info["columns"][:limit_cols]])
        if len(info["columns"]) > limit_cols:
            cols_str += f" ... +{len(info['columns'])-limit_cols} więcej"
        marker = " ⭐KLUCZOWA" if key in KLUCZOWE_TABELE else ""
        lines.append(f'PG {type_label} "{key}"{marker}: {cols_str}')

    # === SQL SERVER ===
    if schema_mssql:
        lines.append("")
        lines.append("# === MS SQL SERVER (bazy: ebayApiDB, STEEPC, SHOP_PMG) ===")
        lines.append("# Sprzedaż, ceny, prowizje, zamówienia klienckie")
        lines.append("# UWAGA: Dialekt T-SQL! Używaj TOP N zamiast LIMIT, GETDATE() zamiast CURRENT_DATE, []/[] dla identyfikatorów")
        # Grupuj po bazie
        for key in sorted(schema_mssql.keys())[:max_tables]:
            info = schema_mssql[key]
            type_label = "VIEW" if info["type"] == "VIEW" else "TABLE"
            limit_cols = min(40, len(info["columns"]))  # MSSQL jest mniej zmapowany, więc pokazujemy więcej
            cols_str = ", ".join([f'{c["name"]} ({c["type"]})' for c in info["columns"][:limit_cols]])
            if len(info["columns"]) > limit_cols:
                cols_str += f" ... +{len(info['columns'])-limit_cols} więcej"
            lines.append(f'MSSQL {type_label} {key}: {cols_str}')

    return "\n".join(lines)


def kolumny_tabeli(schema, tabela_nazwa):
    """Zwraca listę nazw kolumn dla danej tabeli. tabela_nazwa w formacie 'schema.nazwa' lub 'nazwa'."""
    # Szukaj w różnych wariantach zapisu
    for key in schema.keys():
        schema_name, table_name = key.split(".", 1) if "." in key else ("public", key)
        if key == tabela_nazwa or table_name == tabela_nazwa or key.endswith("." + tabela_nazwa):
            return [c["name"] for c in schema[key]["columns"]]
    return []


def wyciagnij_nazwy_tabel_z_sql(sql):
    """Wyciąga nazwy tabel z zapytania SQL (po FROM i JOIN)."""
    sql_clean = re.sub(r'--[^\n]*', '', sql)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
    # Szukaj: FROM "schema"."table" lub FROM schema.table lub FROM table
    pattern = r'(?:FROM|JOIN)\s+(?:"([^"]+)"\.)?"?([^"\s,]+)"?'
    tables = []
    for m in re.finditer(pattern, sql_clean, re.IGNORECASE):
        schema_name = m.group(1) or "public"
        table_name = m.group(2)
        if table_name.upper() in ("SELECT", "WHERE", "GROUP", "ORDER", "LIMIT", "ON"):
            continue
        tables.append(f"{schema_name}.{table_name}")
    return list(set(tables))


# ==========================================
# AI — NATURAL LANGUAGE → SQL (VERTEX AI)
# ==========================================
def wygeneruj_sql_przez_ai(pytanie_pl, schema_text):
    try:
        from vertexai.generative_models import GenerativeModel
    except ImportError:
        return None, "Brak biblioteki google-cloud-aiplatform"

    # Upewnij się że Vertex AI jest zainicjalizowane
    project_id, err = init_vertex_ai()
    if err:
        return None, err

    # Słownik pojęć domenowych (można rozbudowywać w sesji)
    slownik = st.session_state.get("slownik_dziedzinowy", DOMYSLNY_SLOWNIK)

    prompt = f"""Jesteś ekspertem SQL. Użytkownik opisuje po polsku co chce zobaczyć, a Ty generujesz odpowiednie zapytanie SQL.

W FIRMIE SĄ DWIE BAZY (musisz wybrać właściwą na podstawie pytania):

🟢 **POSTGRESQL (baza 'maggo')** — dane PRODUKCYJNE:
- Produkcja skrzyń (kto, kiedy, jaki index)
- Pobory części z magazynu (wmsPakunkiHist, wmsPakunkiPozycjeHist)
- Punkty twórców, wynagrodzenia, reklamacje
- Ceny zakupu części (wmsArtykulySrCenyZakHist, wmsArtykulyKontrahenci)
- Stany magazynowe (wmsMagazynMiejsca)
- Schematy: 'public' i 'rapdb'
- Dialekt: PostgreSQL (LIMIT, CURRENT_DATE, ::date, INTERVAL, STRING_AGG)

🔵 **MS SQL SERVER (bazy: ebayApiDB, STEEPC, SHOP_PMG)** — dane SPRZEDAŻOWE:
- ebayApiDB: sprzedaż i prowizje eBay
- STEEPC: ceny katalogowe, skup używanych skrzyń (austausche), wyceny
- SHOP_PMG: sklep własny shopPL / shopEU
- Dialekt: T-SQL (TOP N, GETDATE(), CAST AS DATE, ISNULL, []/[] identyfikatorów)
- Nie używaj LIMIT w MSSQL! Używaj SELECT TOP 1000 na początku zapytania.

PEŁNY SCHEMAT DOSTĘPNYCH TABEL:
{schema_text}

🧠 SŁOWNIK POJĘĆ DOMENOWYCH (autos-moto — regeneracja skrzyń biegów):
{slownik}

⚠️ KRYTYCZNA ZASADA DLA POSTGRESQL — CUDZYSŁOWY:
W bazie maggo WSZYSTKIE nazwy tabel i kolumn używają wielkich liter w środku (camelCase/PascalCase). PostgreSQL domyślnie zamienia wszystko na lowercase, więc MUSISZ używać cudzysłowów "..." wszędzie gdzie nazwa zawiera wielką literę.

POPRAWNE PG przykłady:
  SELECT T1."proNrPartii", T1."TworcaMaggo" FROM "public"."wmsZleceniaProdukcyjneHist" AS T1
  SELECT "grupaIgo", SUM("IleSkrzyn") FROM "rapdb"."VwNowyRaportMaggoBezUjemnychVer1"

⚠️ KRYTYCZNA ZASADA DLA SQL SERVER — IDENTYFIKATORY I DIALEKT:
POPRAWNE MSSQL przykłady:
  SELECT TOP 1000 [OrderId], [TotalPrice] FROM [ebayApiDB].[dbo].[Orders] WHERE [OrderDate] > CAST(GETDATE()-90 AS DATE)
  SELECT TOP 100 * FROM [STEEPC].[dbo].[Skupy]

ŚWIADOME PRZEŁĄCZANIE MIĘDZY BAZAMI:
- Pytanie o produkcję/twórców/pobory/regenerację → Postgres
- Pytanie o sprzedaż/eBay/Allegro/shop/austausch/skup → SQL Server
- Pytania hybrydowe (np. "zysk per skrzynia") nie da się zrobić jednym zapytaniem — wybierz stronę która lepiej pasuje, użytkownik domknie sam

ZASADY OGÓLNE:
1. Zwracaj TYLKO zapytanie SELECT — nigdy INSERT/UPDATE/DELETE/DROP/TRUNCATE/ALTER/CREATE.
2. Zawsze dodawaj limit wierszy (Postgres: LIMIT 1000 na końcu, MSSQL: TOP 1000 na początku).
3. Dla Postgresa: cudzysłowy i schemat jawny ("public"."tabela")
4. Dla MSSQL: [nawiasy] wokół identyfikatorów, pełne adresy [database].[schema].[table]
5. Wynik to JEDNO zapytanie SQL, bez komentarzy, bez markdown, bez średnika na końcu.

PYTANIE UŻYTKOWNIKA:
{pytanie_pl}

Zwróć tylko samo zapytanie SQL, nic więcej. Wybierz właściwy silnik bazy bazując na treści pytania."""

    try:
        model = GenerativeModel("gemini-2.5-pro")
        response = model.generate_content(prompt, generation_config={"temperature": 0.1})
        sql = response.text.strip()

        # Usuń markdown
        sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'^```\s*', '', sql)
        sql = re.sub(r'\s*```\s*$', '', sql)
        sql = sql.strip().rstrip(';').strip()

        # Walidacja — tylko SELECT
        sql_upper = sql.upper()
        forbidden = ["INSERT ", "UPDATE ", "DELETE ", "DROP ", "TRUNCATE ", "ALTER ", "CREATE ", "GRANT ", "REVOKE "]
        for f in forbidden:
            if f in sql_upper:
                return None, f"⚠️ Zapytanie zawiera niedozwolone słowo: {f.strip()}. Odrzucone."

        if not sql_upper.lstrip().startswith("SELECT") and not sql_upper.lstrip().startswith("WITH"):
            return None, "⚠️ Zapytanie nie zaczyna się od SELECT. Odrzucone."

        return sql, None
    except Exception as e:
        return None, f"Błąd Vertex AI: {e}"


def popraw_sql_po_bledzie(pierwotny_sql, blad, pytanie_pl, schema_text, schema_dict=None, initial_engine=None):
    """Prosi AI o poprawę SQL po błędzie z bazy. Dodaje realne kolumny tabel z błędu.
    initial_engine wymusza zachowanie silnika (żeby AI nie uciekło z MSSQL do PG)."""
    try:
        from vertexai.generative_models import GenerativeModel
    except ImportError:
        return None, "Brak biblioteki google-cloud-aiplatform"

    project_id, err = init_vertex_ai()
    if err:
        return None, err

    # Info o silniku (żeby AI nie zmieniało go między próbami)
    engine_info = ""
    if initial_engine:
        if isinstance(initial_engine, tuple) and initial_engine[0] == "mssql":
            db_name = initial_engine[1]
            engine_info = f"""
🚨 KRYTYCZNE: ORYGINALNE ZAPYTANIE BYŁO DLA MSSQL (baza: {db_name}).
POPRAWIONE ZAPYTANIE TEŻ MUSI BYĆ DLA MSSQL — NIE zmieniaj na Postgres!
- Używaj T-SQL: TOP zamiast LIMIT, GETDATE() zamiast CURRENT_DATE, [] zamiast ""
- Używaj pełnych nazw: [{db_name}].[schema].[tabela]
- Błąd 18456 = "Login failed" = użytkownik BEZ UPRAWNIEŃ do tej bazy. Spróbuj innej z dostępnych: ebayApiDB, STEEPC, SHOP_PMG (te na pewno działają).
- Tabele systemowe pytaj tak: SELECT TABLE_SCHEMA, TABLE_NAME FROM [{db_name}].INFORMATION_SCHEMA.TABLES
"""
        else:
            engine_info = """
🚨 KRYTYCZNE: ORYGINALNE ZAPYTANIE BYŁO DLA POSTGRESA.
POPRAWIONE ZAPYTANIE TEŻ MUSI BYĆ DLA POSTGRESA — NIE zmieniaj na MSSQL!
"""

    # Wyciągnij z błędu nazwę kolumny która nie istnieje i tabeli (alias.kolumna)
    realne_kolumny_info = ""
    if schema_dict:
        # --- POSTGRES: Pattern "kolumna X.Y nie istnieje" ---
        m = re.search(r'kolumna\s+(\w+)\.(\w+)\s+nie istnieje', blad, re.IGNORECASE)
        if m:
            alias = m.group(1)
            kolumna = m.group(2)
            tabela_match = re.search(
                rf'(?:"?(\w+)"?\s*\.\s*)?"?(\w+)"?\s+AS\s+{re.escape(alias)}(?=\s|$|\n)',
                pierwotny_sql,
                re.IGNORECASE
            )
            if tabela_match:
                schema_n = tabela_match.group(1) or "public"
                table_n = tabela_match.group(2)
                kolumny = kolumny_tabeli(schema_dict, f"{schema_n}.{table_n}")
                if not kolumny:
                    kolumny = kolumny_tabeli(schema_dict, table_n)
                if kolumny:
                    podobne = [c for c in kolumny if kolumna.lower() in c.lower() or c.lower() in kolumna.lower()]
                    realne_kolumny_info = f"""
🎯 REALNE KOLUMNY TABELI {schema_n}.{table_n} (alias: {alias}):
{', '.join(kolumny)}

Brakująca kolumna: "{kolumna}"
Podobne kolumny w tej tabeli: {', '.join(podobne) if podobne else 'BRAK podobnych — może trzeba całkiem innej kolumny?'}

⚠️ UŻYJ DOKŁADNIE JEDNEJ Z POWYŻSZYCH nazw. Nie zgaduj, nie wymyślaj wariantów — powyższa lista to PRAWDA z bazy danych!
"""
        wszystkie_matche = re.findall(r'kolumna\s+(\w+)\.(\w+)\s+nie istnieje', blad, re.IGNORECASE)
        if len(wszystkie_matche) > 1 and not realne_kolumny_info:
            tabele = wyciagnij_nazwy_tabel_z_sql(pierwotny_sql)
            kolumny_tabel = []
            for t in tabele:
                kols = kolumny_tabeli(schema_dict, t)
                if kols:
                    kolumny_tabel.append(f"\n{t}: {', '.join(kols)}")
            if kolumny_tabel:
                realne_kolumny_info = f"""
🎯 REALNE KOLUMNY TABEL UŻYTYCH W ZAPYTANIU:
{''.join(kolumny_tabel)}

⚠️ UŻYJ DOKŁADNIE tych nazw. Nie zgaduj.
"""

        # --- MSSQL: Pattern "Invalid column name 'X'" (NIE ma aliasa!) ---
        if not realne_kolumny_info:
            mssql_errors = re.findall(r"Invalid column name ['\"]?(\w+)['\"]?", blad, re.IGNORECASE)
            if mssql_errors:
                # Dla MSSQL musimy znaleźć które tabele są w zapytaniu i wylistować ICH kolumny
                # żeby AI mogło wybrać właściwą z prawdziwej listy
                kolumny_per_tabela = []
                # Wyciągnij wszystkie tabele z SQL w formie [baza].[schema].[tabela] lub [baza].[dbo].[tabela]
                mssql_tables = re.findall(
                    r'\[(\w+(?:-\w+)*)\]\s*\.\s*\[(\w+)\]\s*\.\s*\[(\w+)\]',
                    pierwotny_sql
                )
                for db_n, sch_n, tab_n in set(mssql_tables):
                    # Szukaj w schema_dict klucza [MSSQL] db.schema.table
                    key = f"[MSSQL] {db_n}.{sch_n}.{tab_n}"
                    info = schema_dict.get(key)
                    if info and "columns" in info:
                        cols = [c["name"] for c in info["columns"]]
                        # Dla każdej brakującej kolumny wyszukaj podobne
                        podobne_per_kol = {}
                        for brakujaca in mssql_errors:
                            podobne_kol = [c for c in cols if brakujaca.lower() in c.lower() or c.lower() in brakujaca.lower()]
                            if podobne_kol:
                                podobne_per_kol[brakujaca] = podobne_kol
                        kolumny_per_tabela.append(f"\n📋 [{db_n}].[{sch_n}].[{tab_n}]:\n   {', '.join(cols)}")
                        if podobne_per_kol:
                            for brakujaca, lista in podobne_per_kol.items():
                                kolumny_per_tabela.append(f"\n   ⚠️ Szukane '{brakujaca}' → podobne w tej tabeli: {', '.join(lista)}")
                if kolumny_per_tabela:
                    realne_kolumny_info = f"""
🎯 MSSQL — REALNE KOLUMNY TABEL UŻYTYCH W ZAPYTANIU:
{''.join(kolumny_per_tabela)}

⚠️ BRAKUJĄCE KOLUMNY WG BŁĘDU: {', '.join(mssql_errors)}
⚠️ UŻYJ DOKŁADNIE JEDNEJ z powyższych nazw. Nie zgaduj wariantów typu zksZamNr/zklZamNr — powyższa lista to PRAWDA z bazy!
"""

    # Użyj initial_engine (wymuszone) albo wykryj z pierwotnego SQL
    silnik_info = initial_engine if initial_engine else wykryj_silnik_z_sql(pierwotny_sql)
    if isinstance(silnik_info, tuple) and silnik_info[0] == "mssql":
        silnik_nazwa = "MS SQL SERVER"
        silnik_dialekt = f"""T-SQL (SQL Server). Baza docelowa: {silnik_info[1]}.
- Identyfikatory w [nawiasach] lub bez: [dbo].[Tabela] albo dbo.Tabela
- TOP N zamiast LIMIT N
- GETDATE() zamiast CURRENT_DATE
- CAST(x AS DATE) zamiast ::date
- DATEADD/DATEDIFF zamiast INTERVAL
- LIKE jest case-insensitive domyślnie (nie ILIKE)
- 🚨 BŁĄD 18456 "Login failed": użytkownik artur_ro NIE MA UPRAWNIEŃ do tej bazy.
  → ABSOLUTNIE NIE PRÓBUJ TEJ SAMEJ BAZY DRUGI RAZ
  → Użyj dostępnej bazy: ebayApiDB (sprzedaż eBay), STEEPC (skupy, ceny), SHOP_PMG (sklep własny)
  → Jeśli pytanie o austausche → próbuj STEEPC zamiast AUSTAUCH-ROZLICZENIE
- Nazwy baz MSSQL z myślnikami MUSZĄ być w nawiasach: [AUSTAUCH-ROZLICZENIE], nie AUSTAUCH-ROZLICZENIE"""
    else:
        silnik_nazwa = "POSTGRESQL"
        silnik_dialekt = """PostgreSQL. Baza 'maggo'.
- Identyfikatory z wielkimi literami MUSZĄ być w "cudzysłowach"
- LIMIT N (nie TOP)
- ::date (nie CAST AS DATE)
- CURRENT_DATE - INTERVAL '7 days'
- ILIKE dla case-insensitive"""

    prompt = f"""Jesteś ekspertem SQL. Poprzednie zapytanie padło z błędem — popraw je MINIMALNIE.

SILNIK BAZY: {silnik_nazwa}
Dialekt i zasady: {silnik_dialekt}

SCHEMAT BAZ (skrócony):
{schema_text[:8000]}

PIERWOTNE PYTANIE UŻYTKOWNIKA:
{pytanie_pl}

POPRZEDNIE (BŁĘDNE) ZAPYTANIE:
{pierwotny_sql}

KOMUNIKAT BŁĘDU:
{blad}
{realne_kolumny_info}

🔴 KRYTYCZNE INSTRUKCJE NAPRAWY:

1. **NIE ZMIENIAJ SILNIKA BAZY**: Jeśli oryginalny SQL był T-SQL (MSSQL), poprawka też MUSI być T-SQL. Jeśli był PostgreSQL, poprawka też PostgreSQL. Zwłaszcza NIE zamieniaj [nawiasów] MSSQL na "cudzysłowy" PG!

2. **MINIMALNA ZMIANA**: Popraw TYLKO to co jest błędne. Zachowaj wszystkie pozostałe tabele, JOIN-y, WHERE, kolumny w SELECT. NIE przepisuj zapytania od zera.

3. **WSKAZÓWKA HINT** (Postgres): Jeśli błąd zawiera "HINT: Być może chodziło ci o X" — użyj DOKŁADNIE tego X.

4. **JEŚLI MASZ LISTĘ REALNYCH KOLUMN** (sekcja 🎯 powyżej) — WYBIERZ Z NIEJ WŁAŚCIWĄ. NIE WYMYŚLAJ własnych nazw.

5. **CZĘSTE PUŁAPKI w PostgreSQL**:
   - Identyfikatory kończące się na "ID" są PISANE WIELKIMI LITERAMI: "pakID", "artID", "zlecprID"
   - ZAWSZE cytuj takie kolumny w cudzysłowach

6. **CZĘSTE PUŁAPKI w MSSQL**:
   - Błąd 18456 "Login failed" = brak uprawnień do tej bazy. Spróbuj innej bazy lub dodaj master.
   - Jeśli nazwa bazy ma myślnik (np. AUSTAUCH-ROZLICZENIE), MUSI być w [nawiasach]

7. Zwróć TYLKO poprawione zapytanie SELECT, bez markdown, bez komentarzy, bez średnika.
"""

    try:
        model = GenerativeModel("gemini-2.5-pro")
        response = model.generate_content(prompt, generation_config={"temperature": 0.1})
        sql = response.text.strip()

        sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'^```\s*', '', sql)
        sql = re.sub(r'\s*```\s*$', '', sql)
        sql = sql.strip().rstrip(';').strip()

        sql_upper = sql.upper()
        forbidden = ["INSERT ", "UPDATE ", "DELETE ", "DROP ", "TRUNCATE ", "ALTER ", "CREATE ", "GRANT ", "REVOKE "]
        for f in forbidden:
            if f in sql_upper:
                return None, f"⚠️ Poprawka zawiera niedozwolone słowo: {f.strip()}."

        if not sql_upper.lstrip().startswith("SELECT") and not sql_upper.lstrip().startswith("WITH"):
            return None, "⚠️ Poprawka nie zaczyna się od SELECT."

        return sql, None
    except Exception as e:
        return None, f"Błąd Vertex AI: {e}"


def odpal_zapytanie_sql(sql, dbname="maggo", force_engine=None):
    """Odpala zapytanie. force_engine = 'postgres' lub ('mssql', db_name) wymusza silnik (dla retry).
    Jeśli None — sama wykrywa z SQL."""
    if force_engine is not None:
        engine_info = force_engine
    else:
        engine_info = wykryj_silnik_z_sql(sql)

    if isinstance(engine_info, tuple) and engine_info[0] == "mssql":
        return odpal_zapytanie_mssql(sql, database=engine_info[1])

    # PostgreSQL
    try:
        from sshtunnel import SSHTunnelForwarder
        import psycopg2
    except ImportError:
        return None, "Brak bibliotek"

    try:
        with otworz_tunel() as tunnel:
            conn = pg_connect(tunnel, dbname)
            df = pd.read_sql(sql, conn)
            conn.close()
            return df, None
    except Exception as e:
        return None, str(e)


def odpal_z_self_healing(sql, pytanie_pl, schema_text, schema_dict=None, max_retries=3):
    """Odpala SQL, a w razie błędu prosi AI o poprawkę i ponawia (max_retries razy).
    Wykrywa silnik z ORYGINALNEGO SQL i wymusza go w retry — żeby AI nie 'uciekło' do innego silnika."""
    attempts = []
    current_sql = sql

    # Wykryj silnik z oryginalnego zapytania — będzie wymuszany przez wszystkie próby
    initial_engine = wykryj_silnik_z_sql(sql)

    for attempt in range(max_retries):
        status_box = st.empty()
        if attempt == 0:
            engine_label = "MSSQL" if isinstance(initial_engine, tuple) else "Postgres"
            status_box.info(f"▶️ Próba {attempt + 1}/{max_retries}: odpalam zapytanie na {engine_label}...")
        else:
            status_box.warning(f"🔄 Próba {attempt + 1}/{max_retries}: AI naprawia SQL po błędzie...")

        df, err = odpal_zapytanie_sql(current_sql, force_engine=initial_engine)
        attempts.append({"sql": current_sql, "error": err, "attempt": attempt + 1})

        if err is None:
            status_box.success(f"✅ Sukces na próbie {attempt + 1}")
            return df, None, attempts

        if attempt == max_retries - 1:
            status_box.error(f"❌ Wyczerpano {max_retries} prób")
            return None, err, attempts

        status_box.warning(f"⚠️ Błąd na próbie {attempt + 1}: {err[:200]}")
        with st.spinner("AI analizuje błąd i poprawia SQL..."):
            poprawiony_sql, gen_err = popraw_sql_po_bledzie(
                current_sql, err, pytanie_pl, schema_text, schema_dict,
                initial_engine=initial_engine
            )
            if gen_err or not poprawiony_sql:
                status_box.error(f"❌ Nie udało się poprawić: {gen_err}")
                return None, err, attempts
            current_sql = poprawiony_sql

    return None, "Wyczerpano próby", attempts


# ==========================================
# POBIERANIE WIDOKÓW
# ==========================================
def pobierz_z_bazy(full_query=None, limit=None):
    try:
        from sshtunnel import SSHTunnelForwarder
        import psycopg2
    except ImportError:
        return None, None

    progress = st.progress(0, text="🔒 Otwieram tunel...")
    try:
        with otworz_tunel() as tunnel:
            progress.progress(30, text="✅ Tunel otwarty")
            if full_query:
                db_name, schema, view = full_query
                base_query = f'SELECT * FROM "{schema}"."{view}"'
                if limit:
                    base_query += f" LIMIT {limit}"
                attempts = [(db_name, base_query)]
            else:
                return None, None

            for db_name, query in attempts:
                progress.progress(60, text=f"📊 Odpytuję {db_name}...")
                try:
                    conn = pg_connect(tunnel, db_name)
                    df = pd.read_sql(query, conn)
                    conn.close()
                    if not df.empty:
                        progress.progress(100, text=f"✅ {len(df)} wierszy")
                        progress.empty()
                        view_name = query.split('"')[-2] if '"' in query else "?"
                        meta = {
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "source_view": view_name, "source_db": db_name,
                            "source_query": query, "row_count": len(df),
                            "columns": list(df.columns),
                        }
                        return df, meta
                except Exception as e:
                    st.error(f"❌ {str(e)[:200]}")
                    continue
            progress.empty()
            return None, None
    except Exception as e:
        progress.empty()
        st.error(f"❌ Tunel: {e}")
        return None, None


def pobierz_wszystkie_widoki(widoki_lista, limit=None):
    try:
        from sshtunnel import SSHTunnelForwarder
        import psycopg2
    except ImportError:
        return

    wyniki = []
    progress_main = st.progress(0, text=f"📥 {len(widoki_lista)} widoków...")

    try:
        with otworz_tunel() as tunnel:
            st.success("✅ Tunel otwarty")
            for i, (db_name, schema, view) in enumerate(widoki_lista):
                status = st.empty()
                status.info(f"📊 [{i+1}/{len(widoki_lista)}] {db_name}.{schema}.{view}")
                try:
                    query = f'SELECT * FROM "{schema}"."{view}"'
                    if limit:
                        query += f" LIMIT {limit}"
                    conn = pg_connect(tunnel, db_name)
                    df = pd.read_sql(query, conn)
                    conn.close()
                    if not df.empty:
                        meta = {
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "source_view": view, "source_db": db_name,
                            "source_query": query, "row_count": len(df),
                            "columns": list(df.columns),
                        }
                        zapisz_do_firestore(df, meta, snapshot_id=f"snapshot_{view}_{datetime.now().strftime('%Y%m%d_%H%M')}")
                        wyniki.append((view, len(df), "OK"))
                        status.success(f"✅ [{i+1}/{len(widoki_lista)}] {view}: {len(df)} wierszy")
                    else:
                        wyniki.append((view, 0, "pusty"))
                except Exception as e:
                    wyniki.append((view, 0, f"błąd: {str(e)[:60]}"))
                    status.error(f"❌ {view}: {str(e)[:60]}")
                progress_main.progress((i + 1) / len(widoki_lista))
    except Exception as e:
        st.error(f"❌ Tunel: {e}")
        return

    progress_main.empty()
    st.success(f"✅ Ukończono {sum(1 for _, _, s in wyniki if s == 'OK')}/{len(wyniki)}")
    st.table(pd.DataFrame(wyniki, columns=["Widok", "Wiersze", "Status"]))


# ==========================================
# FIRESTORE
# ==========================================
CHUNK_SIZE = 300

def df_to_records(df):
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            if pd.isna(val):
                record[col] = None
            elif isinstance(val, Decimal):
                record[col] = float(val)
            elif hasattr(val, 'isoformat'):
                record[col] = val.isoformat()
            else:
                try:
                    record[col] = float(val) if isinstance(val, (int, float)) else str(val)
                except:
                    record[col] = str(val)
        records.append(record)
    return records


def zapisz_do_firestore(df, meta, snapshot_id=None):
    import time
    from google.api_core import exceptions as gcp_exceptions

    records = df_to_records(df)
    num_chunks = (len(records) + CHUNK_SIZE - 1) // CHUNK_SIZE
    meta["num_chunks"] = num_chunks
    meta["chunk_size"] = CHUNK_SIZE

    snap_id = snapshot_id or f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M')}"
    progress = st.progress(0, text=f"💾 {len(records)} w {num_chunks} częściach...")

    try:
        old = db.collection("rentownosc_raporty").document("latest").collection("chunks").list_documents()
        for doc in old:
            doc.delete()
    except:
        pass

    def try_write(ref, data, desc=""):
        for attempt in range(3):
            try:
                ref.set(data)
                return True
            except gcp_exceptions.DeadlineExceeded:
                time.sleep(3)
            except Exception as e:
                st.warning(f"{desc}: {e}")
                return False
        return False

    try_write(db.collection("rentownosc_raporty").document("latest"), {"meta": meta}, "meta")
    for i in range(num_chunks):
        chunk = records[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
        ref = db.collection("rentownosc_raporty").document("latest").collection("chunks").document(f"chunk_{i:04d}")
        try_write(ref, {"data": chunk, "index": i, "size": len(chunk)}, f"chunk {i}")
        progress.progress((i + 1) / num_chunks, text=f"💾 {i+1}/{num_chunks}")

    try:
        try_write(db.collection("rentownosc_raporty").document(snap_id), {"meta": meta}, "snap")
        for i in range(num_chunks):
            chunk = records[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
            ref = db.collection("rentownosc_raporty").document(snap_id).collection("chunks").document(f"chunk_{i:04d}")
            try_write(ref, {"data": chunk, "index": i, "size": len(chunk)}, f"snap {i}")
    except:
        pass

    progress.empty()

    try:
        all_docs = db.collection("rentownosc_raporty").list_documents()
        snapshots = sorted([d.id for d in all_docs if d.id.startswith("snapshot_")], reverse=True)
        for old_id in snapshots[30:]:
            old_chunks = db.collection("rentownosc_raporty").document(old_id).collection("chunks").list_documents()
            for doc in old_chunks:
                doc.delete()
            db.collection("rentownosc_raporty").document(old_id).delete()
    except:
        pass


def wyczysc_firestore():
    all_docs = list(db.collection("rentownosc_raporty").list_documents())
    total = len(all_docs)
    if total == 0:
        st.info("Kolekcja pusta.")
        return
    progress = st.progress(0, text=f"Usuwam {total}...")
    deleted = 0
    for i, doc in enumerate(all_docs):
        try:
            chunks = list(doc.collection("chunks").list_documents())
            for chunk in chunks:
                chunk.delete()
            doc.delete()
            deleted += 1
        except Exception as e:
            st.warning(f"{e}")
        progress.progress((i + 1) / total)
    progress.empty()
    st.success(f"✅ Usunięto {deleted}")
    st.cache_data.clear()


@st.cache_data(ttl=300)
def pobierz_z_firestore(doc_id="latest"):
    doc = db.collection("rentownosc_raporty").document(doc_id).get()
    if not doc.exists:
        return None, None
    payload = doc.to_dict()
    meta = payload.get("meta", {})
    records = payload.get("data", [])

    if not records:
        chunks_ref = db.collection("rentownosc_raporty").document(doc_id).collection("chunks")
        chunks = sorted(chunks_ref.stream(), key=lambda d: d.id)
        for chunk_doc in chunks:
            records.extend(chunk_doc.to_dict().get("data", []))

    if not records:
        return None, meta

    df = pd.DataFrame(records)
    tekst_cols = {"TworcaMaggo", "NrPartii", "TypRap", "NazwaSerwera", "Nazwa",
                  "prokwident", "grupaIgo", "DataOd", "DataDo", "DataProduktu",
                  "IndeksMag", "IndeksSkladowy", "protNumer", "zlecprNumer"}

    for col in df.columns:
        if col in tekst_cols:
            continue
        sample = df[col].dropna().head(100)
        if sample.empty:
            continue
        try:
            converted = pd.to_numeric(sample, errors="coerce")
            success_ratio = converted.notna().sum() / len(sample)
            if success_ratio >= 0.7:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        except:
            pass

    for col in df.columns:
        if "data" in col.lower():
            try:
                df[col + "_date"] = pd.to_datetime(df[col], errors="coerce")
            except:
                pass
    return df, meta


def find_col(df, *candidates):
    df_cols_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in df_cols_lower:
            return df_cols_lower[c.lower()]
    return None


def detect_view_type(df, meta):
    view_name = (meta.get("source_view") or "").lower()
    cols_lower = {c.lower() for c in df.columns}
    types = []
    if "grupaigo" in cols_lower and ("ebay" in cols_lower or "shopeu" in cols_lower):
        types.append("rentownosc")
    if "pkttworc" in view_name or any("punkty" in c.lower() for c in df.columns):
        types.append("punkty_tworcow")
    if "kolektor" in view_name:
        types.append("kolektorka")
    if "tworcamaggo" in cols_lower or "tworca_maggo" in cols_lower:
        types.append("produkcja")
    if "typrap" in cols_lower:
        types.append("z_reklamacjami")
    return types


def layout_base(title, height=500):
    return dict(
        title=title, height=height, template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(b=120, l=60, r=30, t=60),
        xaxis_tickangle=-45,
        font=dict(family="Segoe UI, sans-serif", size=12),
    )


# ==========================================
# WYKRESY (13 gotowych)
# ==========================================
def wykres_ranking_tworcow_sztuki(df, top_n):
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    if not col_tworca:
        return
    agg = df.groupby(col_tworca, dropna=True).size().reset_index(name="Sztuki")
    agg = agg.sort_values("Sztuki", ascending=False).head(top_n)
    agg[col_tworca] = agg[col_tworca].astype(str)
    fig = go.Figure(go.Bar(x=agg[col_tworca], y=agg["Sztuki"], marker_color="#3B82F6"))
    fig.update_layout(**layout_base(f"👷 Ranking twórców (Top {top_n})"), yaxis_title="Sztuki")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_top_produkty(df, top_n):
    col_nazwa = find_col(df, "Nazwa", "nazwa_produktu", "artNazwa", "IndeksMag")
    if not col_nazwa:
        return
    agg = df.groupby(col_nazwa, dropna=True).size().reset_index(name="Sztuki")
    agg = agg.sort_values("Sztuki", ascending=False).head(top_n)
    agg[col_nazwa] = agg[col_nazwa].astype(str)
    fig = go.Figure(go.Bar(x=agg[col_nazwa], y=agg["Sztuki"], marker_color="#10B981"))
    fig.update_layout(**layout_base(f"📦 Top produkty (Top {top_n})"), yaxis_title="Liczba")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_nowa_vs_reklamacja(df):
    col_typrap = find_col(df, "TypRap")
    if not col_typrap:
        return
    agg = df.groupby(col_typrap, dropna=True).size().reset_index(name="Liczba")
    agg[col_typrap] = agg[col_typrap].astype(str)
    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure(go.Pie(labels=agg[col_typrap], values=agg["Liczba"], hole=0.4,
                              marker=dict(colors=["#10B981", "#EF4444", "#F59E0B"])))
        fig.update_layout(title="🔄 Nowa vs Reklamacja", height=400,
                         legend=dict(orientation="h", y=-0.1))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig2 = go.Figure(go.Bar(x=agg[col_typrap], y=agg["Liczba"],
                               marker_color=["#10B981" if "now" in str(v).lower() else "#EF4444" for v in agg[col_typrap]]))
        fig2.update_layout(**layout_base("Rozkład", 400))
        st.plotly_chart(fig2, use_container_width=True)
    with st.expander("Dane"):
        agg["Udział %"] = (agg["Liczba"] / agg["Liczba"].sum() * 100).round(2)
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_produkcja_w_czasie(df):
    col_data = find_col(df, "DataProduktu_date", "DataProduktu", "dataTwZlecProd", "pakDataWyd")
    if not col_data:
        return
    df_time = df.copy()
    df_time[col_data] = pd.to_datetime(df_time[col_data], errors="coerce")
    df_time = df_time.dropna(subset=[col_data])
    if df_time.empty:
        return
    okres = st.radio("Grupowanie:", ["Dzień", "Tydzień", "Miesiąc"], horizontal=True, key="okres_czasu")
    freq = {"Dzień": "D", "Tydzień": "W", "Miesiąc": "M"}[okres]
    df_time["_p"] = df_time[col_data].dt.to_period(freq).dt.to_timestamp()
    agg = df_time.groupby("_p").size().reset_index(name="Sztuki")
    fig = go.Figure(go.Scatter(x=agg["_p"], y=agg["Sztuki"], mode="lines+markers",
                              fill="tozeroy", marker=dict(size=6, color="#6366F1"), line=dict(color="#6366F1")))
    fig.update_layout(**layout_base(f"📅 Produkcja w czasie ({okres})"), yaxis_title="Sztuki")
    st.plotly_chart(fig, use_container_width=True)


def wykres_heatmapa_tworcy_produkty(df, top_n):
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    col_nazwa = find_col(df, "Nazwa", "IndeksMag")
    if not col_tworca or not col_nazwa:
        return
    top_tworcy = df[col_tworca].value_counts().head(min(top_n, 20)).index
    top_prod = df[col_nazwa].value_counts().head(min(top_n, 20)).index
    df_f = df[df[col_tworca].isin(top_tworcy) & df[col_nazwa].isin(top_prod)]
    pivot = df_f.groupby([col_tworca, col_nazwa]).size().unstack(fill_value=0)
    fig = go.Figure(go.Heatmap(z=pivot.values, x=[str(x) for x in pivot.columns],
                              y=[str(y) for y in pivot.index], colorscale="Blues",
                              text=pivot.values, texttemplate="%{text}", textfont=dict(size=10)))
    fig.update_layout(**layout_base("🏢 Twórcy × Produkty", 600))
    st.plotly_chart(fig, use_container_width=True)


def wykres_top_prokwident(df, top_n):
    col = find_col(df, "prokwident")
    if not col:
        return
    agg = df.groupby(col, dropna=True).size().reset_index(name="Liczba")
    agg = agg.sort_values("Liczba", ascending=False).head(top_n)
    agg[col] = agg[col].astype(str)
    fig = go.Figure(go.Bar(x=agg[col], y=agg["Liczba"], marker_color="#8B5CF6"))
    fig.update_layout(**layout_base(f"💪 Top prokwident (Top {top_n})"))
    st.plotly_chart(fig, use_container_width=True)


def wykres_ranking_zarobkow(df, top_n):
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    col_punkty = find_col(df, "PunktyProd", "Maggo_razem", "SumPunktyProd", "PunktyMaggo")
    if not col_tworca or not col_punkty:
        st.warning("Brak twórcy lub punktów")
        return
    agg = df.groupby(col_tworca, dropna=True)[col_punkty].sum().reset_index()
    agg = agg.sort_values(col_punkty, ascending=False).head(top_n)
    agg[col_tworca] = agg[col_tworca].astype(str)
    fig = go.Figure(go.Bar(x=agg[col_tworca], y=agg[col_punkty], marker_color="#F59E0B"))
    fig.update_layout(**layout_base(f"💰 Ranking — suma {col_punkty} (Top {top_n})"))
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_ranking_reklamacji(df, top_n):
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    col_typrap = find_col(df, "TypRap")
    if not col_tworca:
        return
    if col_typrap:
        df_rekla = df[df[col_typrap].astype(str).str.lower().str.contains("rekla", na=False)]
        if df_rekla.empty:
            st.info("Brak reklamacji")
            return
        agg = df_rekla.groupby(col_tworca, dropna=True).size().reset_index(name="Reklamacje")
    else:
        col_rekla = find_col(df, "IleReklamacji", "IleProdRekla", "SumPunktyProdRekla")
        if not col_rekla:
            return
        agg = df.groupby(col_tworca, dropna=True)[col_rekla].sum().reset_index()
        agg = agg.rename(columns={col_rekla: "Reklamacje"})
    agg = agg.sort_values("Reklamacje", ascending=False).head(top_n)
    agg[col_tworca] = agg[col_tworca].astype(str)
    fig = go.Figure(go.Bar(x=agg[col_tworca], y=agg["Reklamacje"], marker_color="#EF4444"))
    fig.update_layout(**layout_base(f"⚠️ Reklamacje (Top {top_n})"))
    st.plotly_chart(fig, use_container_width=True)


def wykres_stosunek_nowa_rekla(df, top_n):
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    col_typrap = find_col(df, "TypRap")
    if not col_tworca or not col_typrap:
        return
    pivot = df.groupby([col_tworca, col_typrap]).size().unstack(fill_value=0)
    if pivot.empty:
        return
    nowa_col = next((c for c in pivot.columns if "now" in str(c).lower() and "rekla" not in str(c).lower()), None)
    rekla_col = next((c for c in pivot.columns if "rekla" in str(c).lower()), None)
    if not nowa_col:
        return
    pivot["Razem"] = pivot.sum(axis=1)
    if rekla_col:
        pivot["% reklamacji"] = (pivot[rekla_col] / pivot["Razem"] * 100).round(2)
    pivot = pivot.sort_values("Razem", ascending=False).head(top_n).reset_index()
    pivot[col_tworca] = pivot[col_tworca].astype(str)
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Nowa", x=pivot[col_tworca], y=pivot[nowa_col], marker_color="#10B981"))
    if rekla_col:
        fig.add_trace(go.Bar(name="Reklamacja", x=pivot[col_tworca], y=pivot[rekla_col], marker_color="#EF4444"))
    fig.update_layout(**layout_base(f"📊 Nowa vs Reklamacja per twórca"), barmode="stack")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane"):
        st.dataframe(pivot, use_container_width=True, hide_index=True)


def wykres_top_grupy_igorowe(df, top_n):
    col = find_col(df, "grupaIgo", "grupa_agg", "grupa")
    if not col:
        return
    agg = df.groupby(col, dropna=True).size().reset_index(name="Liczba")
    agg = agg.sort_values("Liczba", ascending=False).head(top_n)
    agg[col] = agg[col].astype(str)
    fig = go.Figure(go.Bar(x=agg[col], y=agg["Liczba"], marker_color="#EC4899"))
    fig.update_layout(**layout_base(f"🛒 Top grupy igorowe"))
    st.plotly_chart(fig, use_container_width=True)


def wykres_ranking_kolektorowcow(df, top_n):
    col = find_col(df, "Uzytkownik", "user", "UserKoleki", "TworcaKolektora", "TworcaMaggo")
    if not col:
        return
    agg = df.groupby(col, dropna=True).size().reset_index(name="Liczba")
    agg = agg.sort_values("Liczba", ascending=False).head(top_n)
    agg[col] = agg[col].astype(str)
    fig = go.Figure(go.Bar(x=agg[col], y=agg["Liczba"], marker_color="#0EA5E9"))
    fig.update_layout(**layout_base(f"👥 Ranking kolektorowców"))
    st.plotly_chart(fig, use_container_width=True)


def wykres_wartosc_skupow_czasie(df):
    col_data = find_col(df, "DataProduktu_date", "DataProduktu", "przDatePrzyjecia", "dataSkupu", "Data")
    col_wartosc = find_col(df, "Wartosc", "CenaLaczna", "przKwotaLacznaTransport", "przICenaZaSztuke")
    if not col_data or not col_wartosc:
        return
    df_time = df.copy()
    df_time[col_data] = pd.to_datetime(df_time[col_data], errors="coerce")
    df_time[col_wartosc] = pd.to_numeric(df_time[col_wartosc], errors="coerce")
    df_time = df_time.dropna(subset=[col_data, col_wartosc])
    if df_time.empty:
        return
    df_time["_p"] = df_time[col_data].dt.to_period("W").dt.to_timestamp()
    agg = df_time.groupby("_p")[col_wartosc].sum().reset_index()
    fig = go.Figure(go.Scatter(x=agg["_p"], y=agg[col_wartosc], mode="lines+markers",
                              fill="tozeroy", marker=dict(size=6, color="#F97316")))
    fig.update_layout(**layout_base("📈 Wartość skupów"))
    st.plotly_chart(fig, use_container_width=True)


def wykres_pie_udzial(df, top_n):
    non_num = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c]) and not c.endswith("_date")]
    if not non_num:
        return
    col = st.selectbox("Kolumna:", non_num, key="pie_col")
    agg = df.groupby(col, dropna=True).size().reset_index(name="Liczba")
    agg = agg.sort_values("Liczba", ascending=False)
    if len(agg) > top_n:
        top = agg.head(top_n)
        inne = agg.tail(len(agg) - top_n)["Liczba"].sum()
        top = pd.concat([top, pd.DataFrame({col: ["Inne"], "Liczba": [inne]})], ignore_index=True)
        agg = top
    agg[col] = agg[col].astype(str)
    fig = go.Figure(go.Pie(labels=agg[col], values=agg["Liczba"], hole=0.3,
                          textposition="inside", textinfo="percent+label"))
    fig.update_layout(title=f"🥧 Udział: {col}", height=500)
    st.plotly_chart(fig, use_container_width=True)


# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.title("📊 Rentowność DSG")
    st.markdown("---")

    st.subheader("🔍 Diagnostyka")
    diag_cached, diag_time = wczytaj_diagnostyke_z_firestore()
    if diag_cached and "diagnostyka_wynik" not in st.session_state:
        st.session_state["diagnostyka_wynik"] = diag_cached

    if diag_time:
        st.caption(f"📌 Cache: {diag_time[:16]}")

    if st.button("Zbadaj strukturę bazy"):
        st.session_state["diagnostyka_wynik"] = diagnostyka()

    st.markdown("---")
    st.subheader("🔄 Pobieranie danych")

    diag = st.session_state.get("diagnostyka_wynik")
    custom_source = None
    wszystkie_widoki_lista = []
    if diag and diag.get("widoki_rentownosci"):
        for db_name, widoki in diag["widoki_rentownosci"].items():
            for schema, view in widoki:
                wszystkie_widoki_lista.append((db_name, schema, view))
        if wszystkie_widoki_lista:
            opcje = [f"{db}.{sch}.{v}" for db, sch, v in wszystkie_widoki_lista]
            wybor = st.selectbox("Widok:", opcje, key="wybor_widoku")
            idx = opcje.index(wybor)
            custom_source = wszystkie_widoki_lista[idx]

    limit_wierszy = st.number_input("Limit wierszy (0 = wszystkie):", value=0, min_value=0, step=1000)

    now = datetime.now()
    is_working_hours = 7 <= now.hour <= 21
    if is_working_hours:
        st.warning(f"⚠️ Godziny produkcyjne ({now.strftime('%H:%M')})")
        can_refresh = st.checkbox("Rozumiem")
    else:
        st.success(f"🌙 Pora nocna ({now.strftime('%H:%M')})")
        can_refresh = True

    c_btn1, c_btn2 = st.columns(2)
    if c_btn1.button("⚡ Jeden", type="primary", disabled=not can_refresh, use_container_width=True):
        with st.spinner("Pobieram..."):
            lim = limit_wierszy if limit_wierszy > 0 else None
            df_new, meta_new = pobierz_z_bazy(full_query=custom_source, limit=lim)
            if df_new is not None:
                try:
                    zapisz_do_firestore(df_new, meta_new)
                    st.cache_data.clear()
                    st.success(f"✅ {meta_new['row_count']} wierszy")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

    if c_btn2.button("⚡⚡ Wszystkie", disabled=not can_refresh or not wszystkie_widoki_lista, use_container_width=True):
        if wszystkie_widoki_lista:
            st.warning(f"⏳ {len(wszystkie_widoki_lista)} widoków")
            lim = limit_wierszy if limit_wierszy > 0 else None
            pobierz_wszystkie_widoki(wszystkie_widoki_lista, limit=lim)
            st.cache_data.clear()
            st.rerun()

    st.markdown("---")
    st.subheader("📅 Snapshoty")
    all_docs = list(db.collection("rentownosc_raporty").list_documents())
    snapshot_ids = sorted([d.id for d in all_docs if not d.id.startswith("_")], reverse=True)

    selected_snapshot = None
    if not snapshot_ids:
        st.info("Brak snapshotów.")
    else:
        display_names = {}
        for sid in snapshot_ids:
            if sid == "latest":
                display_names[sid] = "📌 Najnowsze"
            else:
                parts = sid.replace("snapshot_", "")
                if len(parts) >= 13 and parts[:4].isdigit():
                    display_names[sid] = f"📷 {parts[:4]}-{parts[4:6]}-{parts[6:8]} {parts[9:11]}:{parts[11:13]}"
                else:
                    display_names[sid] = f"📷 {parts[:50]}"
        selected_snapshot = st.selectbox("Snapshot:", snapshot_ids, format_func=lambda x: display_names.get(x, x))

    st.markdown("---")
    st.subheader("🔧 Filtry")
    top_n = st.slider("Top N:", 5, 100, 30)

    st.markdown("---")
    st.subheader("🗑️ Firestore")
    all_fs_docs = list(db.collection("rentownosc_raporty").list_documents())
    st.caption(f"Dokumentów: {len(all_fs_docs)}")
    if "confirm_clean" not in st.session_state:
        st.session_state["confirm_clean"] = False
    if not st.session_state["confirm_clean"]:
        if st.button("🗑️ Wyczyść Firestore"):
            st.session_state["confirm_clean"] = True
            st.rerun()
    else:
        st.warning("⚠️ Usunie WSZYSTKO!")
        c1, c2 = st.columns(2)
        if c1.button("✅ TAK", type="primary"):
            wyczysc_firestore()
            st.session_state["confirm_clean"] = False
            st.rerun()
        if c2.button("❌ Nie"):
            st.session_state["confirm_clean"] = False
            st.rerun()


# ==========================================
# GŁÓWNY INTERFEJS — TABS
# ==========================================
tab_dashboard, tab_ai = st.tabs(["📊 Dashboard", "🤖 Zapytanie AI"])

# ============ TAB: DASHBOARD ============
with tab_dashboard:
    if st.session_state.get("diagnostyka_wynik"):
        diag = st.session_state["diagnostyka_wynik"]
        with st.expander("🔍 Wynik diagnostyki", expanded=False):
            if diag.get("bazy"):
                st.write(f"**Bazy:** {', '.join(diag['bazy'])}")
            if diag.get("widoki_rentownosci"):
                st.write("**Widoki:**")
                for db_name, widoki in diag["widoki_rentownosci"].items():
                    for schema, view in widoki:
                        st.code(f"{db_name}.{schema}.{view}", language=None)

    if not snapshot_ids or selected_snapshot is None:
        st.info("👈 Pobierz dane z bazy")
    else:
        with st.spinner("Ładowanie..."):
            df, meta = pobierz_z_firestore(selected_snapshot)

        if df is None:
            st.warning("⚠️ Pusty snapshot")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Wierszy", meta.get("row_count", len(df)))
            c2.metric("Źródło", meta.get("source_view", "?"))
            c3.metric("Baza", meta.get("source_db", "?"))
            c4.metric("Aktualizacja", meta.get("updated_at", "?")[:16])

            with st.expander(f"📋 Struktura ({len(df.columns)} kolumn)", expanded=False):
                col_info = []
                for col in df.columns:
                    if col.endswith("_date"):
                        continue
                    dtype = str(df[col].dtype)
                    non_null = df[col].notna().sum()
                    sample_val = df[col].dropna().head(1)
                    sample = str(sample_val.iloc[0])[:60] if len(sample_val) > 0 else "—"
                    col_info.append({"Kolumna": col, "Typ": dtype, "Nie-null": non_null, "Przykład": sample})
                st.dataframe(pd.DataFrame(col_info), use_container_width=True, hide_index=True)

            view_types = detect_view_type(df, meta)
            st.markdown("---")
            st.header("📊 Gotowe wykresy")
            st.caption(f"Typy: **{', '.join(view_types) if view_types else 'uniwersalne'}**")

            WYKRESY = [
                ("w1", "👷 Ranking — sztuki", "produkcja", lambda: wykres_ranking_tworcow_sztuki(df, top_n)),
                ("w2", "📦 Top produkty", "produkcja", lambda: wykres_top_produkty(df, top_n)),
                ("w3", "🔄 Nowa vs Rekla", "z_reklamacjami", lambda: wykres_nowa_vs_reklamacja(df)),
                ("w4", "📅 Produkcja w czasie", None, lambda: wykres_produkcja_w_czasie(df)),
                ("w5", "🏢 Twórcy × Produkty", "produkcja", lambda: wykres_heatmapa_tworcy_produkty(df, top_n)),
                ("w6", "💪 Top prokwident", None, lambda: wykres_top_prokwident(df, top_n)),
                ("w7", "💰 Zarobki", "punkty_tworcow", lambda: wykres_ranking_zarobkow(df, top_n)),
                ("w8", "⚠️ Reklamacje", None, lambda: wykres_ranking_reklamacji(df, top_n)),
                ("w9", "📊 Stosunek nowa/rekla", "z_reklamacjami", lambda: wykres_stosunek_nowa_rekla(df, top_n)),
                ("w10", "🛒 Top grupy", None, lambda: wykres_top_grupy_igorowe(df, top_n)),
                ("w11", "👥 Kolektorowcy", "kolektorka", lambda: wykres_ranking_kolektorowcow(df, top_n)),
                ("w12", "📈 Wartość skupów", "kolektorka", lambda: wykres_wartosc_skupow_czasie(df)),
                ("w15", "🥧 Pie chart", None, lambda: wykres_pie_udzial(df, top_n)),
            ]
            available = [(w, l, f) for w, l, t, f in WYKRESY if t is None or t in view_types]

            if available:
                st.caption(f"✨ Dostępnych: **{len(available)}**")
                if "aktywne_wykresy" not in st.session_state:
                    st.session_state["aktywne_wykresy"] = set()
                cols = st.columns(3)
                for i, (wid, label, func) in enumerate(available):
                    col = cols[i % 3]
                    is_active = wid in st.session_state["aktywne_wykresy"]
                    btn_type = "primary" if is_active else "secondary"
                    if col.button(label, key=f"btn_{wid}", type=btn_type, use_container_width=True):
                        if is_active:
                            st.session_state["aktywne_wykresy"].discard(wid)
                        else:
                            st.session_state["aktywne_wykresy"].add(wid)
                        st.rerun()
                c1, c2 = st.columns(2)
                if c1.button("Pokaż wszystkie", use_container_width=True):
                    st.session_state["aktywne_wykresy"] = {w for w, _, _ in available}
                    st.rerun()
                if c2.button("Ukryj wszystkie", use_container_width=True):
                    st.session_state["aktywne_wykresy"] = set()
                    st.rerun()
                st.markdown("---")
                for wid, label, func in available:
                    if wid in st.session_state["aktywne_wykresy"]:
                        st.subheader(label)
                        try:
                            func()
                        except Exception as e:
                            st.error(f"Błąd: {e}")
                        st.markdown("---")

            st.header("📊 Szybka agregacja")
            numeric_cols = [c for c in df.select_dtypes(include="number").columns if not c.endswith("_date")]
            text_cols = [c for c in df.columns if c not in numeric_cols and not c.endswith("_date")]
            if numeric_cols and text_cols:
                c1, c2, c3 = st.columns(3)
                with c1:
                    group_col = st.selectbox("Grupuj:", text_cols, key="qa_group")
                with c2:
                    agg_col = st.selectbox("Agreguj:", numeric_cols, key="qa_agg")
                with c3:
                    agg_func = st.selectbox("Funkcja:", ["sum", "mean", "count", "max", "min"], key="qa_func")
                try:
                    agg_result = df.groupby(group_col, dropna=True)[agg_col].agg(agg_func).reset_index()
                    agg_result = agg_result.sort_values(agg_col, ascending=False).head(top_n)
                    agg_result[group_col] = agg_result[group_col].astype(str)
                    fig = go.Figure(go.Bar(x=agg_result[group_col], y=agg_result[agg_col], marker_color="#3B82F6"))
                    fig.update_layout(**layout_base(f"{agg_func.upper()}({agg_col}) per {group_col}"))
                    st.plotly_chart(fig, use_container_width=True)
                    with st.expander("Dane"):
                        st.dataframe(agg_result, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"{e}")


# ============ TAB: ZAPYTANIE AI ============
with tab_ai:
    st.header("🤖 Zapytanie do bazy w języku naturalnym")
    st.caption("Opisz po polsku co chcesz zobaczyć. AI wygeneruje SQL, pokaże ci go do zatwierdzenia, potem odpali.")

    # Sprawdź Vertex AI
    project_id, err = init_vertex_ai()
    if err:
        st.error(f"❌ {err}")
        st.info("Sprawdź czy `GCP_PROJECT_IDS`, `GCP_LOCATION` i `FIREBASE_CREDS` są w secrets.")
        st.stop()

    st.success(f"✅ Vertex AI gotowe (projekt: {project_id})")

    # === AUTOLOAD schematu MSSQL z Firestore (jeśli nie ma w pamięci sesji) ===
    if "_schema_mssql" not in st.session_state:
        with st.spinner("⛰️ Wczytuję schemat MSSQL z Firestore..."):
            cached_schema = wczytaj_schema_mssql_firestore()
            if cached_schema:
                st.session_state["_schema_mssql"] = cached_schema
                # Pokaż datę zapisania
                try:
                    meta = db.collection("schema_mssql_cache").document("_meta").get()
                    if meta.exists:
                        saved_at = meta.to_dict().get("saved_at", "?")[:16].replace("T", " ")
                        unique_dbs = set(v.get("database", "?") for v in cached_schema.values())
                        st.info(f"⛰️ Schemat MSSQL wczytany z Firestore: {len(cached_schema)} obiektów z {len(unique_dbs)} baz (zapisany: {saved_at} UTC). Kliknij '🔵 Zbadaj MSSQL' żeby odświeżyć.")
                except Exception:
                    pass

    schema_col1, schema_col2, schema_col3, schema_col4 = st.columns([2, 1, 1, 1])
    with schema_col1:
        st.markdown("**Schemat bazy:** apka musi znać tabele żeby AI dobrze generowało SQL.")
    with schema_col2:
        if st.button("🔄 Odśwież schemat", help="Czyści cache w pamięci. Schemat z Firestore zostaje. Po kliknięciu kliknij 'Zbadaj MSSQL' żeby pobrać świeży."):
            st.cache_data.clear()
            st.session_state.pop("_schema_mssql", None)
            st.rerun()
    with schema_col3:
        if st.button("🔵 Zbadaj MSSQL"):
            # DIAGNOSTYKA: pokaż ile baz jest w secrets
            databases_in_secrets = st.secrets.get("MSSQL_DATABASES", ["ebayApiDB", "STEEPC", "SHOP_PMG"])
            st.info(f"🔍 Próbuję pobrać schemat z {len(databases_in_secrets)} baz: {', '.join(databases_in_secrets)}")
            with st.spinner(f"Pobieram schemat SQL Server z {len(databases_in_secrets)} baz..."):
                try:
                    s = pobierz_schema_mssql()
                    st.session_state["_schema_mssql"] = s or {}
                    if s:
                        # Policz unikalne bazy w wyniku
                        unique_dbs = set()
                        for k, v in s.items():
                            unique_dbs.add(v.get("database", "?"))
                        st.success(f"✅ SQL Server: {len(s)} obiektów z {len(unique_dbs)} baz")
                        st.info(f"📊 Bazy faktycznie pobrane: {', '.join(sorted(unique_dbs))}")
                        st.success("⛰️ Schemat zapisany w Firestore — przeżyje restart apki!")
                        # Pokaż problemy jeśli były
                        problemy = st.session_state.get("_mssql_problemy_bazy", [])
                        if problemy:
                            with st.expander(f"⚠️ {len(problemy)} baz rzuciło błąd (rozwiń)"):
                                for p in problemy:
                                    st.text(p)
                    else:
                        st.warning("⚠️ Nie znaleziono tabel — spróbuj 🔧 Diagnoza")
                except Exception as e:
                    st.error(f"Błąd: {str(e)[:200]}")
            st.rerun()
    with schema_col4:
        if st.button("🔧 Diagnoza MSSQL"):
            st.session_state["_run_mssql_diag"] = True

    # Diagnostyka MSSQL (rozwijana, z szczegółami)
    if st.session_state.get("_run_mssql_diag"):
        with st.expander("🔧 Wyniki diagnostyki SQL Server (krok po kroku)", expanded=True):
            with st.spinner("Sprawdzam krok po kroku..."):
                kroki = diagnostyka_mssql()
            for k in kroki:
                if "✅" in k["status"]:
                    st.success(f"{k['krok']}: {k['info']}")
                elif "⚠️" in k["status"]:
                    st.warning(f"{k['krok']}: {k['info']}")
                elif "💡" in k["krok"]:
                    st.info(f"{k['krok']}: {k['info']}")
                else:
                    st.error(f"{k['krok']}: {k['info']}")
            if st.button("Zamknij diagnozę"):
                st.session_state["_run_mssql_diag"] = False
                st.rerun()

    schema = pobierz_schema_bazy()
    if not schema:
        st.error("Nie udało się pobrać schematu bazy.")
        st.stop()

    # Pobierz schemat SQL Server - TYLKO NA ŻĄDANIE (bo może padać i blokować apkę)
    schema_mssql = st.session_state.get("_schema_mssql", None)

    if schema_mssql is None:
        st.info("🔵 **SQL Server nie zbadany jeszcze.** Kliknij '🔧 Zbadaj SQL Server' żeby pobrać schemat sprzedażowy.")

    liczba_pg = len(schema)
    liczba_mssql = len(schema_mssql) if schema_mssql else 0

    with st.expander(f"📚 Schemat — 🟢 Postgres: {liczba_pg} | 🔵 MSSQL: {liczba_mssql}", expanded=False):
        # Dynamicznie buduj tytuł zakładki MSSQL z list faktycznie pobranych baz
        if schema_mssql:
            unique_dbs = sorted(set(info.get("database", "?") for info in schema_mssql.values()))
            if len(unique_dbs) <= 5:
                mssql_tab_label = f"🔵 SQL Server ({', '.join(unique_dbs)})"
            else:
                mssql_tab_label = f"🔵 SQL Server ({len(unique_dbs)} baz)"
        else:
            mssql_tab_label = "🔵 SQL Server (nie zbadany)"
        tab_pg, tab_ms = st.tabs(["🟢 PostgreSQL (maggo)", mssql_tab_label])

        with tab_pg:
            for key, info in sorted(schema.items()):
                type_label = "📊 VIEW" if info["type"] == "VIEW" else "📋 TABLE"
                st.caption(f"**{type_label} {key}** — {len(info['columns'])} kolumn")

        with tab_ms:
            if schema_mssql:
                # Grupuj po bazie
                by_db = {}
                for key, info in schema_mssql.items():
                    db = info.get("database", "?")
                    by_db.setdefault(db, []).append((key, info))
                for db_name in sorted(by_db.keys()):
                    st.markdown(f"#### Baza: **{db_name}** ({len(by_db[db_name])} obiektów)")
                    for key, info in sorted(by_db[db_name]):
                        type_label = "📊 VIEW" if info["type"] == "VIEW" else "📋 TABLE"
                        st.caption(f"**{type_label} {key}** — {len(info['columns'])} kolumn")
            else:
                st.info("Nie udało się pobrać schematu SQL Server — może być problem z połączeniem.")

    schema_text = schema_do_tekstu_dla_ai(schema, schema_mssql=schema_mssql)

    # --- Słownik pojęć domenowych ---
    if "slownik_dziedzinowy" not in st.session_state:
        st.session_state["slownik_dziedzinowy"] = DOMYSLNY_SLOWNIK

    with st.expander("🧠 Słownik pojęć domenowych (edytowalny)", expanded=False):
        st.caption("Ten słownik mówi AI jak mapować polskie pojęcia na tabele/kolumny w bazie. "
                  "Możesz go edytować — im lepszy słownik, tym celniejsze zapytania. "
                  "Zapisuje się na czas sesji. Gdy dowiesz się czegoś nowego od Krzyśka, dopisz tutaj.")
        edited_slownik = st.text_area(
            "Słownik:",
            value=st.session_state["slownik_dziedzinowy"],
            height=400,
            key="slownik_edit"
        )
        c1, c2, c3 = st.columns(3)
        if c1.button("💾 Zapisz zmiany w słowniku"):
            st.session_state["slownik_dziedzinowy"] = edited_slownik
            # Opcjonalnie zapisz do Firestore żeby było trwałe
            try:
                db.collection("rentownosc_raporty").document("_slownik_dziedzinowy").set({
                    "tresc": edited_slownik,
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                st.success("✅ Zapisano (trwałe)")
            except Exception as e:
                st.warning(f"Zapisano na czas sesji (Firestore błąd: {e})")
        if c2.button("↩️ Reset do domyślnego"):
            st.session_state["slownik_dziedzinowy"] = DOMYSLNY_SLOWNIK
            st.rerun()
        if c3.button("📥 Wczytaj zapisany"):
            try:
                doc = db.collection("rentownosc_raporty").document("_slownik_dziedzinowy").get()
                if doc.exists:
                    tresc = doc.to_dict().get("tresc", DOMYSLNY_SLOWNIK)
                    st.session_state["slownik_dziedzinowy"] = tresc
                    st.success("✅ Wczytano")
                    st.rerun()
                else:
                    st.info("Brak zapisanego słownika")
            except Exception as e:
                st.error(f"Błąd: {e}")

    # Auto-wczytaj zapisany słownik przy pierwszym wejściu
    if "slownik_loaded" not in st.session_state:
        try:
            doc = db.collection("rentownosc_raporty").document("_slownik_dziedzinowy").get()
            if doc.exists:
                tresc = doc.to_dict().get("tresc", DOMYSLNY_SLOWNIK)
                st.session_state["slownik_dziedzinowy"] = tresc
            st.session_state["slownik_loaded"] = True
        except:
            st.session_state["slownik_loaded"] = True

    pytanie = st.text_area(
        "Twoje pytanie:",
        placeholder="np. Pokaż które skrzynie twórca MM pobrał w marcu 2026 i jakie części były w nich użyte",
        height=100,
        key="ai_pytanie",
    )

    st.caption("💡 **Przykłady:**")
    ex1, ex2, ex3 = st.columns(3)
    if ex1.button("📦 Co produkuje MM", use_container_width=True):
        st.session_state["ai_pytanie_fill"] = "Pokaż produkty które twórca MM produkował najwięcej w ostatnich 30 dniach, sortuj malejąco"
        st.rerun()
    if ex2.button("🔧 Skrzynie i części", use_container_width=True):
        st.session_state["ai_pytanie_fill"] = "Dla 10 ostatnio wyprodukowanych skrzyń pokaż numer partii, twórcę i listę części pobranych z magazynu"
        st.rerun()
    if ex3.button("💸 Top zarobki", use_container_width=True):
        st.session_state["ai_pytanie_fill"] = "Top 10 twórców po punktach produkcji w ostatnim tygodniu"
        st.rerun()

    if "ai_pytanie_fill" in st.session_state:
        pytanie = st.session_state.pop("ai_pytanie_fill")
        st.session_state["ai_pytanie"] = pytanie

    if st.button("🪄 Generuj SQL", type="primary", disabled=not pytanie):
        with st.spinner("AI myśli..."):
            sql, err = wygeneruj_sql_przez_ai(pytanie, schema_text)
            if err:
                st.error(err)
            else:
                st.session_state["ai_sql"] = sql
                st.session_state["ai_sql_pytanie"] = pytanie
                st.rerun()

    if "ai_sql" in st.session_state:
        st.markdown("---")
        st.subheader("📝 Wygenerowany SQL")
        st.caption(f"Dla pytania: *{st.session_state.get('ai_sql_pytanie', '')}*")

        edited_sql = st.text_area("SQL (możesz edytować):", value=st.session_state["ai_sql"],
                                  height=200, key="ai_sql_edit")

        c1, c2, c3 = st.columns(3)
        if c1.button("▶️ Odpal zapytanie", type="primary"):
            if not can_refresh:
                st.error("⚠️ Zaznacz w sidebarze że rozumiesz że odpytujesz produkcję w godzinach pracy.")
            else:
                pytanie_orig = st.session_state.get("ai_sql_pytanie", "")
                # Połącz schematy PG + MSSQL żeby self-healing widział kolumny obu baz
                combined_schema = dict(schema or {})
                if schema_mssql:
                    combined_schema.update(schema_mssql)
                df_ai, err, attempts = odpal_z_self_healing(edited_sql, pytanie_orig, schema_text, schema_dict=combined_schema, max_retries=3)

                # Pokaż historię prób jeśli były jakieś poprawki
                if len(attempts) > 1:
                    with st.expander(f"🔧 Historia prób ({len(attempts)})", expanded=False):
                        for a in attempts:
                            st.caption(f"**Próba {a['attempt']}:**")
                            st.code(a["sql"], language="sql")
                            if a["error"]:
                                st.caption(f"❌ Błąd: {a['error'][:300]}")
                            else:
                                st.caption("✅ OK")

                if err:
                    st.error(f"❌ Ostatecznie nie udało się: {err}")
                elif df_ai is not None:
                    st.session_state["ai_df"] = df_ai
                    # Zaktualizuj SQL na ten który zadziałał
                    if len(attempts) > 1:
                        st.session_state["ai_sql"] = attempts[-1]["sql"]
                        st.info(f"ℹ️ SQL został automatycznie poprawiony ({len(attempts)} prób)")
                    # Ostrzeżenie jeśli 0 wierszy po retry
                    if len(df_ai) == 0 and len(attempts) > 1:
                        st.warning("⚠️ Zapytanie zwróciło 0 wierszy po automatycznej naprawie. "
                                  "AI mogło zmienić strategię i poszukać w nieodpowiednich tabelach. "
                                  "Spójrz w 'Historia prób' powyżej — porównaj pierwszą (błędną) i finalną wersję SQL. "
                                  "Jeśli finalna używa innych tabel niż chciałeś, kliknij 'Regeneruj' albo ręcznie popraw SQL.")
                    else:
                        st.success(f"✅ Pobrano {len(df_ai)} wierszy")

        if c2.button("🔄 Regeneruj"):
            del st.session_state["ai_sql"]
            st.rerun()

        if c3.button("🗑️ Wyczyść"):
            st.session_state.pop("ai_sql", None)
            st.session_state.pop("ai_df", None)
            st.rerun()

    if "ai_df" in st.session_state:
        df_ai = st.session_state["ai_df"]
        st.markdown("---")
        st.subheader(f"📊 Wynik ({len(df_ai)} wierszy)")

        c1, c2 = st.columns(2)
        c1.metric("Wiersze", len(df_ai))
        c2.metric("Kolumny", len(df_ai.columns))

        st.dataframe(df_ai, use_container_width=True)

        numeric_cols = df_ai.select_dtypes(include="number").columns.tolist()
        text_cols = [c for c in df_ai.columns if c not in numeric_cols]

        if numeric_cols and text_cols and len(df_ai) > 1:
            st.markdown("---")
            st.subheader("📊 Szybka wizualizacja")
            c1, c2, c3 = st.columns(3)
            with c1:
                x_col = st.selectbox("X:", text_cols, key="ai_x")
            with c2:
                y_col = st.selectbox("Y:", numeric_cols, key="ai_y")
            with c3:
                chart_type = st.selectbox("Typ:", ["Bar", "Line", "Pie"], key="ai_type")

            try:
                if chart_type == "Bar":
                    fig = go.Figure(go.Bar(x=df_ai[x_col].astype(str), y=df_ai[y_col], marker_color="#3B82F6"))
                elif chart_type == "Line":
                    fig = go.Figure(go.Scatter(x=df_ai[x_col].astype(str), y=df_ai[y_col], mode="lines+markers"))
                else:
                    fig = go.Figure(go.Pie(labels=df_ai[x_col].astype(str), values=df_ai[y_col], hole=0.3))
                fig.update_layout(**layout_base(f"{y_col} per {x_col}" if chart_type != "Pie" else f"Udział {y_col}"))
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.warning(f"Nie udało się: {e}")

        csv = df_ai.to_csv(index=False).encode('utf-8-sig')
        st.download_button("⬇️ Pobierz CSV", csv, f"ai_wynik_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", "text/csv")


st.markdown("---")
st.caption("Dashboard Rentowności DSG | SSH tunel → PostgreSQL → Firestore | AI: Vertex AI")
