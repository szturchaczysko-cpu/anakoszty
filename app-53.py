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

# CSS — wymuszamy czarne czcionki w stylowanych tabelach (st.dataframe ze .style.apply)
st.markdown("""
<style>
/* Stylowane dataframe — zawsze czarne czcionki na kolorowanych tłach */
.stDataFrame [data-testid="stDataFrameResizable"] table tbody tr td {
    color: #000000 !important;
}
.stDataFrame table tbody tr td * {
    color: inherit !important;
}
/* Headery tabel */
.stDataFrame table thead tr th {
    color: #FFFFFF !important;
    background-color: #2C3E50 !important;
}
</style>
""", unsafe_allow_html=True)

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
- **`wmsArtykulySrCenyZakHist`** — historyczna średnia cena zakupu (wyliczana automatycznie z dokumentów przyjęć)
  - `asczhSrCenaZakPo` = aktualna średnia cena zakupu (netto)
  - `asczhSrCenaZakPoWalID` = waluta (26 = PLN)
  - `aschDataTw` = data zapisu (weź najnowszą)
  - `artID` = łączymy z `wmsArtykuly`
  - ⚠️ **UWAGA: ta tabela jest WYPEŁNIANA TYLKO dla artykułów wprowadzanych przez moduł "Przyjęcie - dokumenty"!** Pozycje wprowadzane bezpośrednio (skupy, pakunki itp.) NIE mają tu ceny.
- **`wmsArtykulyKontrahenci`** ⭐⭐⭐ MAIN — **ceny zakupu uzupełniane RĘCZNIE przez Dariusza/zespół zakupowy**
  - `artkCena` = cena netto od tego dostawcy
  - `artkCenaBrutto` = cena z VATem
  - `artkWiodacy` = True jeśli główny dostawca
  - `konID` = ID kontrahenta
  - `artkDataTw` = data wpisu (najnowszy = aktualna cena)
  - **TO JEST GŁÓWNE ŹRÓDŁO CEN dla artykułów które nie idą przez moduł przyjęć**
- **`wmsArtykulyCeny`** — pusta, nie używaj
- 🔴 **REGUŁA AI dla pytań o cenę/wartość artykułu/magazynu:**
  Sprawdzaj OBA źródła: `wmsArtykulyKontrahenci` (ceny ręczne) ORAZ `wmsArtykulySrCenyZakHist` (ceny automatyczne). Użyj COALESCE — jeśli jest cena w "kontrahenci" to ta, w przeciwnym razie ze "średnich". Wzór:
  ```sql
  COALESCE(
    (SELECT MAX("artkCenaBrutto") FROM "public"."wmsArtykulyKontrahenci" WHERE "artID" = a."artID" AND "artkCenaBrutto" > 0),
    (SELECT "asczhSrCenaZakPo" FROM "public"."wmsArtykulySrCenyZakHist" WHERE "artID" = a."artID" ORDER BY "aschDataTw" DESC LIMIT 1)
  ) AS cena_zakupu
  ```

**⚠️ PSEUDO-INDEKSY do pomijania w analizach cen/rentowności:**
W tabeli `wmsArtykuly` są nie tylko realne towary, ale też pseudo-indeksy operacyjne:
- `KALFAS` (~72k szt), `POLPALETA`, `ROZNE` — opakowania zbiorcze, nie wycena per szt
- `Service nach Ablauf 2`, `Service nach Ablauf der G` — niemieckie statusy serwisowe (nie towary!)
- `REG_ELEMENT`, `GET_ELEMENT` — elementy regeneracji (statusy procesowe)
- `towar_obcy` — "Towar nie pochodzący z firmy Autos" (austausche/depozyty klientów)
- `77777` — refaktura zwrotu kosztów (w ogóle nie towar)
- Filtruj: `WHERE artIndeks NOT IN ('KALFAS', 'REG_ELEMENT', 'GET_ELEMENT', 'towar_obcy', 'ROZNE', 'POLPALETA', '77777') AND artNazwa NOT LIKE 'Service nach%' AND artNazwa NOT LIKE 'Refaktura%'`

**Nazewnictwo realnych indeksów części:**
- Prefiks `BMW20_` / `BMW30_` = kolektory BMW 2.0d / 3.0d
- Prefiks `20TDI_` / `27_` = kolektory TDI (2.0 / 2.7-3.0)
- Prefiks `ORG` = oryginalne (kompletne) z producenta
- Prefiks `REG` = regenerowane
- Sufiks `_KORP` = sam korpus, `_NAST` = nastawnik, `_DRUT` = drucik, `_GRUSZ` = gruszka, `_KPL` = komplet
- Sufiks `_USZK` = uszkodzony (cena niższa, np. 30-50% kompletnego)
- `27_ZGBG_SZASZ` = "szaszłyk" 2.7/3.0 TDI (kosz/sleeve)

**Stany magazynowe:**
- **`wmsMagazynMiejsca`** — gdzie fizycznie leży i ile jest
  - `magmIlosc` = ile sztuk w tej lokalizacji
  - `magmIloscRezerw` = ile zarezerwowane
  - `magmRegal`, `magmPoziom`, `magmMiejsce` = lokalizacja fizyczna (R/P/M)
  - `artID` = łączymy z `wmsArtykuly`
- ⚠️ **ZAWSZE przy pytaniach o "stany magazynowe" DOŁĄCZ WARTOŚĆ** — czyli JOIN do `wmsArtykulySrCenyZakHist` i dodaj kolumny "Cena jedn." oraz "Wartość" (ilość × cena). Użytkownik pytając o stany prawie zawsze chce też wiedzieć ile to warte!

**UWAGA:** W tabeli `wmsPakunkiPozycjeHist` NIE MA kolumny z ceną — nie jest zapisywana w momencie pobrania. Koszt pobranej części trzeba wyliczyć jako `ilość × aktualna_średnia_cena_zakupu`.

### 🔔 DZWONEK RENTOWNOŚCI (system kontroli przekroczeń kosztów na produkcji):

Każda skrzynia ma ustawiony **maksymalny próg kosztów części** które tworca może pobrać do regeneracji. Po przekroczeniu progu — **blokada wydania**, kierownik produkcji musi zatwierdzić.

**Tabele:**
- **`wmsArtykulyMaksWartoscZamHist`** ⭐ — historia ustawień progu per skrzynia (artID)
  - `artID` = ID skrzyni (link do `wmsArtykuly`)
  - `amwzhMaksWartoscZam` = **PRÓG kwotowy w PLN** (np. 1500, 2500, 3330)
  - `amwzhSprawdzajMaksWartoscZam` = **TRUE = aktywny dzwonek**, FALSE = wyłączony
  - `amwzhUserTw` = kto ustawił (głównie **dariusz**, czasem **igor**)
  - `amwzhDataTw` = kiedy ustawił (najnowsza data = aktualna konfiguracja)
- **`wmsZleceniaMaksWartoscZamHist`** — analogiczna tabela dla zleceń (per zlecprID, nie per artID)

**REGUŁA: bierz NAJNOWSZY rekord per artID:**
```sql
SELECT DISTINCT ON ("artID") "artID", "amwzhMaksWartoscZam", "amwzhSprawdzajMaksWartoscZam"
FROM "public"."wmsArtykulyMaksWartoscZamHist"
ORDER BY "artID", "amwzhDataTw" DESC
```

**KLUCZOWE FAKTY (audyt 8 maja 2026):**
- 99.9% skrzyń ma ustawiony próg (2 277 z 2 279)
- Średni próg: **904 zł**, Max: **3 330 zł**
- **Dariusz** odpowiada za ustawianie progów
- **PROBLEM**: progi są zdezaktualizowane — 50.3% partii (4 993 z 9 928 w 10mc) **przekraczało próg**
  - 26.8% przekroczyło 1.5× próg
  - **10.6% przekroczyło 2× próg** (1 052 partii — wymagają analizy)
  - **1.0% przekroczyło 3× próg** (104 partie — outliery)
- Dla TOP 30 najdroższych skrzyń **WSZYSTKIE** mają próg niższy od średniego kosztu części!
  - Np. `22620GPGRUP3` koszt śr. 3 798 zł, próg 2 500 zł → dzwonek dzwoni ZAWSZE
  - Skutek: dzwonek stał się **martwym mechanizmem** — kierownik klika "akceptuj" rutynowo
- Sugerowany nowy próg: **1.3× średni koszt 10mc per skrzynia** (cel: zmniejszyć przekroczenia z 50% do 10%)

**Pytania użytkownika typu "dzwonek rentowności / progi / blokady na produkcji" → zawsze idziemy do `wmsArtykulyMaksWartoscZamHist`.**

### 👥 ROLE OPERACYJNE FIRMY (ważne przy interpretacji userów w bazie):

- **igor** — specjalista doboru. Dostaje VIN auta od sprzedawcy, zwraca konkretny indeks skrzyni. Pojawia się w `amwzhUserTw` (ustawia niektóre progi dzwonka), w `zksUserTw` zamówień jako finalizator.
- **dariusz** — odpowiada za **ceny zakupu** (`wmsArtykulyKontrahenci`) ORAZ **progi dzwonka** (`wmsArtykulyMaksWartoscZamHist`). Pracuje na bieżąco — wpisy nawet z dnia bieżącego.
- **lisu** — **magazynier główny**. Pobiera/wydaje części z magazynu do produkcji (`wmsPakunkiHist.pakUserTw` przy `pakRodzaj=3`). NIE jest twórcą skrzyni — szykuje części dla twórcy.
- **mateusz_j** — magazynier zastępczy (jak lisu nieobecny).
- **dawid_l, arek_l** — pomocnicy magazynowi.
- **patrycja_s** — z działu reklamacji. Pojawia się w zamówieniach `zknUserTw` przy `zknTypZam=16` (RMA/zwroty).
- **krzysztof_g (kb)** — IT admin. Pojawia się przy administrowaniu zleceń.
- **czarek, emil, sylwester, daniel, anna** — twórcy/operatorzy (wykonują pracę regeneracji).
- **sylwia** — sprzedawca (zamówienia `zksUserTw`).
- **Foto_Kuba** — pracownik fotografujący/wprowadzający parametry techniczne (`artpdUserTw` w wmsArtykulyParametryDodatkowe).
- **Sys, SYS** — systemowy użytkownik (automatyczne wpisy).

**REGUŁA:** Gdy widzimy pakunek z dwoma magazynierami (lisu+mateusz_j) — to NIE są podwójne pobory, tylko zmiana zmiany lub nieobecność lisa. Nie liczyć tego jako problem.

### 🔧 PARTIA = NUMER (nie pojedyncza skrzynia!):

**WAŻNA pułapka analityczna**: `zadprElementNrPartii` to numer partii produkcyjnej, NIE numer pojedynczej skrzyni. Niektóre partie są:
- **Otwarte przez LATA** (np. WHS otwarta od maja 2023, 56 poborów do dziś — to "partia magazynowa" do reklamacji)
- **Wykorzystywane wielokrotnie** — gdy klient wraca po 22 miesiącach z reklamacją gwarancyjną, dorabia się do tej samej partii
- **Multi-skrzyniowe (RZADKO, 4 z 13 744)** — jedna partia obsługuje 2 typy skrzyń (np. RBT → 195TDI5GRUP1 + GRUP3) — wariantyzacja

**REGUŁA dla SQL liczących koszt partii**: zawsze używaj `DISTINCT pakID` w CTE, inaczej JOIN do `wmsZleceniaProdukcyjnePozycjeHist` może podwoić koszt dla bliźniaków.

### 💱 STRUKTURA SPRZEDAŻY — KORONA RENTOWNOŚCI

**Łańcuch sprzedaży DSG (od VIN do faktury):**
```
1. Klient → VIN → sprzedawca
2. Sprzedawca → wysyła VIN do IGORA (dobór)
3. Igor → zwraca konkretny indeks skrzyni (np. 306M40GRUP1)
4. Sprzedawca finalizuje zamówienie w `scZAMKLINAG` + `scZAMKLISZCZEG`
5. Magazynier (lisu) szykuje części z magazynu (rośnie `wmsPakunkiHist`)
6. Twórca regeneruje skrzynię  
7. Wysyłka + list przewozowy
8. Po dostawie → faktura w `scFAKTNAG` + `scFAKTSZCZEG` (dla DE klientów uproszczona = "GET" / "Getriebe")
```

**Tabele MSSQL kluczowe dla rentowności (w bazie STEEPC):**
- **`scZAMKLINAG`** — nagłówki zamówień klienta
  - `zknZamNr` = numer zamówienia (klucz)
  - `zknKlID` = ID klienta (link do `scKLIFAKT` = adresy fakturowania)
  - `zknDataTw` = data utworzenia
  - `zknTypZam` = typ (1=normal, 16=RMA/reklamacja)
  - `zknfkID` = kategoria fakturowania (smallint, NIE link do faktury!)
  - `zknKliFaktID` = ID adresu fakturowania (link do `scKLIFAKT`)
  - `zknOddzID_WEB` lub `zknProf_WEB` = kod kraju klienta (POL, GER, ENG)
- **`scZAMKLISZCZEG`** ⭐ — pozycje zamówień (TUTAJ SĄ KONKRETNE INDEKSY!)
  - `zksZknZamNr` = link do nagłówka
  - `zksIndex` = indeks **księgowy** (często 'GET' dla niemieckich faktur — "Getriebe")
  - **`zksIndex_WEB`** ⭐⭐⭐ = **realny indeks skrzyni** (np. `306M40GRUP1`, `1922T5`) — ZAWSZE używaj tego pola dla rentowności!
  - `zksItNr_WEB` = link do `swItem` (artikuł w katalogu)
  - `zksCenaBrutto`, `zksCenaNetto` = cena sprzedaży w walucie zamówienia
  - `zksIleZam` = ilość zamówiona
  - `zksUserTw` = sprzedawca (sylwia, igor itd.)
  - `zksPackIndex` = indeks pakietu (gdy skrzynia w zestawie z olejem itp.)
  - `zksTypZam` = 2 dla austauschu (z depozytem starej skrzyni = KAUCJA)
- **`scFAKTNAG`** — faktury księgowe
  - `fnID` = ID faktury
  - `fnFaktNr` = numer (np. `2026/05/T/002767`)
  - `fnFaktLang` = język (PL, DE, ENG) — nie zawsze tożsamy z walutą!
  - `fnwaID` = **kod waluty: 1=PLN, 2=EUR**
  - `fnZapBrutto` = brutto faktury
  - `fnZamKliNr` = numer zamówienia klienta
  - `fnKLiNazwa` = klient
- **`scFAKTSZCZEG`** — pozycje faktury (dla DE często 'GET'/'Getriebe' zbiorczo!)
  - `fsIndex` = "GET" dla niemieckich (99.7% sprzedaży skrzyń DE pod jednym indeksem)
  - `fsCenaBrutto` = cena
  - `fsFnID` = link do nagłówka
  - `fsCzyAustauch` = TRUE dla austauschu (klient daje swoją starą skrzynię)
- **`scKLIFAKT`** — adresy fakturowania klientów (NIE faktury!)
- **`scTypFakt`** — słownik typów faktur

**Pseudo-indeksy w sprzedaży (do filtrowania):**
- `KAUCJA` — depozyt zwrotny przy austauschu (cena 0)
- `TOWAR_WEWN` — towar wewnętrzny
- `99999` — refaktura kosztów (np. wysyłki PMG Technik)
- `GET` / `Getriebe` — uniwersalny indeks skrzyni dla DE (99.7% sprzedaży skrzyń DE)
- `OLEJ_TITAN_75W_30` itp. — oleje dorzucane do zamówień
- Wszystko z `OLEJ%`, `PAK_%` na początku indeksu

**REGUŁA AI dla pytań o sprzedaż/rentowność/marżę per skrzynia:**
1. Cena zawsze z **`scZAMKLISZCZEG.zksIndex_WEB`** (NIE `fsIndex` z faktury — tam jest GET!)
2. Filtruj `zksCzyAustauch=1` lub `zksTypZam=2` dla austauschu
3. Wyklucz pseudo-indeksy: NOT IN ('KAUCJA','TOWAR_WEWN','99999','GET','') AND NOT LIKE 'ORG%' AND NOT LIKE 'REG%' AND NOT LIKE 'OLEJ%' AND NOT LIKE 'PAK_%'
4. Filtruj `zksCenaBrutto > 100` żeby pominąć drobne (uszczelki, oleje)
5. Min `HAVING COUNT(*) >= 10` żeby średnia miała sens

### 💼 KATEGORIE TOWARÓW (z `taNazwa` w widoku VwZknFvKliItemSzczegVer1_KORONA):

- **SKRZYNIE** — kompletne skrzynie biegów (DSG, manualne, automaty). 99.7% sprzedaży leci przez indeks 'GET' w fakturach!
- **KOLEKTORY** — kolektory ssące (BMW20_KORP, ORG30_BMW_SAKS_LEK_KPL itd.). 38 unikalnych typów, dobrze rozróżniane na fakturach.
- **MECHATRONIC** — mechatronika DSG (drogie, krytyczne komponenty).
- **NORMAL** — pozostałe (uszczelki, łożyska, oleje, refaktury).

### 📊 WIDOKI BI W BAZIE RAPDB (**używaj ich dla raportów rentowności!**):

- **`VwZknFvKliItemSzczegVer1_KORONA`** ⭐⭐⭐ — KORONA: zamówienia + faktury + klient + item + szczegóły. Zawiera kolumny: `zknZamNr`, `zksIndex`, `zksIndex_WEB`, `fnFaktNr`, `fnFaktLang`, `zknWaNazwa` (waluta jako tekst!), `klNazwa`, `kaCountry`, `taNazwa`. **TO JEST GŁÓWNY widok do raportów sprzedażowych.**
- `VwZknFvKliItemSzczegVer1` — wcześniejsza wersja, pomijaj na rzecz `_KORONA`.
- `VwFvZamKliItem` / `VwFvZamKliItemVer2` — Faktura + Zamówienie + Klient + Item.
- `VwZknFvUpsZwnVer1` — Zamówienia + Faktury + UPS + Zwroty (cały łańcuch wysyłki).
- `VwSprzedazRapPmgTech` / `VwSprzedazRapPmgTechTransCode` — raporty sprzedaży PMG Technik.
- `Vw_PremTeam_Fv_Main_Ver1` — główny widok dla zespołu sprzedaży (premie + marża).
- `VwAustauschNiezwrocneWszystkieSpolkiVer2` — austausche niezwrócne.
- `VwSkupyPrzyjeteFvKsiegKasetkiKlientVer1` — skupy przyjęte.
- `VwZamModelMarkaZDoboru` — agregacja zamówień per marka/model auta (bez indeksu skrzyni!).
- `VwCsvFvUpsZamZPodzialemArtMaxZZksVer1` — CSV faktur z podziałem artykułów.

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
  - ⚠️ JEDNA FAKTURA = WIELE WIERSZY (po pozycjach) — sumuj prztKwotaBrutto, NIE Rozchod (bo Rozchod się duplikuje)
- `[RAPDB].[dbo].[VwKosztyCsv]` (18 kol) — wersja eksportu CSV
- `[RAPDB].[dbo].[TblKoszty]` (28 kol) — tabela bazowa
- `[RAPDB].[dbo].[TblTagiP1/P2/P3]` — słowniki tagów kosztowych

💰 **PRZELEWY BANKOWE** (faktyczne ruchy gotówki):
- `[RAPDB].[dbo].[TblBankPko]` (52 kol) ⭐⭐⭐ — wyciągi PKO (PLN, EUR, USD)
  - PkoDataOper, PkoTyp ('Przelew z rachunku' = wych., 'Wpływ na rachunek' = przych.)
  - PkoKwota (UJEMNA gdy wychodzące, DODATNIA gdy przychodzące)
  - PkoOpis (pełny opis przelewu z nazwą kontrahenta i nr faktury)
  - PkoIbanRef (IBAN drugiej strony — łączy z TblIbanOpisKontrahent)
  - m_FvZnaleziona (TAK/NIE — czy zmatchowano z fakturą)
  - m_FnNrT1 (numer faktury jeśli zmatchowano)
  - m_FnKliNazwa (nazwa kontrahenta jeśli rozpoznana)
  - Tag, Tag2, Tag3 — kategorie spójne z VwKosztyVer1 (np. Tag2='utrzymanie', Tag3='transportobcy')
  - ⚠️ **PUŁAPKA: PkoDataOper jest typu nvarchar (string)!** Trzeba CAST: `CAST([PkoDataOper] AS date)`
  - ⚠️ FORMAT() na nvarchar daje błąd: `Argument data type nvarchar is invalid` — najpierw CAST!
- `[RAPDB].[dbo].[TblBankBV]` (51 kol) — bank BV
- `[RAPDB].[dbo].[TblBankDB]` / `TblBankDBPl` (51 kol) — bank DB (EUR i PLN)
- `[RAPDB].[dbo].[TblPayPal]` (58 kol), `[TblPayU]` (51 kol) — operatorzy płatności
- `[RAPDB].[dbo].[TblIbanOpisKontrahent]` (26 kol) — mapowanie IBAN ↔ kontrahent (gotowe!)
- `[RAPDB].[dbo].[VwPkoVer1]` (53 kol) — gotowy widok PKO
- `[RAPDB].[dbo].[VwPkoSpkPlnUpsPobraniaZestKsiegowe]` (34 kol) — zestawienie pobrań UPS

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

    # ⭐ Kluczowe widoki MSSQL z RAPDB (najczęściej używane do raportów rentowności)
    KLUCZOWE_MSSQL = {
        "[MSSQL] RAPDB.dbo.VwKosztyVer1",
        "[MSSQL] RAPDB.dbo.VwZknFvUpsZwnVer1",
        "[MSSQL] RAPDB.dbo.VwAustauschNiezwrocneWszystkieSpolkiVer2",
        "[MSSQL] RAPDB.dbo.VwAustauschNiezwrocnePmgTechVer2",
        "[MSSQL] RAPDB.dbo.VwSkupyPrzyjeteFvKsiegKasetkiKlientVer1",
        "[MSSQL] RAPDB.dbo.VwKosztySkupyPrzyjeteKlientVer1",
        "[MSSQL] RAPDB.dbo.Vw_PremTeam_Fv_Main_Ver1",
        "[MSSQL] RAPDB.dbo.VwZknFvKliItemSzczegVer1",
        "[MSSQL] RAPDB.dbo.VwFvZamKliItemVer2",
        "[MSSQL] RAPDB.dbo.VwWstazkiSprzedPodsum",
        "[MSSQL] RAPDB.dbo.TblBankPko",
        "[MSSQL] RAPDB.dbo.TblBankBV",
        "[MSSQL] RAPDB.dbo.TblBankDB",
        "[MSSQL] RAPDB.dbo.TblBankDBPl",
        "[MSSQL] RAPDB.dbo.TblPayPal",
        "[MSSQL] RAPDB.dbo.TblPayU",
        "[MSSQL] RAPDB.dbo.TblIbanOpisKontrahent",
        "[MSSQL] RAPDB.dbo.VwPkoVer1",
        "[MSSQL] RAPDB.dbo.VwPkoSpkPlnUpsPobraniaZestKsiegowe",
        "[MSSQL] RAPDB.dbo.TblKoszty",
        "[MSSQL] RAPDB.dbo.TblTagiP1",
        "[MSSQL] RAPDB.dbo.TblTagiP2",
        "[MSSQL] RAPDB.dbo.TblTagiP3",
        "[MSSQL] STEEPC.dbo.scKLIENT",
        "[MSSQL] STEEPC.dbo.scZAMKLINAG",
        "[MSSQL] STEEPC.dbo.scZAMKLISZCZEG",
        "[MSSQL] STEEPC.dbo.scFAKTNAG",
        "[MSSQL] STEEPC.dbo.scZwrotNag",
        "[MSSQL] STEEPC.dbo.scZwrotSzczeg",
        "[MSSQL] STEEPC.dbo.scAustauch",
        "[MSSQL] STEEPC.dbo.v_austachStatusProsty",
    }

    def priority(key):
        if key in KLUCZOWE_TABELE:
            return -1  # kluczowe zawsze na górze
        for i, kw in enumerate(priority_keywords):
            if kw.lower() in key.lower():
                return i
        return len(priority_keywords)

    def priority_mssql(key):
        # Kluczowe MSSQL na samym górze
        if key in KLUCZOWE_MSSQL:
            return -2
        # RAPDB ma wysoki priorytet (tam są raporty)
        if "RAPDB" in key:
            return -1
        # Główne bazy operacyjne
        if any(b in key for b in ["STEEPC", "STEEPW", "WWMSDB", "ebayApiDB", "SHOP_PMG"]):
            return 0
        # Reszta
        return 1

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
        # Lista wszystkich faktycznie pobranych baz
        unique_dbs = sorted(set(v.get("database", "?") for v in schema_mssql.values()))
        lines.append("")
        lines.append(f"# === MS SQL SERVER (DOSTĘPNE BAZY: {', '.join(unique_dbs)}) ===")
        lines.append("# UWAGA: Dialekt T-SQL! TOP N zamiast LIMIT, GETDATE() zamiast CURRENT_DATE, []/[] dla identyfikatorów")
        lines.append("# UWAGA: PEŁNA ŚCIEŻKA z bazą - [BAZA].[dbo].[TABELA] - inaczej AI zgubi się w której bazie szukać!")
        lines.append("# RAPDB to baza raportowa - tu są widoki do KOSZTÓW, SPRZEDAŻY, AUSTAUSCHÓW, BANKÓW, PREMII")
        
        # Sortuj z priorytetem dla RAPDB i kluczowych widoków
        # Limit MSSQL jest 3x większy bo mamy 1295 obiektów
        mssql_limit = max_tables * 4
        for key in sorted(schema_mssql.keys(), key=priority_mssql)[:mssql_limit]:
            info = schema_mssql[key]
            type_label = "VIEW" if info["type"] == "VIEW" else "TABLE"
            # Dla kluczowych - WSZYSTKIE kolumny, dla reszty - 30
            if key in KLUCZOWE_MSSQL:
                limit_cols = len(info["columns"])
                marker = " ⭐KLUCZOWA"
            else:
                limit_cols = 30
                marker = ""
            cols_str = ", ".join([f'{c["name"]} ({c["type"]})' for c in info["columns"][:limit_cols]])
            if len(info["columns"]) > limit_cols:
                cols_str += f" ... +{len(info['columns'])-limit_cols} więcej"
            lines.append(f'MSSQL {type_label} {key}{marker}: {cols_str}')

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
tab_dashboard, tab_bilanse, tab_ai = st.tabs(["📊 Dashboard", "💰 Bilanse", "🤖 Zapytanie AI"])

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


# ============ TAB: BILANSE ============
with tab_bilanse:
    st.header("💰 Bilanse operacyjne")
    st.caption("Kluczowe wykresy diagnostyczne — kurierzy (faktury vs przelewy) oraz koszt części per skrzynia. Dane na żywo z bazy.")
    
    bilans_typ = st.radio(
        "Wybierz raport:",
        ["🚛 Kurierzy: faktury vs przelewy (12mc)", "📦 Koszt części: top 15 skrzyń (10mc)", "💸 Rentowność: najgorsza marża", "🔔 Audyt dzwonka rentowności", "📊 Pełny obraz per skrzynia (3mc)"],
        horizontal=True,
        key="bilans_typ"
    )
    
    if "🚛" in bilans_typ:
        st.subheader("Bilans kurierski — faktury (księgowo) vs realne przelewy z PKO")
        
        odswiez_kurierzy = st.button("🔄 Pobierz / odśwież dane", key="kurierzy_pobierz")
        
        if odswiez_kurierzy:
            with st.spinner("Pobieram dane z RAPDB (faktury + bank PKO)..."):
                try:
                    # SQL 1: Faktury per kurier per miesiąc
                    sql_faktury = """
                    SELECT 
                        FORMAT([Data], 'yyyy-MM') AS "Miesiac",
                        CASE 
                            WHEN [klNazwa] LIKE '%FEDEX%' THEN 'FEDEX'
                            WHEN [klNazwa] LIKE '%UPS POLSKA%' OR [klNazwa] LIKE '%UPS SP%' OR [klNazwa] LIKE '%UPS KUNDE%' THEN 'UPS'
                            WHEN [klNazwa] LIKE '%SCHENKER%' THEN 'SCHENKER'
                            ELSE 'INNI'
                        END AS "Kurier",
                        SUM([prztKwotaBrutto]) AS "Kwota_faktury",
                        COUNT(*) AS "Liczba_pozycji"
                    FROM [RAPDB].[dbo].[VwKosztyVer1]
                    WHERE [Data] >= DATEADD(month, -12, CAST(GETDATE() AS DATE))
                      AND [TagNazwaP1] = 'utrzymanie' 
                      AND [TagNazwaP2] = 'transportobcy'
                      AND [prztKwotaBrutto] > 0
                      AND ([klNazwa] LIKE '%FEDEX%' OR [klNazwa] LIKE '%UPS POLSKA%' 
                           OR [klNazwa] LIKE '%UPS SP%' OR [klNazwa] LIKE '%UPS KUNDE%' 
                           OR [klNazwa] LIKE '%SCHENKER%')
                    GROUP BY FORMAT([Data], 'yyyy-MM'),
                        CASE 
                            WHEN [klNazwa] LIKE '%FEDEX%' THEN 'FEDEX'
                            WHEN [klNazwa] LIKE '%UPS POLSKA%' OR [klNazwa] LIKE '%UPS SP%' OR [klNazwa] LIKE '%UPS KUNDE%' THEN 'UPS'
                            WHEN [klNazwa] LIKE '%SCHENKER%' THEN 'SCHENKER'
                            ELSE 'INNI'
                        END
                    ORDER BY "Miesiac", "Kurier"
                    """
                    df_faktury, err1 = odpal_zapytanie_mssql(sql_faktury, database="RAPDB")
                    if err1:
                        st.error(f"Błąd pobierania faktur: {err1}")
                        st.stop()
                    
                    # SQL 2: Przelewy z PKO per kurier per miesiąc
                    sql_przelewy = """
                    SELECT 
                        FORMAT(CAST([PkoDataOper] AS date), 'yyyy-MM') AS "Miesiac",
                        CASE 
                            WHEN [PkoOpis] LIKE '%FEDEX%' THEN 'FEDEX'
                            WHEN [PkoOpis] LIKE '%UPS POLSKA%' OR [PkoOpis] LIKE '%UPS SP%' OR [PkoOpis] LIKE '%UPS KUNDE%' THEN 'UPS'
                            WHEN [PkoOpis] LIKE '%SCHENKER%' THEN 'SCHENKER'
                            ELSE 'INNI'
                        END AS "Kurier",
                        SUM(CASE WHEN [PkoKwota] < 0 THEN -[PkoKwota] ELSE 0 END) AS "Wyplacone",
                        SUM(CASE WHEN [PkoKwota] > 0 THEN [PkoKwota] ELSE 0 END) AS "Wplywy",
                        COUNT(*) AS "Liczba_przelewow"
                    FROM [RAPDB].[dbo].[TblBankPko]
                    WHERE CAST([PkoDataOper] AS date) >= DATEADD(month, -12, CAST(GETDATE() AS DATE))
                      AND ([PkoOpis] LIKE '%FEDEX%' OR [PkoOpis] LIKE '%UPS POLSKA%' 
                           OR [PkoOpis] LIKE '%UPS SP%' OR [PkoOpis] LIKE '%UPS KUNDE%' 
                           OR [PkoOpis] LIKE '%SCHENKER%')
                    GROUP BY FORMAT(CAST([PkoDataOper] AS date), 'yyyy-MM'),
                        CASE 
                            WHEN [PkoOpis] LIKE '%FEDEX%' THEN 'FEDEX'
                            WHEN [PkoOpis] LIKE '%UPS POLSKA%' OR [PkoOpis] LIKE '%UPS SP%' OR [PkoOpis] LIKE '%UPS KUNDE%' THEN 'UPS'
                            WHEN [PkoOpis] LIKE '%SCHENKER%' THEN 'SCHENKER'
                            ELSE 'INNI'
                        END
                    ORDER BY "Miesiac", "Kurier"
                    """
                    df_przelewy, err2 = odpal_zapytanie_mssql(sql_przelewy, database="RAPDB")
                    if err2:
                        st.error(f"Błąd pobierania przelewów: {err2}")
                        st.stop()
                    
                    st.session_state["_bilans_kurierzy_dane"] = {
                        "faktury": df_faktury,
                        "przelewy": df_przelewy
                    }
                except Exception as e:
                    st.error(f"Błąd: {e}")
                    st.stop()
        
        dane = st.session_state.get("_bilans_kurierzy_dane")
        if dane is None:
            st.info("Kliknij '🔄 Pobierz świeże dane' żeby załadować raport.")
        else:
            df_fak = dane["faktury"]
            df_prz = dane["przelewy"]
            
            # Połącz dane
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
            
            kurierzy = ["FEDEX", "UPS", "SCHENKER"]
            kolory = {"FEDEX": "#E74C3C", "UPS": "#FFB347", "SCHENKER": "#27AE60"}
            
            # Wszystkie miesiące
            miesiace_fak = set(df_fak["Miesiac"].astype(str)) if not df_fak.empty else set()
            miesiace_prz = set(df_prz["Miesiac"].astype(str)) if not df_prz.empty else set()
            miesiace = sorted(miesiace_fak | miesiace_prz)
            
            # KPI panel
            st.markdown("### 📊 Saldo niezapłaconych faktur (12 mc)")
            kpi_cols = st.columns(4)
            
            saldo_total = 0
            saldo_per_kurier = {}
            for k in kurierzy:
                fak_k = df_fak[df_fak["Kurier"] == k]["Kwota_faktury"].sum() if not df_fak.empty else 0
                wych_k = df_prz[df_prz["Kurier"] == k]["Wyplacone"].sum() if not df_prz.empty else 0
                wpl_k = df_prz[df_prz["Kurier"] == k]["Wplywy"].sum() if not df_prz.empty else 0
                netto = wych_k - wpl_k
                saldo = float(fak_k) - float(netto)
                saldo_per_kurier[k] = saldo
                saldo_total += saldo
            
            with kpi_cols[0]:
                st.metric("🔴 FedEx — saldo", f"{saldo_per_kurier.get('FEDEX', 0):+,.0f} zł", help="Faktury minus realne wypłaty netto")
            with kpi_cols[1]:
                st.metric("🟡 UPS — saldo", f"{saldo_per_kurier.get('UPS', 0):+,.0f} zł", help="UWAGA: wpływy = pewnie pobrania od klientów (COD)")
            with kpi_cols[2]:
                st.metric("🟢 Schenker — saldo", f"{saldo_per_kurier.get('SCHENKER', 0):+,.0f} zł")
            with kpi_cols[3]:
                st.metric("📊 RAZEM saldo", f"{saldo_total:+,.0f} zł", help='"Wiszące" zobowiązania kurierskie')
            
            # 3 wykresy obok siebie
            st.markdown("### 📈 Faktury vs Realne wypłaty per kurier")
            
            for k in kurierzy:
                fak_dict = {str(r["Miesiac"]): float(r["Kwota_faktury"]) for _, r in df_fak[df_fak["Kurier"] == k].iterrows()}
                prz_subset = df_prz[df_prz["Kurier"] == k]
                netto_dict = {}
                for _, r in prz_subset.iterrows():
                    netto_dict[str(r["Miesiac"])] = float(r["Wyplacone"]) - float(r["Wplywy"])
                
                fak_vals = [fak_dict.get(m, 0) / 1000 for m in miesiace]
                netto_vals = [netto_dict.get(m, 0) / 1000 for m in miesiace]
                
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=miesiace, y=fak_vals, name="Faktury (księgowo)",
                    marker_color=kolory[k],
                    text=[f"{v:.0f}k" if v >= 50 else "" for v in fak_vals],
                    textposition='outside'
                ))
                fig.add_trace(go.Bar(
                    x=miesiace, y=netto_vals, name="Realne wypłaty netto z PKO",
                    marker_color="#34495E",
                    text=[f"{v:.0f}k" if v >= 50 else "" for v in netto_vals],
                    textposition='outside'
                ))
                fig.update_layout(
                    template="plotly_white",
                    title=f"<b>{k}</b> — porównanie kwot zafakturowanych z realnie wypłaconymi (tys. PLN)",
                    barmode='group',
                    height=350,
                    legend=dict(orientation='h', y=1.15, x=0.5, xanchor='center'),
                    yaxis=dict(title='tys. PLN'),
                    margin=dict(t=70, b=40, l=70, r=30)
                )
                st.plotly_chart(fig, use_container_width=True, key=f"chart_kurier_{k}")
            
            # Tabela szczegółowa
            with st.expander("📋 Pełna tabela szczegółowa (klik aby rozwinąć)"):
                # Łączymy faktury z przelewami
                rows = []
                for m in miesiace:
                    row = {"Miesiąc": m}
                    for k in kurierzy:
                        fak_k = df_fak[(df_fak["Miesiac"].astype(str) == m) & (df_fak["Kurier"] == k)]["Kwota_faktury"].sum()
                        prz_subset = df_prz[(df_prz["Miesiac"].astype(str) == m) & (df_prz["Kurier"] == k)]
                        wych_k = prz_subset["Wyplacone"].sum() if not prz_subset.empty else 0
                        wpl_k = prz_subset["Wplywy"].sum() if not prz_subset.empty else 0
                        row[f"{k} faktury"] = float(fak_k)
                        row[f"{k} netto wypł."] = float(wych_k) - float(wpl_k)
                    rows.append(row)
                df_tabela = pd.DataFrame(rows)
                st.dataframe(df_tabela, use_container_width=True)
            
            # Info biznesowe
            st.info("""
            **🚨 Klucz do interpretacji:**
            - **FedEx** — duże saldo dodatnie = faktury wystawione (kary za pakowanie?), korekty się ciągną
            - **UPS** — duże wpływy zwrotne = pewnie pobrania COD od klientów (do zweryfikowania z księgową)
            - **Schenker** — wzór zdrowy, faktury ≈ wypłaty, brak sporu
            - **Pytania do księgowości**: czy do VwKosztyVer1 wpadają korekty? Jakie saldo nieskonsumowanych pism reklamacyjnych?
            """)
    
    elif "📦" in bilans_typ:  # Koszt części top 15 skrzyń
        st.subheader("Trend kosztu części pobranych do regeneracji — top 15 skrzyń")
        
        odswiez_skrzynie = st.button("🔄 Pobierz / odśwież dane", key="skrzynie_pobierz")
        
        if odswiez_skrzynie:
            with st.spinner("Liczę koszty... (może potrwać 30-60 sek)"):
                try:
                    sql_skrzynie = """
                    WITH 
                    zlecenia AS (
                        SELECT DISTINCT
                            zp."artID",
                            art."artIndeks" AS skrzynia,
                            date_trunc('month', zp."zlecprpDataTw")::date AS miesiac,
                            zad."zadprElementNrPartii" AS partia
                        FROM "public"."wmsZleceniaProdukcyjnePozycjeHist" zp
                        JOIN "public"."wmsArtykuly" art ON art."artID" = zp."artID"
                        JOIN "public"."wmsZleceniaZadaniaProdukcyjneHist" zad 
                            ON zad."zlecprpID" = zp."zlecprpID"
                        WHERE zp."zlecprpDataTw" >= CURRENT_DATE - INTERVAL '10 months'
                          AND zad."zadprElementNrPartii" IS NOT NULL
                          AND zad."zadprElementTyp" = 2
                    ),
                    top15 AS (
                        SELECT skrzynia
                        FROM zlecenia
                        GROUP BY skrzynia
                        ORDER BY COUNT(DISTINCT partia) DESC
                        LIMIT 15
                    ),
                    pobory AS (
                        SELECT 
                            z.skrzynia,
                            z.miesiac,
                            z.partia,
                            SUM(
                                poz."pakpIlosc" * 
                                COALESCE(
                                    (SELECT MAX(ak."artkCenaBrutto") 
                                     FROM "public"."wmsArtykulyKontrahenci" ak 
                                     WHERE ak."artID" = poz."artID" AND ak."artkCenaBrutto" > 0),
                                    (SELECT sc."asczhSrCenaZakPo" 
                                     FROM "public"."wmsArtykulySrCenyZakHist" sc 
                                     WHERE sc."artID" = poz."artID" 
                                     ORDER BY sc."aschDataTw" DESC LIMIT 1),
                                    0
                                )
                            ) AS koszt_partii
                        FROM zlecenia z
                        JOIN "public"."wmsPakunkiHist" pak 
                            ON pak."zadprNrPartii" = z.partia
                            AND pak."pakRodzaj" = 3
                        JOIN "public"."wmsPakunkiPozycjeHist" poz 
                            ON poz."pakID" = pak."pakID"
                        WHERE z.skrzynia IN (SELECT skrzynia FROM top15)
                        GROUP BY z.skrzynia, z.miesiac, z.partia
                    )
                    SELECT 
                        skrzynia,
                        TO_CHAR(miesiac, 'YYYY-MM') AS miesiac,
                        COUNT(DISTINCT partia) AS sztuk,
                        ROUND(AVG(koszt_partii)::numeric, 0) AS sr_koszt,
                        ROUND(MIN(koszt_partii)::numeric, 0) AS min_koszt,
                        ROUND(MAX(koszt_partii)::numeric, 0) AS max_koszt
                    FROM pobory
                    GROUP BY skrzynia, miesiac
                    ORDER BY skrzynia, miesiac
                    """
                    df_skrz, err_pg = odpal_zapytanie_sql(sql_skrzynie, force_engine='postgres')
                    if err_pg:
                        st.error(f"Błąd: {err_pg}")
                        st.stop()
                    st.session_state["_bilans_skrzynie_dane"] = df_skrz
                except Exception as e:
                    st.error(f"Błąd: {e}")
                    st.stop()
        
        df_skrz = st.session_state.get("_bilans_skrzynie_dane")
        if df_skrz is None or df_skrz.empty:
            st.info("Kliknij '🔄 Pobierz świeże dane'")
        else:
            import plotly.graph_objects as go
            
            # KPI: Trendy
            skrzynie_lista = sorted(df_skrz["skrzynia"].unique())
            
            trendy = []
            for s in skrzynie_lista:
                subset = df_skrz[df_skrz["skrzynia"] == s].sort_values("miesiac")
                if len(subset) >= 4:
                    first_half = subset.head(3)
                    last_half = subset.tail(3)
                    sr_first = (first_half["sr_koszt"] * first_half["sztuk"]).sum() / first_half["sztuk"].sum()
                    sr_last = (last_half["sr_koszt"] * last_half["sztuk"]).sum() / last_half["sztuk"].sum()
                    delta_pct = (sr_last - sr_first) / sr_first * 100 if sr_first > 0 else 0
                    sztuk_total = int(subset["sztuk"].sum())
                    var_pct = (subset["sr_koszt"].max() - subset["sr_koszt"].min()) / subset["sr_koszt"].mean() * 100
                    trendy.append({
                        "skrzynia": s, "sztuk": sztuk_total, 
                        "sr_first": float(sr_first), "sr_last": float(sr_last),
                        "delta_pct": float(delta_pct), "var_pct": float(var_pct)
                    })
            
            stabilne = sum(1 for t in trendy if abs(t["delta_pct"]) <= 5)
            spadek = sum(1 for t in trendy if t["delta_pct"] < -5)
            wzrost = sum(1 for t in trendy if t["delta_pct"] > 5)
            wysoka_var = sum(1 for t in trendy if t["var_pct"] > 50)
            
            st.markdown("### 📊 Status hipotezy: 'Biorą za dużo części z magazynu'")
            kpi_cols = st.columns(4)
            with kpi_cols[0]:
                st.metric("⚖️ Stabilne (±5%)", f"{stabilne} z {len(trendy)}")
            with kpi_cols[1]:
                st.metric("📉 Spadek kosztu", f"{spadek} z {len(trendy)}")
            with kpi_cols[2]:
                st.metric("📈 Wzrost kosztu", f"{wzrost} z {len(trendy)}", 
                         delta="HIPOTEZA OBALONA" if wzrost == 0 else None,
                         delta_color="normal" if wzrost == 0 else "inverse")
            with kpi_cols[3]:
                st.metric("🚨 Wysoka wariancja (>50%)", f"{wysoka_var} z {len(trendy)}")
            
            # Wykres liniowy 15 skrzyń
            st.markdown("### 📈 Trend średniego kosztu części per skrzynia")
            
            kolory_palette = ['#E74C3C','#C0392B','#27AE60','#F39C12','#E67E22','#3498DB',
                              '#9B59B6','#1ABC9C','#34495E','#16A085','#8E44AD','#D35400',
                              '#2ECC71','#E91E63','#00BCD4']
            
            fig = go.Figure()
            for i, s in enumerate(skrzynie_lista):
                subset = df_skrz[df_skrz["skrzynia"] == s].sort_values("miesiac")
                fig.add_trace(go.Scatter(
                    x=subset["miesiac"].astype(str),
                    y=subset["sr_koszt"].astype(float),
                    name=s,
                    mode='lines+markers',
                    line=dict(color=kolory_palette[i % len(kolory_palette)], width=2),
                    marker=dict(size=7),
                    hovertemplate=f"<b>{s}</b><br>%{{x}}: %{{y:,.0f}} zł/szt<extra></extra>"
                ))
            fig.update_layout(
                template="plotly_white",
                height=500,
                legend=dict(orientation='v', y=1, x=1.02),
                yaxis=dict(title='średni koszt zł/szt'),
                margin=dict(t=20, b=50, l=70, r=200),
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True, key="chart_skrzynie_linie")
            
            # Heatmapa
            st.markdown("### 🔥 Heatmapa: skrzynia × miesiąc (kolor = średni koszt)")
            
            miesiace_lista = sorted(df_skrz["miesiac"].astype(str).unique())
            heatmap_z = []
            for s in skrzynie_lista:
                row = []
                for m in miesiace_lista:
                    val = df_skrz[(df_skrz["skrzynia"] == s) & (df_skrz["miesiac"].astype(str) == m)]
                    if not val.empty:
                        row.append(float(val.iloc[0]["sr_koszt"]))
                    else:
                        row.append(None)
                heatmap_z.append(row)
            
            fig_heat = go.Figure(data=go.Heatmap(
                z=heatmap_z,
                x=miesiace_lista,
                y=skrzynie_lista,
                colorscale='YlOrRd',
                hoverongaps=False,
                hovertemplate='<b>%{y}</b><br>%{x}: %{z:,.0f} zł/szt<extra></extra>'
            ))
            fig_heat.update_layout(
                template="plotly_white",
                height=500,
                yaxis=dict(autorange='reversed'),
                margin=dict(t=20, b=50, l=180, r=50)
            )
            st.plotly_chart(fig_heat, use_container_width=True, key="chart_skrzynie_heatmap")
            
            # Tabela trendów
            st.markdown("### 📋 Tabela trendów per skrzynia")
            df_trendy = pd.DataFrame(trendy)
            df_trendy["sr_first"] = df_trendy["sr_first"].round(0)
            df_trendy["sr_last"] = df_trendy["sr_last"].round(0)
            df_trendy["delta_pct"] = df_trendy["delta_pct"].round(1)
            df_trendy["var_pct"] = df_trendy["var_pct"].round(0)
            df_trendy.columns = ["Skrzynia", "Sztuk total", "Śr. 1szych 3mc", "Śr. ostatnich 3mc", "Δ %", "Wariancja %"]
            df_trendy = df_trendy.sort_values("Sztuk total", ascending=False)
            st.dataframe(df_trendy, use_container_width=True, hide_index=True)
            
            # Wniosek biznesowy
            if wzrost == 0:
                st.success("""
                **✅ HIPOTEZA "biorą za dużo części z magazynu" — TWARDO OBALONA**
                
                Żadna z 15 skrzyń nie pokazuje wzrostu kosztu materiałowego >5% w ostatnich 3 miesiącach. 
                Pracownicy pracują zgodnie z procesem. Realny problem 300k bufora kwietnia jest gdzie indziej (transport/FedEx).
                """)
            
            if wysoka_var > 0:
                st.warning(f"""
                **🚨 Sygnał:** {wysoka_var} skrzyń ma wysoką wariancję kosztu (>50%). 
                To może oznaczać brak standardu procesu lub bardzo różne stopnie zużycia od klientów.
                """)
    
    elif "💸" in bilans_typ:  # Rentowność najgorsza marża
        st.subheader("Rentowność per skrzynia — najgorsza marża materiałowa")
        st.caption("""
        Łączymy: **cenę sprzedaży z zamówień** (`scZAMKLISZCZEG.zksIndex_WEB`) × **kurs walutowy** 
        z **kosztem części** pobranych z magazynu w 10mc. Dolicza się **stałe koszty pozostałe 800 zł/sztukę** 
        (robocizna + utrzymanie + administracja). Marża = cena - koszt części - koszty pozostałe.
        """)
        
        col_a, col_b, col_c = st.columns([1, 1, 2])
        with col_a:
            koszty_pozostale = st.number_input("Koszty pozostałe / szt (PLN)", value=800, step=50, key="koszty_poz")
        with col_b:
            kurs_eur = st.number_input("Kurs EUR/PLN", value=4.30, step=0.05, format="%.2f", key="kurs_eur_input")
        
        odswiez_rent = st.button("🔄 Pobierz / odśwież dane", key="rentownosc_pobierz")
        
        if odswiez_rent:
            with st.spinner("Liczę rentowność (sprzedaż MSSQL + koszty Postgres)... 30-60 sek..."):
                try:
                    # SQL 1: Sprzedaż z zamówień (MSSQL) — bierzemy zksIndex_WEB (konkretny indeks)
                    sql_sprzedaz = """
                    SELECT 
                        zks.[zksIndex_WEB] AS skrzynia,
                        COUNT(*) AS sztuk,
                        AVG(CAST(zks.[zksCenaBrutto] AS float)) AS sr_cena_w_walucie,
                        SUM(CAST(zks.[zksCenaBrutto] AS float)) AS suma_w_walucie,
                        MIN(CAST(zks.[zksCenaBrutto] AS float)) AS min_cena,
                        MAX(CAST(zks.[zksCenaBrutto] AS float)) AS max_cena
                    FROM [STEEPC].[dbo].[scZAMKLISZCZEG] zks
                    WHERE zks.[zksDataTw] >= DATEADD(month, -10, GETDATE())
                      AND zks.[zksIndex_WEB] IS NOT NULL
                      AND zks.[zksIndex_WEB] NOT IN ('KAUCJA', 'TOWAR_WEWN', '99999', 'GET', '')
                      AND zks.[zksIndex_WEB] NOT LIKE 'ORG%'
                      AND zks.[zksIndex_WEB] NOT LIKE 'REG%'
                      AND zks.[zksIndex_WEB] NOT LIKE 'OLEJ%'
                      AND zks.[zksIndex_WEB] NOT LIKE 'PAK_%'
                      AND CAST(zks.[zksCenaBrutto] AS float) > 100
                    GROUP BY zks.[zksIndex_WEB]
                    HAVING COUNT(*) >= 10
                    ORDER BY COUNT(*) DESC
                    """
                    df_sprzedaz, err1 = odpal_zapytanie_mssql(sql_sprzedaz, database="STEEPC")
                    if err1:
                        st.error(f"Błąd MSSQL: {err1}")
                        st.stop()
                    
                    # SQL 2: Koszty części z Postgres (z popraw — DISTINCT pakID)
                    sql_koszty = """
                    WITH 
                    pakunki_unikalne AS (
                        SELECT DISTINCT pak."pakID", pak."zadprNrPartii"
                        FROM "public"."wmsPakunkiHist" pak
                        WHERE pak."pakRodzaj" = 3
                          AND pak."zadprNrPartii" IS NOT NULL
                          AND pak."pakDataTw" >= CURRENT_DATE - INTERVAL '10 months'
                    ),
                    koszty_pakunkow AS (
                        SELECT 
                            pu."zadprNrPartii" AS partia,
                            SUM(
                                poz."pakpIlosc" * 
                                COALESCE(
                                    (SELECT MAX(ak."artkCenaBrutto") 
                                     FROM "public"."wmsArtykulyKontrahenci" ak 
                                     WHERE ak."artID" = poz."artID" AND ak."artkCenaBrutto" > 0),
                                    (SELECT sc."asczhSrCenaZakPo" 
                                     FROM "public"."wmsArtykulySrCenyZakHist" sc 
                                     WHERE sc."artID" = poz."artID" 
                                     ORDER BY sc."aschDataTw" DESC LIMIT 1),
                                    0
                                )
                            ) AS koszt
                        FROM pakunki_unikalne pu
                        JOIN "public"."wmsPakunkiPozycjeHist" poz ON poz."pakID" = pu."pakID"
                        GROUP BY pu."zadprNrPartii"
                    ),
                    skrzynie_partii AS (
                        SELECT DISTINCT
                            art."artIndeks" AS skrzynia,
                            zad."zadprElementNrPartii" AS partia
                        FROM "public"."wmsZleceniaProdukcyjnePozycjeHist" zp
                        JOIN "public"."wmsArtykuly" art ON art."artID" = zp."artID"
                        JOIN "public"."wmsZleceniaZadaniaProdukcyjneHist" zad 
                            ON zad."zlecprpID" = zp."zlecprpID"
                        WHERE zp."zlecprpDataTw" >= CURRENT_DATE - INTERVAL '10 months'
                          AND zad."zadprElementNrPartii" IS NOT NULL
                          AND zad."zadprElementTyp" = 2
                    )
                    SELECT 
                        sp.skrzynia,
                        ROUND(AVG(kp.koszt)::numeric, 0) AS sr_koszt_czesci,
                        COUNT(DISTINCT kp.partia) AS partii_wyprodukowanych
                    FROM skrzynie_partii sp
                    JOIN koszty_pakunkow kp ON kp.partia = sp.partia
                    WHERE kp.koszt > 0
                    GROUP BY sp.skrzynia
                    """
                    df_koszty, err_pg = odpal_zapytanie_sql(sql_koszty, force_engine='postgres')
                    if err_pg:
                        st.error(f"Błąd Postgres: {err_pg}")
                        st.stop()
                    
                    st.session_state["_bilans_rentownosc_dane"] = {
                        "sprzedaz": df_sprzedaz,
                        "koszty": df_koszty
                    }
                except Exception as e:
                    st.error(f"Błąd: {e}")
                    st.stop()
        
        dane = st.session_state.get("_bilans_rentownosc_dane")
        if dane is None:
            st.info("Kliknij '🔄 Pobierz świeże dane' żeby załadować raport.")
        else:
            df_sprz = dane["sprzedaz"]
            df_kszt = dane["koszty"]
            
            if df_sprz.empty or df_kszt.empty:
                st.warning("Brak danych — sprawdź połączenia z bazami.")
            else:
                # Łączymy
                df_sprz["skrzynia"] = df_sprz["skrzynia"].astype(str)
                df_kszt["skrzynia"] = df_kszt["skrzynia"].astype(str)
                
                df = df_sprz.merge(df_kszt, on="skrzynia", how="inner")
                
                # Konwersja cen — przyjmuję ŚREDNIĄ × kurs (mix PLN/EUR)
                # W praktyce większość pozycji to EUR (klient zagraniczny obsługiwany przez polski oddział)
                # Najprostsza heurystyka: cena × (kurs_eur * 0.7 + 1.0 * 0.3) = mix 70% EUR, 30% PLN
                df["sr_cena_pln"] = df["sr_cena_w_walucie"].astype(float) * (kurs_eur * 0.7 + 1.0 * 0.3)
                df["sr_koszt_czesci"] = df["sr_koszt_czesci"].astype(float)
                df["koszty_pozostale"] = float(koszty_pozostale)
                df["marza_pln"] = df["sr_cena_pln"] - df["sr_koszt_czesci"] - df["koszty_pozostale"]
                df["marza_pct"] = (df["marza_pln"] / df["sr_cena_pln"] * 100).round(1)
                df["razem_marza"] = (df["marza_pln"] * df["sztuk"]).round(0)
                df["sztuk"] = df["sztuk"].astype(int)
                
                # KPI
                ujemne = (df["marza_pln"] < 0).sum()
                niskie = ((df["marza_pln"] >= 0) & (df["marza_pln"] < 500)).sum()
                ok = (df["marza_pln"] >= 500).sum()
                suma_marza = df["razem_marza"].sum()
                
                st.markdown("### 📊 KPI rentowności")
                kpi_cols = st.columns(4)
                with kpi_cols[0]:
                    st.metric("🔴 Deficytowe", f"{ujemne} z {len(df)}", 
                             help="Skrzynie ze stratą (cena - części - koszty pozostałe < 0)")
                with kpi_cols[1]:
                    st.metric("⚠️ Niska marża (<500)", f"{niskie} z {len(df)}")
                with kpi_cols[2]:
                    st.metric("✅ Marża OK (≥500)", f"{ok} z {len(df)}")
                with kpi_cols[3]:
                    st.metric("💰 Suma marży 10mc", f"{suma_marza:,.0f} zł")
                
                # Tabela posortowana po marży rosnąco (najgorsze pierwsze)
                st.markdown("### 📋 Tabela rentowności (sortowana od najgorszej marży)")
                df_show = df.sort_values("marza_pln")[
                    ["skrzynia", "sztuk", "sr_cena_w_walucie", "sr_cena_pln", 
                     "sr_koszt_czesci", "koszty_pozostale", "marza_pln", "marza_pct", "razem_marza"]
                ].copy()
                df_show.columns = [
                    "Skrzynia", "Sztuk 10mc", "Śr. cena w walucie", "Śr. cena PLN", 
                    "Koszt części", "Koszty pozostałe", "Marża /szt", "Marża %", "Marża 10mc total"
                ]
                
                # Funkcja kolorująca wiersze - JAWNIE czarna czcionka!
                def koloruj_marze(row):
                    marza = row["Marża /szt"]
                    if marza < 0:
                        return ['background-color: #ffcccc; color: #000000; font-weight: 600'] * len(row)
                    elif marza < 500:
                        return ['background-color: #fff3a8; color: #000000'] * len(row)
                    else:
                        return ['background-color: #c8e6c9; color: #000000'] * len(row)
                
                styled = df_show.style.apply(koloruj_marze, axis=1).format({
                    "Śr. cena w walucie": "{:.0f}",
                    "Śr. cena PLN": "{:,.0f}",
                    "Koszt części": "{:,.0f}",
                    "Koszty pozostałe": "{:,.0f}",
                    "Marża /szt": "{:+,.0f}",
                    "Marża %": "{:+.1f}%",
                    "Marża 10mc total": "{:+,.0f}"
                })
                st.dataframe(styled, use_container_width=True, hide_index=True, height=600)
                
                # Wykres słupkowy — TOP 15 NAJGORSZYCH marż
                st.markdown("### 📊 Top 15 najgorszych marży (do natychmiastowego przeglądu)")
                import plotly.graph_objects as go
                df_top = df.sort_values("marza_pln").head(15)
                kolory = ['#E74C3C' if m < 0 else '#FFB347' if m < 500 else '#27AE60' 
                          for m in df_top["marza_pln"]]
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=df_top["skrzynia"],
                    y=df_top["marza_pln"],
                    text=[f"{m:+,.0f} zł" for m in df_top["marza_pln"]],
                    textposition='outside',
                    marker_color=kolory,
                    hovertemplate="<b>%{x}</b><br>Marża: %{y:+,.0f} PLN/szt<extra></extra>"
                ))
                fig.update_layout(
                    template="plotly_white",
                    height=450,
                    yaxis=dict(title='Marża /szt (PLN)', zerolinecolor='#2C3E50', zerolinewidth=2),
                    xaxis=dict(tickangle=-45),
                    margin=dict(t=20, b=120, l=70, r=30),
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True, key="chart_marza_top_najgorsza")
                
                # Wniosek biznesowy
                if ujemne > 0:
                    st.error(f"""
                    **🚨 ALERT:** {ujemne} skrzyń sprzedawanych ze STRATĄ materiałową!
                    
                    Po doliczeniu kosztów części + {koszty_pozostale} zł kosztów pozostałych, sprzedaż jest deficytowa. 
                    To kandydaci do natychmiastowego przeglądu cennika lub kosztów regeneracji.
                    """)
                
                if niskie > 0:
                    st.warning(f"""
                    **⚠️ {niskie} skrzyń ma niską marżę (<500 zł/szt).** 
                    Wąski margines bezpieczeństwa — drobny wzrost kosztów = strata. Warto przyjrzeć się cenom.
                    """)
                
                st.info(f"""
                **🔍 Założenia kalkulacji:**
                - **Cena PLN** = cena z zamówienia × ({kurs_eur} × 0.7 + 1.0 × 0.3) — mix walut (~70% EUR, 30% PLN)
                - **Koszt części** = średni koszt pakunków pobranych do produkcji per skrzynia
                - **Koszty pozostałe** = stała wartość {koszty_pozostale} zł/szt (robocizna + administracja + utrzymanie + transport)
                - **Marża** = cena PLN - koszt części - koszty pozostałe
                
                **⚠️ To jest UPROSZCZONE PRZYBLIŻENIE** — realna kalkulacja wymaga:
                - Rozdzielenia transakcji PLN/EUR per skrzynia
                - Doprecyzowania kosztów pozostałych per typ skrzyni
                - Uwzględnienia depozytu starej skrzyni klienta (austausch)
                """)
    
    elif "🔔" in bilans_typ:  # Audyt dzwonka rentowności
        st.subheader("Audyt dzwonka rentowności — kontrola kosztów partii produkcyjnych")
        st.caption("""
        System dzwonka: każda skrzynia ma ustawiony **maksymalny próg kwoty części** (`wmsArtykulyMaksWartoscZamHist`). 
        Po przekroczeniu — blokada wydania, kierownik produkcji zatwierdza. Sprawdzamy czy progi są aktualne 
        względem realnych kosztów 10mc i ile partii przekracza próg.
        """)
        
        col_a, col_b = st.columns([1, 3])
        with col_a:
            mnoznik_sugestia = st.number_input("Mnożnik sugerowanego progu × średni koszt", 
                                                value=1.3, step=0.1, format="%.1f", key="mnoznik_prog",
                                                help="Sugerowany nowy próg = ten mnożnik × średni koszt 10mc")
        
        odswiez_dzw = st.button("🔄 Pobierz / odśwież dane", key="dzwonek_pobierz")
        
        if odswiez_dzw:
            with st.spinner("Liczę audyt dzwonka (progi vs realne koszty 10mc)... 30-60 sek..."):
                try:
                    # SQL 1: Statystyki globalne
                    sql_globalne = """
                    WITH 
                    ostatnie_progi AS (
                        SELECT DISTINCT ON ("artID") 
                            "artID", "amwzhMaksWartoscZam" AS prog,
                            "amwzhSprawdzajMaksWartoscZam" AS aktywny
                        FROM "public"."wmsArtykulyMaksWartoscZamHist"
                        ORDER BY "artID", "amwzhDataTw" DESC
                    ),
                    pakunki_unikalne AS (
                        SELECT DISTINCT pak."pakID", pak."zadprNrPartii"
                        FROM "public"."wmsPakunkiHist" pak
                        WHERE pak."pakRodzaj" = 3 
                          AND pak."pakDataTw" >= CURRENT_DATE - INTERVAL '10 months'
                    ),
                    koszty AS (
                        SELECT pu."zadprNrPartii" AS partia,
                            SUM(poz."pakpIlosc" * COALESCE(
                                (SELECT MAX(ak."artkCenaBrutto") FROM "public"."wmsArtykulyKontrahenci" ak 
                                 WHERE ak."artID" = poz."artID" AND ak."artkCenaBrutto" > 0),
                                (SELECT sc."asczhSrCenaZakPo" FROM "public"."wmsArtykulySrCenyZakHist" sc 
                                 WHERE sc."artID" = poz."artID" ORDER BY sc."aschDataTw" DESC LIMIT 1),
                                0
                            )) AS koszt
                        FROM pakunki_unikalne pu
                        JOIN "public"."wmsPakunkiPozycjeHist" poz ON poz."pakID" = pu."pakID"
                        GROUP BY pu."zadprNrPartii"
                    ),
                    partie_z_progiem AS (
                        SELECT 
                            zp."artID", zad."zadprElementNrPartii" AS partia,
                            op.prog, k.koszt
                        FROM "public"."wmsZleceniaProdukcyjnePozycjeHist" zp
                        JOIN "public"."wmsZleceniaZadaniaProdukcyjneHist" zad 
                            ON zad."zlecprpID" = zp."zlecprpID" AND zad."zadprElementTyp" = 2
                        JOIN ostatnie_progi op ON op."artID" = zp."artID" AND op.aktywny = TRUE
                        JOIN koszty k ON k.partia = zad."zadprElementNrPartii"
                        WHERE zad."zadprElementNrPartii" IS NOT NULL
                          AND op.prog IS NOT NULL
                    )
                    SELECT 
                        COUNT(*) AS partii_total,
                        SUM(CASE WHEN koszt > prog THEN 1 ELSE 0 END) AS przekroczyly,
                        SUM(CASE WHEN koszt > prog * 1.5 THEN 1 ELSE 0 END) AS x15,
                        SUM(CASE WHEN koszt > prog * 2 THEN 1 ELSE 0 END) AS x2,
                        SUM(CASE WHEN koszt > prog * 3 THEN 1 ELSE 0 END) AS x3,
                        SUM(CASE WHEN koszt > prog THEN koszt - prog ELSE 0 END) AS suma_przekroczen_pln
                    FROM partie_z_progiem
                    """
                    df_glob, err1 = odpal_zapytanie_sql(sql_globalne, force_engine='postgres')
                    if err1:
                        st.error(f"Błąd: {err1}")
                        st.stop()
                    
                    # SQL 2: Per skrzynia (TOP 30)
                    sql_per_skrzynia = """
                    WITH 
                    ostatnie_progi AS (
                        SELECT DISTINCT ON ("artID") 
                            "artID", "amwzhMaksWartoscZam" AS prog,
                            "amwzhSprawdzajMaksWartoscZam" AS aktywny,
                            "amwzhUserTw" AS user_ustawil,
                            "amwzhDataTw" AS data_ustawienia
                        FROM "public"."wmsArtykulyMaksWartoscZamHist"
                        ORDER BY "artID", "amwzhDataTw" DESC
                    ),
                    pakunki_unikalne AS (
                        SELECT DISTINCT pak."pakID", pak."zadprNrPartii"
                        FROM "public"."wmsPakunkiHist" pak
                        WHERE pak."pakRodzaj" = 3 
                          AND pak."pakDataTw" >= CURRENT_DATE - INTERVAL '10 months'
                    ),
                    koszty AS (
                        SELECT pu."zadprNrPartii" AS partia,
                            SUM(poz."pakpIlosc" * COALESCE(
                                (SELECT MAX(ak."artkCenaBrutto") FROM "public"."wmsArtykulyKontrahenci" ak 
                                 WHERE ak."artID" = poz."artID" AND ak."artkCenaBrutto" > 0),
                                (SELECT sc."asczhSrCenaZakPo" FROM "public"."wmsArtykulySrCenyZakHist" sc 
                                 WHERE sc."artID" = poz."artID" ORDER BY sc."aschDataTw" DESC LIMIT 1),
                                0
                            )) AS koszt
                        FROM pakunki_unikalne pu
                        JOIN "public"."wmsPakunkiPozycjeHist" poz ON poz."pakID" = pu."pakID"
                        GROUP BY pu."zadprNrPartii"
                    ),
                    skrzynie_partii AS (
                        SELECT DISTINCT
                            zp."artID", art."artIndeks" AS skrzynia,
                            zad."zadprElementNrPartii" AS partia
                        FROM "public"."wmsZleceniaProdukcyjnePozycjeHist" zp
                        JOIN "public"."wmsArtykuly" art ON art."artID" = zp."artID"
                        JOIN "public"."wmsZleceniaZadaniaProdukcyjneHist" zad 
                            ON zad."zlecprpID" = zp."zlecprpID" AND zad."zadprElementTyp" = 2
                        WHERE zad."zadprElementNrPartii" IS NOT NULL
                    )
                    SELECT 
                        sp.skrzynia,
                        COUNT(DISTINCT sp.partia) AS sztuk_10mc,
                        ROUND(AVG(k.koszt)::numeric, 0) AS sredni_koszt,
                        ROUND(MAX(k.koszt)::numeric, 0) AS max_koszt,
                        op.prog AS prog_dzwonka,
                        op.user_ustawil,
                        op.data_ustawienia::date AS data_ust,
                        op.aktywny,
                        SUM(CASE WHEN k.koszt > op.prog THEN 1 ELSE 0 END) AS partii_przekroczylo,
                        SUM(CASE WHEN k.koszt > op.prog * 2 THEN 1 ELSE 0 END) AS partii_x2
                    FROM skrzynie_partii sp
                    JOIN koszty k ON k.partia = sp.partia
                    LEFT JOIN ostatnie_progi op ON op."artID" = sp."artID"
                    WHERE k.koszt > 0
                    GROUP BY sp.skrzynia, op.prog, op.user_ustawil, op.data_ustawienia, op.aktywny
                    HAVING COUNT(DISTINCT sp.partia) >= 5
                    ORDER BY AVG(k.koszt) DESC
                    LIMIT 30
                    """
                    df_skrz, err2 = odpal_zapytanie_sql(sql_per_skrzynia, force_engine='postgres')
                    if err2:
                        st.error(f"Błąd: {err2}")
                        st.stop()
                    
                    st.session_state["_bilans_dzwonek_dane"] = {
                        "globalne": df_glob, "skrzynie": df_skrz
                    }
                except Exception as e:
                    st.error(f"Błąd: {e}")
                    st.stop()
        
        dane = st.session_state.get("_bilans_dzwonek_dane")
        if dane is None:
            st.info("Kliknij '🔄 Pobierz świeże dane' żeby załadować raport.")
        else:
            df_glob = dane["globalne"]
            df_skrz = dane["skrzynie"]
            
            if df_glob.empty:
                st.warning("Brak danych globalnych.")
            else:
                # KPI globalne
                row = df_glob.iloc[0]
                total = int(row["partii_total"])
                przekr = int(row["przekroczyly"])
                x15 = int(row["x15"])
                x2 = int(row["x2"])
                x3 = int(row["x3"])
                suma_pln = float(row["suma_przekroczen_pln"])
                
                st.markdown("### 🚨 SKALA PROBLEMU (10 miesięcy)")
                kpi_cols = st.columns(5)
                with kpi_cols[0]:
                    st.metric("Wszystkich partii", f"{total:,}")
                with kpi_cols[1]:
                    pct = przekr/total*100 if total else 0
                    st.metric("Przekroczyły próg", f"{przekr:,}", f"{pct:.1f}% partii",
                             delta_color="inverse")
                with kpi_cols[2]:
                    st.metric("Przekroczyły 1.5×", f"{x15:,}", f"{x15/total*100:.1f}%",
                             delta_color="inverse")
                with kpi_cols[3]:
                    st.metric("Przekroczyły 2×", f"{x2:,}", f"{x2/total*100:.1f}%",
                             delta_color="inverse")
                with kpi_cols[4]:
                    st.metric("Skrajne (3×)", f"{x3:,}", f"{x3/total*100:.1f}%",
                             delta_color="inverse")
                
                if pct > 30:
                    st.error(f"""
                    🔴 **DZWONEK MARTWY** — {pct:.0f}% partii dzwoni (zalecane: <10%)!
                    
                    Średnio **{przekr/10:.0f} alertów miesięcznie** ⇒ kierownik produkcji rutynowo akceptuje. 
                    Łączna suma przekroczeń ponad próg: **{suma_pln:,.0f} zł** w 10mc.
                    
                    **Konkretne case'y do analizy biznesowej**: {x2} partii > 2× próg + {x3} skrajnych (3× próg).
                    """)
                
                # Tabela per skrzynia
                if not df_skrz.empty:
                    st.markdown("### 📋 TOP 30 najdroższych skrzyń: progi vs realne koszty")
                    
                    df_skrz["prog_dzwonka"] = df_skrz["prog_dzwonka"].astype(float)
                    df_skrz["sredni_koszt"] = df_skrz["sredni_koszt"].astype(float)
                    df_skrz["sugerowany_prog"] = (df_skrz["sredni_koszt"] * mnoznik_sugestia).round(0)
                    df_skrz["status"] = df_skrz.apply(
                        lambda r: "🚨 Próg < średnia" if pd.notna(r["prog_dzwonka"]) and r["prog_dzwonka"] < r["sredni_koszt"]
                        else "⚠️ Blisko" if pd.notna(r["prog_dzwonka"]) and r["prog_dzwonka"] < r["sredni_koszt"] * 1.2
                        else "✅ OK" if pd.notna(r["prog_dzwonka"])
                        else "⚪ Brak progu",
                        axis=1
                    )
                    
                    df_show = df_skrz[[
                        "skrzynia", "sztuk_10mc", "sredni_koszt", "max_koszt", 
                        "prog_dzwonka", "sugerowany_prog", "user_ustawil", "data_ust",
                        "partii_przekroczylo", "partii_x2", "status"
                    ]].copy()
                    df_show.columns = [
                        "Skrzynia", "Sztuk 10mc", "Śr. koszt", "Max koszt",
                        "Próg dzwonka", f"Sugerowany ({mnoznik_sugestia}×)", "Kto ustawił", "Data ustawienia",
                        "Partii > próg", "Partii > 2× próg", "Status"
                    ]
                    
                    def koloruj_status(row):
                        st_val = str(row["Status"])
                        if "🚨" in st_val:
                            return ['background-color: #ffcccc; color: #000000; font-weight: 600'] * len(row)
                        elif "⚠️" in st_val:
                            return ['background-color: #fff3a8; color: #000000'] * len(row)
                        elif "✅" in st_val:
                            return ['background-color: #c8e6c9; color: #000000'] * len(row)
                        else:
                            return ['background-color: #f0f0f0; color: #000000'] * len(row)
                    
                    styled = df_show.style.apply(koloruj_status, axis=1).format({
                        "Śr. koszt": "{:,.0f}",
                        "Max koszt": "{:,.0f}",
                        "Próg dzwonka": "{:,.0f}",
                        f"Sugerowany ({mnoznik_sugestia}×)": "{:,.0f}"
                    })
                    st.dataframe(styled, use_container_width=True, hide_index=True, height=600)
                    
                    # Eksport CSV dla Dariusza
                    csv_data = df_show.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        "📥 Pobierz CSV dla Dariusza (TOP 30 skrzyń do aktualizacji)",
                        data=csv_data,
                        file_name=f"audyt_dzwonka_dla_dariusza_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime='text/csv'
                    )
                
                # Wnioski biznesowe
                st.markdown("### 💡 Wnioski biznesowe")
                st.info(f"""
                **🎯 Dlaczego dzwonek nie działa:**
                - Progi ustawione lata temu (głównie przez Dariusza) — ceny części wzrosły
                - {pct:.0f}% partii przekracza próg → kierownik produkcji ma rutynę "akceptuj"
                - Outliery (2-3× próg) giną w masie 1.5× przekroczeń
                
                **🔔 Sugerowane działania:**
                1. **Dariusz**: aktualizacja progów ({mnoznik_sugestia}× śr. koszt 10mc) — CSV wyżej
                2. **Proces firmowy**: kwartalny przegląd progów (ceny rosną — progi też muszą)
                3. **Escalation path**: 1.5× = kierownik | 2× = EA | 3× = STOP, EA + Igor zatwierdzają
                4. **Tygodniowy raport**: lista partii > 2× próg z wymogiem uzasadnienia
                
                **💰 Wartość problemu**: ~{suma_pln/1_000_000:.1f} mln zł "po cichu" wydanych ponad próg w 10mc 
                (nie konieczne że źle — może być uzasadnione, ale **nikt tego nie waliduje**).
                """)
    
    else:  # 📊 Pełny obraz per skrzynia (3mc)
        st.subheader("Pełny obraz per skrzynia — 3 miesiące")
        st.caption("""
        Najważniejsze metryki w jednej tabeli per skrzynia: **sprzedaż, koszt, marża, próg dzwonka i przekroczenia**.
        Łączymy: ceny sprzedaży z `scZAMKLISZCZEG.zksIndex_WEB`, koszty części z Postgres, próg z `wmsArtykulyMaksWartoscZamHist`,
        partie produkcyjne z `wmsZleceniaProdukcyjnePozycjeHist`. **Okres: ostatnie 3 miesiące.**
        """)
        
        col_a, col_b = st.columns([1, 3])
        with col_a:
            kurs_eur_full = st.number_input("Kurs EUR/PLN", value=4.30, step=0.05, format="%.2f", key="kurs_eur_full")
        
        odswiez_pelny = st.button("🔄 Pobierz / odśwież dane (~60 sek)", key="pelny_pobierz")
        
        if odswiez_pelny:
            with st.spinner("Liczę pełny obraz... 60-90 sek..."):
                try:
                    # SQL 1: Sprzedaż 3mc per skrzynia (z scZAMKLISZCZEG.zksIndex_WEB)
                    sql_sprzedaz = """
                    SELECT 
                        zks.[zksIndex_WEB] AS skrzynia,
                        COUNT(*) AS sztuk_sprzedanych,
                        AVG(CAST(zks.[zksCenaBrutto] AS float)) AS sr_cena_w_walucie,
                        SUM(CAST(zks.[zksCenaBrutto] AS float)) AS suma_w_walucie,
                        MIN(CAST(zks.[zksCenaBrutto] AS float)) AS min_cena,
                        MAX(CAST(zks.[zksCenaBrutto] AS float)) AS max_cena
                    FROM [STEEPC].[dbo].[scZAMKLISZCZEG] zks
                    WHERE zks.[zksDataTw] >= DATEADD(month, -3, GETDATE())
                      AND zks.[zksIndex_WEB] IS NOT NULL
                      AND zks.[zksIndex_WEB] NOT IN ('KAUCJA', 'TOWAR_WEWN', '99999', 'GET', '')
                      AND zks.[zksIndex_WEB] NOT LIKE 'ORG%'
                      AND zks.[zksIndex_WEB] NOT LIKE 'REG%'
                      AND zks.[zksIndex_WEB] NOT LIKE 'OLEJ%'
                      AND zks.[zksIndex_WEB] NOT LIKE 'PAK_%'
                      AND zks.[zksIndex_WEB] NOT LIKE '%_USZK'
                      AND CAST(zks.[zksCenaBrutto] AS float) > 100
                    GROUP BY zks.[zksIndex_WEB]
                    HAVING COUNT(*) >= 3
                    """
                    df_sprz, err1 = odpal_zapytanie_mssql(sql_sprzedaz, database="STEEPC")
                    if err1:
                        st.error(f"Błąd MSSQL: {err1}")
                        st.stop()
                    
                    # SQL 2: Koszty części + partie + przekroczenia + próg dzwonka (Postgres)
                    sql_full = """
                    WITH 
                    ostatnie_progi AS (
                        SELECT DISTINCT ON ("artID") 
                            "artID", 
                            "amwzhMaksWartoscZam" AS prog,
                            "amwzhSprawdzajMaksWartoscZam" AS aktywny
                        FROM "public"."wmsArtykulyMaksWartoscZamHist"
                        ORDER BY "artID", "amwzhDataTw" DESC
                    ),
                    pakunki_unikalne AS (
                        SELECT DISTINCT pak."pakID", pak."zadprNrPartii"
                        FROM "public"."wmsPakunkiHist" pak
                        WHERE pak."pakRodzaj" = 3 
                          AND pak."pakDataTw" >= CURRENT_DATE - INTERVAL '3 months'
                    ),
                    koszty_partii AS (
                        SELECT pu."zadprNrPartii" AS partia,
                            SUM(poz."pakpIlosc" * COALESCE(
                                (SELECT MAX(ak."artkCenaBrutto") FROM "public"."wmsArtykulyKontrahenci" ak 
                                 WHERE ak."artID" = poz."artID" AND ak."artkCenaBrutto" > 0),
                                (SELECT sc."asczhSrCenaZakPo" FROM "public"."wmsArtykulySrCenyZakHist" sc 
                                 WHERE sc."artID" = poz."artID" ORDER BY sc."aschDataTw" DESC LIMIT 1),
                                0
                            )) AS koszt
                        FROM pakunki_unikalne pu
                        JOIN "public"."wmsPakunkiPozycjeHist" poz ON poz."pakID" = pu."pakID"
                        GROUP BY pu."zadprNrPartii"
                    ),
                    skrzynie_partii AS (
                        SELECT DISTINCT
                            zp."artID",
                            art."artIndeks" AS skrzynia,
                            zad."zadprElementNrPartii" AS partia
                        FROM "public"."wmsZleceniaProdukcyjnePozycjeHist" zp
                        JOIN "public"."wmsArtykuly" art ON art."artID" = zp."artID"
                        JOIN "public"."wmsZleceniaZadaniaProdukcyjneHist" zad 
                            ON zad."zlecprpID" = zp."zlecprpID" AND zad."zadprElementTyp" = 2
                        WHERE zad."zadprElementNrPartii" IS NOT NULL
                    )
                    SELECT 
                        sp.skrzynia,
                        COUNT(DISTINCT sp.partia) AS sztuk_wyprodukowanych,
                        ROUND(AVG(kp.koszt)::numeric, 0) AS sr_koszt_czesci,
                        ROUND(MAX(kp.koszt)::numeric, 0) AS max_koszt_czesci,
                        op.prog AS prog_dzwonka,
                        op.aktywny AS dzwonek_aktywny,
                        SUM(CASE WHEN kp.koszt > op.prog THEN 1 ELSE 0 END) AS dzwonil_razy,
                        SUM(CASE WHEN kp.koszt > op.prog * 1.5 THEN 1 ELSE 0 END) AS dzwonil_15x,
                        SUM(CASE WHEN kp.koszt > op.prog * 2 THEN 1 ELSE 0 END) AS dzwonil_2x
                    FROM skrzynie_partii sp
                    JOIN koszty_partii kp ON kp.partia = sp.partia
                    LEFT JOIN ostatnie_progi op ON op."artID" = sp."artID"
                    WHERE kp.koszt > 0
                    GROUP BY sp.skrzynia, op.prog, op.aktywny
                    HAVING COUNT(DISTINCT sp.partia) >= 3
                    """
                    df_full, err2 = odpal_zapytanie_sql(sql_full, force_engine='postgres')
                    if err2:
                        st.error(f"Błąd Postgres: {err2}")
                        st.stop()
                    
                    st.session_state["_bilans_pelny_obraz"] = {
                        "sprzedaz": df_sprz,
                        "produkcja": df_full
                    }
                except Exception as e:
                    st.error(f"Błąd: {e}")
                    st.stop()
        
        dane = st.session_state.get("_bilans_pelny_obraz")
        if dane is None:
            st.info("Kliknij '🔄 Pobierz świeże dane' żeby załadować raport.")
        else:
            df_sprz = dane["sprzedaz"]
            df_full = dane["produkcja"]
            
            if df_sprz.empty or df_full.empty:
                st.warning("Brak danych — sprawdź połączenia z bazami.")
            else:
                # Łączenie
                df_sprz["skrzynia"] = df_sprz["skrzynia"].astype(str)
                df_full["skrzynia"] = df_full["skrzynia"].astype(str)
                
                df = df_sprz.merge(df_full, on="skrzynia", how="inner")
                
                # Cena PLN — heurystyka 70/30 EUR/PLN
                df["sr_cena_pln"] = df["sr_cena_w_walucie"].astype(float) * (kurs_eur_full * 0.7 + 1.0 * 0.3)
                df["sr_koszt_czesci"] = df["sr_koszt_czesci"].astype(float)
                df["marza_pln"] = df["sr_cena_pln"] - df["sr_koszt_czesci"]
                df["marza_pct"] = (df["marza_pln"] / df["sr_cena_pln"] * 100).round(1)
                
                # Filtruj wartości NULL
                df["prog_dzwonka"] = df["prog_dzwonka"].fillna(0).astype(float)
                df["dzwonil_razy"] = df["dzwonil_razy"].fillna(0).astype(int)
                df["dzwonil_15x"] = df["dzwonil_15x"].fillna(0).astype(int)
                df["dzwonil_2x"] = df["dzwonil_2x"].fillna(0).astype(int)
                df["sztuk_wyprodukowanych"] = df["sztuk_wyprodukowanych"].astype(int)
                df["sztuk_sprzedanych"] = df["sztuk_sprzedanych"].astype(int)
                
                # KPI globalne
                st.markdown("### 📊 Podsumowanie 3mc")
                kpi_cols = st.columns(5)
                with kpi_cols[0]:
                    st.metric("Skrzyń w analizie", f"{len(df)}")
                with kpi_cols[1]:
                    st.metric("Suma sprzedaży", f"{df['sztuk_sprzedanych'].sum():,} szt")
                with kpi_cols[2]:
                    st.metric("Suma produkcji", f"{df['sztuk_wyprodukowanych'].sum():,} szt")
                with kpi_cols[3]:
                    deficytowe = (df['marza_pln'] < 0).sum()
                    st.metric("🔴 Deficytowe", f"{deficytowe}", delta_color="inverse")
                with kpi_cols[4]:
                    dzwoniacych = (df['dzwonil_razy'] > 0).sum()
                    st.metric("Z dzwonkami", f"{dzwoniacych}")
                
                # Tabela pełna posortowana po marży rosnąco
                st.markdown("### 📋 Pełna tabela per skrzynia (sortowana od najgorszej marży)")
                df_show = df.sort_values("marza_pln")[
                    ["skrzynia", "sztuk_sprzedanych", "sr_cena_w_walucie", "sr_cena_pln",
                     "sztuk_wyprodukowanych", "sr_koszt_czesci", "max_koszt_czesci",
                     "marza_pln", "marza_pct",
                     "prog_dzwonka", "dzwonil_razy", "dzwonil_15x", "dzwonil_2x"]
                ].copy()
                df_show.columns = [
                    "Skrzynia", "Sprzed. szt", "Śr. cena waluta", "Śr. cena PLN",
                    "Wyprod. szt", "Śr. koszt cz.", "Max koszt",
                    "Marża /szt", "Marża %",
                    "Próg dzwonka", "Dzwonił × próg", "× 1.5", "× 2"
                ]
                
                def koloruj_pelny(row):
                    marza = row["Marża /szt"]
                    if marza < 0:
                        return ['background-color: #ffcccc; color: #000000; font-weight: 600'] * len(row)
                    elif marza < 500:
                        return ['background-color: #fff3a8; color: #000000'] * len(row)
                    else:
                        return ['background-color: #c8e6c9; color: #000000'] * len(row)
                
                styled = df_show.style.apply(koloruj_pelny, axis=1).format({
                    "Śr. cena waluta": "{:.0f}",
                    "Śr. cena PLN": "{:,.0f}",
                    "Śr. koszt cz.": "{:,.0f}",
                    "Max koszt": "{:,.0f}",
                    "Marża /szt": "{:+,.0f}",
                    "Marża %": "{:+.1f}%",
                    "Próg dzwonka": "{:,.0f}"
                })
                st.dataframe(styled, use_container_width=True, hide_index=True, height=600)
                
                # Eksport CSV
                csv_data = df_show.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    "📥 Pobierz pełny raport CSV",
                    data=csv_data,
                    file_name=f"pelny_obraz_skrzyn_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime='text/csv'
                )
                
                # Wnioski
                st.info(f"""
                **Co widzisz:**
                - **Sprzed./Wyprod. szt** — wolumen sprzedaży vs produkcja w 3mc (mogą się różnić bo skrzynia z stocku)
                - **Śr. cena waluta** — surowa średnia z zamówień (mix PLN i EUR)
                - **Śr. cena PLN** — przeliczona heurystyką 70/30 EUR/PLN
                - **Marża /szt** = cena PLN - koszt części (BEZ kosztów pozostałych — surowy wynik materiałowy)
                - **Próg dzwonka** = max wartość poborów części przed alertem
                - **Dzwonił × próg / × 1.5 / × 2** = ile partii przekroczyło dany mnożnik
                
                **🚨 Czerwone wiersze** = strata materiałowa (cena < koszt), wymagają natychmiastowej interwencji.  
                **⚠️ Żółte** = niska marża <500 zł, drobny wzrost kosztu = strata.  
                **✅ Zielone** = OK marża, ale i tak warto sprawdzać dzwonek (jeśli dzwoni za często — próg do aktualizacji).
                """)


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
