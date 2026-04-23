"""
=============================================================================
DASHBOARD RENTOWNOŚCI DSG — Streamlit Cloud (all-in-one)
=============================================================================
Architektura:
  Streamlit Cloud → SSH tunel → serwer PMG → PostgreSQL
                 ↓
              Firestore (cache wyników, snapshoty historyczne)
=============================================================================
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from decimal import Decimal
import json
import firebase_admin
from firebase_admin import credentials, firestore

# --- KONFIGURACJA ---
st.set_page_config(
    page_title="Rentowność DSG",
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="expanded"
)

# --- FIREBASE ---
@st.cache_resource
def init_firebase():
    if not firebase_admin._apps:
        creds_dict = json.loads(st.secrets["FIREBASE_CREDS"])
        cred = credentials.Certificate(creds_dict)
        firebase_admin.initialize_app(cred)
    return firestore.client()

db = init_firebase()

# --- BRAMKA HASŁA ---
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
# HELPER — OTWIERANIE TUNELU SSH
# ==========================================
def otworz_tunel():
    """Context manager — zwraca (tunnel, config) lub None przy błędzie."""
    from sshtunnel import SSHTunnelForwarder
    ssh_host = st.secrets["SSH_HOST"]
    ssh_port = int(st.secrets["SSH_PORT"])
    ssh_user = st.secrets["SSH_USER"]
    ssh_password = st.secrets["SSH_PASSWORD"]
    pg_host = st.secrets["PG_HOST"]
    pg_port = int(st.secrets["PG_PORT"])
    return SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_password=ssh_password,
        remote_bind_address=(pg_host, pg_port),
        local_bind_address=('127.0.0.1', 0),
    )


def pg_connect(tunnel, dbname):
    """Łączy z PostgreSQL przez otwarty tunel."""
    import psycopg2
    return psycopg2.connect(
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        user=st.secrets["PG_USER"],
        password=st.secrets["PG_PASSWORD"],
        dbname=dbname,
        options="-c statement_timeout=300000",
        connect_timeout=15,
    )


# ==========================================
# DIAGNOSTYKA — WYKRYWANIE STRUKTURY BAZY
# ==========================================
def diagnostyka():
    """
    Lista baz + wszystkich widoków zawierających 'raport' lub 'rentow'.
    Nic nie pobiera z widoków — tylko metadata. Zero obciążenia.
    """
    wyniki = {
        "bazy": [],
        "widoki_rentownosci": {},  # {db_name: [(schema, view), ...]}
        "wszystkie_schematy": {},
    }

    try:
        with otworz_tunel() as tunnel:
            st.info(f"✅ Tunel SSH otwarty (lokalny port: {tunnel.local_bind_port})")

            # Krok 1: lista baz (łączymy się z "postgres" lub "template1")
            for sys_db in ["postgres", "template1", "maggo"]:
                try:
                    conn = pg_connect(tunnel, sys_db)
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT datname FROM pg_database 
                        WHERE datistemplate = false 
                        ORDER BY datname
                    """)
                    wyniki["bazy"] = [r[0] for r in cur.fetchall()]
                    conn.close()
                    break
                except Exception:
                    continue

            if not wyniki["bazy"]:
                st.error("Nie udało się pobrać listy baz.")
                return wyniki

            st.success(f"📋 Dostępne bazy: {', '.join(wyniki['bazy'])}")

            # Krok 2: dla każdej bazy — szukaj widoków z 'raport' lub 'rentow'
            for db_name in wyniki["bazy"]:
                if db_name in ("postgres", "template0", "template1"):
                    continue

                try:
                    conn = pg_connect(tunnel, db_name)
                    cur = conn.cursor()

                    # Wszystkie widoki z 'raport'/'rentow' we WSZYSTKICH schematach
                    cur.execute("""
                        SELECT schemaname, viewname 
                        FROM pg_views 
                        WHERE (viewname ILIKE '%raport%' 
                            OR viewname ILIKE '%rentow%'
                            OR viewname ILIKE '%rpt%')
                        AND schemaname NOT IN ('pg_catalog', 'information_schema')
                        ORDER BY schemaname, viewname
                    """)
                    widoki = cur.fetchall()
                    if widoki:
                        wyniki["widoki_rentownosci"][db_name] = widoki

                    # Lista schematów użytkownika
                    cur.execute("""
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                        ORDER BY schema_name
                    """)
                    wyniki["wszystkie_schematy"][db_name] = [r[0] for r in cur.fetchall()]

                    conn.close()
                except Exception as e:
                    st.warning(f"Baza {db_name}: {str(e)[:100]}")
                    continue

    except Exception as e:
        st.error(f"❌ Błąd tunelu SSH: {e}")

    return wyniki


# ==========================================
# POŁĄCZENIE Z BAZĄ (POBRANIE DANYCH)
# ==========================================
def pobierz_z_bazy(full_query=None, source_info=None):
    """
    Pobiera dane z bazy przez SSH tunel.
    
    full_query: (db_name, schema, view) — jeśli podane, odpytuje dokładnie to.
    Jeśli None — próbuje auto-wykryć przez diagnostykę.
    """
    try:
        from sshtunnel import SSHTunnelForwarder
        import psycopg2
    except ImportError:
        st.error("Brakuje bibliotek: `sshtunnel`, `psycopg2-binary`. Sprawdź requirements.txt")
        return None, None

    progress = st.progress(0, text="🔒 Otwieram tunel SSH...")

    try:
        with otworz_tunel() as tunnel:
            progress.progress(30, text=f"✅ Tunel otwarty")

            # Lista prób — albo z jawnej ścieżki, albo auto
            if full_query:
                db_name, schema, view = full_query
                attempts = [(db_name, f'SELECT * FROM "{schema}"."{view}"')]
            else:
                # Auto-dyskwria — najpierw szukaj gdzie siedzi widok
                progress.progress(40, text="🔍 Szukam widoków rentowności...")
                candidates = []
                for db_name in ["maggo", "postgres"]:
                    try:
                        conn = pg_connect(tunnel, db_name)
                        cur = conn.cursor()
                        cur.execute("""
                            SELECT schemaname, viewname 
                            FROM pg_views 
                            WHERE viewname ILIKE '%rentow%'
                            AND schemaname NOT IN ('pg_catalog', 'information_schema')
                            ORDER BY 
                                CASE WHEN viewname = 'RaportRentownosciVer1' THEN 0 
                                     WHEN viewname = 'RaportRentownosciBezTrendVer1' THEN 1
                                     ELSE 2 END,
                                viewname
                        """)
                        for schema, view in cur.fetchall():
                            candidates.append((db_name, schema, view))
                        conn.close()
                    except:
                        continue

                if not candidates:
                    progress.empty()
                    st.error("❌ Nie znaleziono żadnego widoku rentowności. Uruchom 'Diagnostyka'.")
                    return None, None

                attempts = [(db, f'SELECT * FROM "{schema}"."{view}"') for db, schema, view in candidates]

            # Próbuj kolejne kombinacje
            last_error = None
            for db_name, query in attempts:
                progress.progress(60, text=f"📊 Odpytuję {db_name}...")
                try:
                    conn = pg_connect(tunnel, db_name)
                    df = pd.read_sql(query, conn)
                    conn.close()

                    if not df.empty:
                        progress.progress(100, text=f"✅ Pobrano {len(df)} wierszy")
                        progress.empty()

                        # Wyciągnij nazwę widoku z zapytania
                        view_name = query.split('"')[-2] if '"' in query else "?"

                        meta = {
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "source_view": view_name,
                            "source_db": db_name,
                            "source_query": query,
                            "row_count": len(df),
                            "columns": list(df.columns),
                        }
                        return df, meta
                    else:
                        last_error = f"{db_name}: widok pusty"
                except Exception as e:
                    last_error = f"{db_name}: {str(e)[:150]}"
                    st.caption(f"   Próba {db_name}: {str(e)[:100]}")
                    continue

            progress.empty()
            st.error(f"❌ Nie udało się pobrać danych. Ostatni błąd: {last_error}")
            return None, None

    except Exception as e:
        progress.empty()
        st.error(f"❌ Błąd tunelu SSH: {e}")
        return None, None


def zapisz_do_firestore(df, meta):
    """Cache'uje dane do Firestore."""
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

    payload = {"meta": meta, "data": records}

    db.collection("rentownosc_raporty").document("latest").set(payload)
    snap_id = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M')}"
    db.collection("rentownosc_raporty").document(snap_id).set(payload)

    try:
        all_docs = db.collection("rentownosc_raporty").list_documents()
        snapshots = sorted([d.id for d in all_docs if d.id.startswith("snapshot_")], reverse=True)
        for old_id in snapshots[30:]:
            db.collection("rentownosc_raporty").document(old_id).delete()
    except:
        pass


@st.cache_data(ttl=300)
def pobierz_z_firestore(doc_id="latest"):
    doc = db.collection("rentownosc_raporty").document(doc_id).get()
    if not doc.exists:
        return None, None
    payload = doc.to_dict()
    meta = payload.get("meta", {})
    records = payload.get("data", [])
    if not records:
        return None, meta
    df = pd.DataFrame(records)
    for col in df.columns:
        if col in ("grupaIgo", "DataOd", "DataDo"):
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df, meta


def find_col(df, *candidates):
    df_cols_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        key = c.lower()
        if key in df_cols_lower:
            return df_cols_lower[key]
    return None


# --- KOLORY ---
COLORS = {
    "ebay": "#3B82F6", "shopEU": "#10B981", "allegro": "#F59E0B", "shopPL": "#8B5CF6",
    "pobor": "#EF4444", "skup": "#F97316", "tworcy": "#A855F7", "sprzedawcy": "#EC4899",
    "piask": "#6B7280", "spedycja": "#0EA5E9",
    "prowizja_ebay": "#2563EB", "prowizja_allegro": "#D97706",
}


# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.title("📊 Rentowność DSG")
    st.markdown("---")

    # --- DIAGNOSTYKA ---
    st.subheader("🔍 Diagnostyka")
    if st.button("Zbadaj strukturę bazy"):
        st.session_state["diagnostyka_wynik"] = diagnostyka()

    st.markdown("---")

    # --- ODŚWIEŻANIE ---
    st.subheader("🔄 Pobieranie danych")

    # Jeśli mamy wynik diagnostyki — pokaż wybór widoku
    diag = st.session_state.get("diagnostyka_wynik")
    custom_source = None

    if diag and diag["widoki_rentownosci"]:
        wszystkie = []
        for db_name, widoki in diag["widoki_rentownosci"].items():
            for schema, view in widoki:
                wszystkie.append((db_name, schema, view))

        if wszystkie:
            opcje = [f"{db}.{sch}.{v}" for db, sch, v in wszystkie]
            wybor = st.selectbox("Widok do pobrania:", opcje, key="wybor_widoku")
            idx = opcje.index(wybor)
            custom_source = wszystkie[idx]

    now = datetime.now()
    is_working_hours = 7 <= now.hour <= 21

    if is_working_hours:
        st.warning(f"⚠️ Godziny produkcyjne ({now.strftime('%H:%M')})")
        confirm_daytime = st.checkbox("Rozumiem, odpytuję mimo to")
        can_refresh = confirm_daytime
    else:
        st.success(f"🌙 Pora nocna ({now.strftime('%H:%M')})")
        can_refresh = True

    if st.button("⚡ Pobierz dane", type="primary", disabled=not can_refresh):
        with st.spinner("Łączenie z produkcją..."):
            df_new, meta_new = pobierz_z_bazy(full_query=custom_source)
            if df_new is not None:
                zapisz_do_firestore(df_new, meta_new)
                st.cache_data.clear()
                st.success(f"✅ Pobrano {meta_new['row_count']} wierszy")
                st.rerun()

    st.markdown("---")

    # --- SNAPSHOTY ---
    st.subheader("📅 Snapshoty")
    all_docs = list(db.collection("rentownosc_raporty").list_documents())
    snapshot_ids = sorted([d.id for d in all_docs], reverse=True)

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
                if len(parts) >= 13:
                    formatted = f"{parts[:4]}-{parts[4:6]}-{parts[6:8]} {parts[9:11]}:{parts[11:13]}"
                    display_names[sid] = f"📷 {formatted}"
                else:
                    display_names[sid] = f"📷 {parts}"

        selected_snapshot = st.selectbox(
            "Snapshot:",
            snapshot_ids,
            format_func=lambda x: display_names.get(x, x),
        )

    st.markdown("---")

    # --- FILTRY ---
    st.subheader("🔧 Filtry")
    top_n = st.slider("Top N grup:", 5, 50, 20)
    min_skrzyn = st.number_input("Min. ilość skrzyń:", value=0, min_value=0)

    st.markdown("---")
    st.subheader("📈 Wykresy")
    show_zysk = st.checkbox("Zysk per kanał", value=True)
    show_koszty = st.checkbox("Struktura kosztów", value=True)
    show_cena_vs_koszt = st.checkbox("Cena vs Koszt (eBay)", value=True)
    show_porownanie_cen = st.checkbox("Porównanie cen kanałów", value=True)
    show_wolumen = st.checkbox("Wolumen i trend", value=True)
    show_heatmapa = st.checkbox("Heatmapa zysków", value=True)
    show_tabela = st.checkbox("Tabela surowa", value=False)


# ==========================================
# GŁÓWNY INTERFEJS
# ==========================================

# --- Wyświetl wynik diagnostyki ---
if st.session_state.get("diagnostyka_wynik"):
    diag = st.session_state["diagnostyka_wynik"]
    with st.expander("🔍 Wynik diagnostyki", expanded=True):
        if diag["bazy"]:
            st.write(f"**Dostępne bazy:** {', '.join(diag['bazy'])}")

        if diag["widoki_rentownosci"]:
            st.write("**Znalezione widoki rentowności:**")
            for db_name, widoki in diag["widoki_rentownosci"].items():
                for schema, view in widoki:
                    full = f"{db_name}.{schema}.{view}"
                    st.code(full, language=None)
        else:
            st.warning("⚠️ Nie znaleziono widoków z 'raport'/'rentow' w nazwie.")

        if diag["wszystkie_schematy"]:
            st.write("**Schematy per baza:**")
            for db_name, schematy in diag["wszystkie_schematy"].items():
                st.caption(f"• {db_name}: {', '.join(schematy)}")

        st.info("👈 Wybierz widok w panelu bocznym i kliknij 'Pobierz dane'")

# --- Dashboard ---
if not snapshot_ids or selected_snapshot is None:
    st.info("👈 Kliknij **'Zbadaj strukturę bazy'** → wybierz widok → **'Pobierz dane'**")
    st.stop()

df, meta = pobierz_z_firestore(selected_snapshot)

if df is None:
    st.warning("⚠️ Wybrany snapshot jest pusty.")
    st.stop()

col_info1, col_info2, col_info3, col_info4 = st.columns(4)
col_info1.metric("Grup igorowych", meta.get("row_count", len(df)))
col_info2.metric("Źródło", meta.get("source_view", "?"))
col_info3.metric("Baza", meta.get("source_db", "?"))
col_info4.metric("Aktualizacja", meta.get("updated_at", "?")[:16])
st.markdown("---")

# --- Identyfikacja kolumn ---
COL_GRUPA = find_col(df, "grupaIgo")
COL_ILE = find_col(df, "IleSkrzyn")
COL_POBOR = find_col(df, "SrPoborCzesci")
COL_SKUP = find_col(df, "SrKosztSkupu")
COL_PKT_TWORCY = find_col(df, "SrPktTworcy")
COL_PKT_SPRZED = find_col(df, "SrPktSprzedawcy")
COL_PIASK = find_col(df, "SrPiaskPakowanie_PLN")
COL_SPED = find_col(df, "SrKosztSpedycjiPlEu")
COL_PROW_EBAY = find_col(df, "SrProwizjaEbay_PLN")
COL_PROW_ALLEGRO = find_col(df, "SrProwizjaAllegro_PLN")
COL_CENA_EBAY = find_col(df, "SrCenaSprzed_Ebay_PLN")
COL_CENA_SHOPEU = find_col(df, "SrCenaSprzed_shopEU_PLN")
COL_CENA_ALLEGRO = find_col(df, "SrCenaSprzed_Allegro_PLN")
COL_CENA_SHOPPL = find_col(df, "SrCenaSprzed_shopPL_PLN")
COL_BAZA_EBAY = find_col(df, "AvgLastCenaBazowa0_Ebay_PLN")
COL_BAZA_SHOPEU = find_col(df, "AvgLastCenaBazowa0_shopEU_PLN")
COL_BAZA_ALLEGRO = find_col(df, "AvgLastCenaBazowa0_Allegro_PLN")
COL_BAZA_SHOPPL = find_col(df, "AvgLastCenaBazowa0_shopPL_PLN")
COL_KOSZT_EBAY = find_col(df, "KosztPozyskEbay")
COL_KOSZT_SHOPEU = find_col(df, "KosztPozyskShopEu")
COL_KOSZT_ALLEGRO = find_col(df, "KosztPozyskAllegro")
COL_KOSZT_SHOPPL = find_col(df, "KosztPozyskShopPL")
COL_ZYSK_EBAY = find_col(df, "ebay")
COL_ZYSK_SHOPEU = find_col(df, "shopEU")
COL_ZYSK_ALLEGRO = find_col(df, "Allegro")
COL_ZYSK_SHOPPL = find_col(df, "shopPL")
COL_TREND = find_col(df, "TrendPoboruCzesci")

if not COL_GRUPA:
    st.error("❌ Nie znaleziono kolumny grupaIgo.")
    st.write("Dostępne kolumny:", list(df.columns))
    st.stop()

if COL_ILE:
    df = df.sort_values(COL_ILE, ascending=False, na_position="last")
    if min_skrzyn > 0:
        df = df[df[COL_ILE] >= min_skrzyn]
df_top = df.head(top_n).copy()

if df_top.empty:
    st.warning("⚠️ Brak danych po zastosowaniu filtrów.")
    st.stop()

st.subheader("📊 Podsumowanie")
m1, m2, m3, m4 = st.columns(4)
if COL_ILE:
    m1.metric("Łączna ilość skrzyń", f"{df[COL_ILE].sum():,.0f}")
if COL_ZYSK_EBAY:
    avg_zysk = df[COL_ZYSK_EBAY].mean()
    m2.metric("Śr. zysk eBay / szt.", f"{avg_zysk:,.0f} PLN" if pd.notna(avg_zysk) else "—")
if COL_ZYSK_SHOPEU:
    avg_zysk_shop = df[COL_ZYSK_SHOPEU].mean()
    m3.metric("Śr. zysk Shop EU / szt.", f"{avg_zysk_shop:,.0f} PLN" if pd.notna(avg_zysk_shop) else "—")
if COL_POBOR:
    avg_pobor = df[COL_POBOR].mean()
    m4.metric("Śr. pobór części / szt.", f"{avg_pobor:,.0f} PLN" if pd.notna(avg_pobor) else "—")

st.markdown("---")


def layout_base(title, height=500):
    return dict(
        title=title, height=height, template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(b=120, l=60, r=30, t=60),
        xaxis_tickangle=-45,
        font=dict(family="Segoe UI, sans-serif", size=12),
    )

if show_zysk:
    st.subheader("💰 Zysk na skrzyni per kanał sprzedaży")
    fig = go.Figure()
    for col, name, color in [
        (COL_ZYSK_EBAY, "eBay", COLORS["ebay"]),
        (COL_ZYSK_SHOPEU, "Shop EU", COLORS["shopEU"]),
        (COL_ZYSK_ALLEGRO, "Allegro", COLORS["allegro"]),
        (COL_ZYSK_SHOPPL, "Shop PL", COLORS["shopPL"]),
    ]:
        if col and col in df_top.columns:
            fig.add_trace(go.Bar(
                name=name, x=df_top[COL_GRUPA], y=df_top[col],
                marker_color=color, opacity=0.85,
                hovertemplate=f"{name}<br>%{{x}}<br>Zysk: %{{y:,.0f}} PLN<extra></extra>"
            ))
    fig.update_layout(**layout_base("Zysk na skrzyni per kanał (PLN)"), barmode="group", yaxis_title="Zysk (PLN)")
    fig.add_hline(y=0, line_dash="dot", line_color="red", opacity=0.5)
    st.plotly_chart(fig, use_container_width=True)


if show_koszty:
    st.subheader("🔧 Struktura kosztów pozyskania skrzyni")
    fig = go.Figure()
    for col, name, color in [
        (COL_POBOR, "Pobór części", COLORS["pobor"]),
        (COL_SKUP, "Koszt skupu", COLORS["skup"]),
        (COL_PKT_TWORCY, "Punkty twórcy", COLORS["tworcy"]),
        (COL_PKT_SPRZED, "Punkty sprzedawcy", COLORS["sprzedawcy"]),
        (COL_PIASK, "Piaskowanie + pak.", COLORS["piask"]),
        (COL_SPED, "Spedycja", COLORS["spedycja"]),
        (COL_PROW_EBAY, "Prowizja eBay", COLORS["prowizja_ebay"]),
        (COL_PROW_ALLEGRO, "Prowizja Allegro", COLORS["prowizja_allegro"]),
    ]:
        if col and col in df_top.columns:
            fig.add_trace(go.Bar(
                name=name, x=df_top[COL_GRUPA], y=df_top[col].fillna(0),
                marker_color=color,
                hovertemplate=f"{name}<br>%{{x}}<br>%{{y:,.0f}} PLN<extra></extra>"
            ))
    fig.update_layout(**layout_base("Rozbicie kosztów pozyskania (PLN)"), barmode="stack", yaxis_title="Koszt (PLN)")
    st.plotly_chart(fig, use_container_width=True)


if show_cena_vs_koszt and COL_CENA_EBAY and COL_KOSZT_EBAY:
    st.subheader("🏷️ eBay: Cena sprzedaży vs Koszt pozyskania")
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Śr. cena sprzedaży eBay", x=df_top[COL_GRUPA], y=df_top[COL_CENA_EBAY],
                        marker_color=COLORS["ebay"], opacity=0.7))
    fig.add_trace(go.Bar(name="Koszt pozyskania eBay", x=df_top[COL_GRUPA], y=df_top[COL_KOSZT_EBAY],
                        marker_color=COLORS["pobor"], opacity=0.7))
    if COL_BAZA_EBAY and COL_BAZA_EBAY in df_top.columns:
        fig.add_trace(go.Scatter(name="Cena bazowa 0-0", x=df_top[COL_GRUPA], y=df_top[COL_BAZA_EBAY],
                                mode="markers+lines", marker=dict(size=8, color="#1D4ED8"), line=dict(dash="dot")))
    fig.update_layout(**layout_base("eBay: Cena vs Koszt (PLN)"), barmode="group", yaxis_title="PLN")
    st.plotly_chart(fig, use_container_width=True)


if show_porownanie_cen:
    st.subheader("📈 Średnia cena sprzedaży per kanał")
    col_left, col_right = st.columns(2)

    with col_left:
        fig_a = go.Figure()
        for col_name, name, color in [
            (COL_CENA_EBAY, "eBay śr.", COLORS["ebay"]),
            (COL_CENA_SHOPEU, "Shop EU śr.", COLORS["shopEU"]),
            (COL_CENA_ALLEGRO, "Allegro śr.", COLORS["allegro"]),
            (COL_CENA_SHOPPL, "Shop PL śr.", COLORS["shopPL"]),
        ]:
            if col_name and col_name in df_top.columns:
                fig_a.add_trace(go.Scatter(name=name, x=df_top[COL_GRUPA], y=df_top[col_name],
                                          mode="markers+lines", marker=dict(size=6, color=color), line=dict(color=color)))
        fig_a.update_layout(**layout_base("Ceny sprzedaży — średnie (PLN)", height=450), yaxis_title="Cena (PLN)")
        st.plotly_chart(fig_a, use_container_width=True)

    with col_right:
        fig_b = go.Figure()
        for col_name, name, color in [
            (COL_BAZA_EBAY, "eBay 0-0", COLORS["ebay"]),
            (COL_BAZA_SHOPEU, "Shop EU 0-0", COLORS["shopEU"]),
            (COL_BAZA_ALLEGRO, "Allegro 0-0", COLORS["allegro"]),
            (COL_BAZA_SHOPPL, "Shop PL 0-0", COLORS["shopPL"]),
        ]:
            if col_name and col_name in df_top.columns:
                fig_b.add_trace(go.Scatter(name=name, x=df_top[COL_GRUPA], y=df_top[col_name],
                                          mode="markers+lines", marker=dict(size=6, color=color),
                                          line=dict(color=color, dash="dot")))
        fig_b.update_layout(**layout_base("Ceny bazowe 0-0 (PLN)", height=450), yaxis_title="Cena (PLN)")
        st.plotly_chart(fig_b, use_container_width=True)


if show_wolumen:
    st.subheader("📦 Wolumen produkcji i trend kosztów poboru")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if COL_ILE:
        fig.add_trace(go.Bar(name="Ilość skrzyń", x=df_top[COL_GRUPA], y=df_top[COL_ILE],
                            marker_color="#6366F1", opacity=0.7), secondary_y=False)
    if COL_TREND and COL_TREND in df_top.columns:
        trend_data = df_top[COL_TREND]
        if trend_data.notna().any():
            colors_trend = ["#EF4444" if pd.notna(v) and v < 1 else "#10B981" for v in trend_data]
            fig.add_trace(go.Scatter(name="Trend poboru (vs 3mce)", x=df_top[COL_GRUPA], y=trend_data,
                                    mode="markers+lines", marker=dict(size=9, color=colors_trend),
                                    line=dict(color="#6B7280", dash="dot")), secondary_y=True)
    fig.update_layout(**layout_base("Wolumen i trend", height=500))
    fig.update_yaxes(title_text="Ilość skrzyń", secondary_y=False)
    fig.update_yaxes(title_text="Trend (1.0 = bez zmian)", secondary_y=True)
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.3, secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)


if show_heatmapa:
    st.subheader("🗺️ Mapa zysków: Grupa × Kanał")
    kanaly_heat = [
        ("eBay", COL_ZYSK_EBAY), ("Shop EU", COL_ZYSK_SHOPEU),
        ("Allegro", COL_ZYSK_ALLEGRO), ("Shop PL", COL_ZYSK_SHOPPL),
    ]
    available_heat = [(k, c) for k, c in kanaly_heat if c and c in df_top.columns]
    if available_heat:
        z_data = [df_top[col].fillna(0).tolist() for _, col in available_heat]
        fig = go.Figure(go.Heatmap(
            z=z_data, x=df_top[COL_GRUPA].tolist(), y=[k for k, _ in available_heat],
            colorscale=[[0, "#EF4444"], [0.35, "#FEF3C7"], [0.5, "#F5F5F5"], [0.65, "#D1FAE5"], [1, "#10B981"]],
            zmid=0,
            text=[[f"{v:.0f}" for v in row] for row in z_data],
            texttemplate="%{text}", textfont=dict(size=11),
            hovertemplate="Grupa: %{x}<br>Kanał: %{y}<br>Zysk: %{z:,.0f} PLN<extra></extra>",
        ))
        fig.update_layout(**layout_base("Zysk per grupa × kanał (PLN — czerwony=strata, zielony=zysk)", height=400))
        st.plotly_chart(fig, use_container_width=True)


if show_tabela:
    st.subheader("📋 Dane surowe")
    st.dataframe(
        df_top.style.format({col: "{:,.0f}" for col in df_top.select_dtypes(include="number").columns}),
        use_container_width=True, hide_index=True,
    )

st.markdown("---")
st.caption(
    f"Dashboard z Firestore (`rentownosc_raporty/{selected_snapshot}`). "
    f"Dane: {meta.get('updated_at', '?')} | Widok: {meta.get('source_view', '?')} | Baza: {meta.get('source_db', '?')}"
)
