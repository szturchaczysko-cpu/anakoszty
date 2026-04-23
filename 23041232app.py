"""
DASHBOARD RENTOWNOŚCI DSG — Streamlit Cloud
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
                st.error("Nie udało się pobrać listy baz.")
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
        st.error(f"❌ Błąd tunelu: {e}")
    return wyniki


def pobierz_z_bazy(full_query=None):
    try:
        from sshtunnel import SSHTunnelForwarder
        import psycopg2
    except ImportError:
        st.error("Brakuje bibliotek.")
        return None, None

    progress = st.progress(0, text="🔒 Otwieram tunel...")
    try:
        with otworz_tunel() as tunnel:
            progress.progress(30, text="✅ Tunel otwarty")
            if full_query:
                db_name, schema, view = full_query
                attempts = [(db_name, f'SELECT * FROM "{schema}"."{view}"')]
            else:
                candidates = []
                for db_name in ["maggo", "postgres"]:
                    try:
                        conn = pg_connect(tunnel, db_name)
                        cur = conn.cursor()
                        cur.execute("""
                            SELECT schemaname, viewname FROM pg_views 
                            WHERE viewname ILIKE '%rentow%'
                            AND schemaname NOT IN ('pg_catalog', 'information_schema')
                        """)
                        for schema, view in cur.fetchall():
                            candidates.append((db_name, schema, view))
                        conn.close()
                    except:
                        continue
                if not candidates:
                    progress.empty()
                    st.error("❌ Brak widoków rentowności.")
                    return None, None
                attempts = [(db, f'SELECT * FROM "{schema}"."{view}"') for db, schema, view in candidates]

            last_error = None
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
                    else:
                        last_error = f"{db_name}: pusty"
                except Exception as e:
                    last_error = f"{db_name}: {str(e)[:150]}"
                    st.caption(f"   {db_name}: {str(e)[:100]}")
                    continue
            progress.empty()
            st.error(f"❌ {last_error}")
            return None, None
    except Exception as e:
        progress.empty()
        st.error(f"❌ Błąd tunelu: {e}")
        return None, None


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


def zapisz_do_firestore(df, meta):
    import time
    from google.api_core import exceptions as gcp_exceptions

    records = df_to_records(df)
    num_chunks = (len(records) + CHUNK_SIZE - 1) // CHUNK_SIZE
    meta["num_chunks"] = num_chunks
    meta["chunk_size"] = CHUNK_SIZE

    snap_id = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M')}"
    progress = st.progress(0, text=f"💾 Zapis {len(records)} wierszy w {num_chunks} częściach...")

    try:
        old = db.collection("rentownosc_raporty").document("latest").collection("chunks").list_documents()
        for doc in old:
            doc.delete()
    except Exception as e:
        st.caption(f"Czyszczenie: {e}")

    def try_write(ref, data, desc=""):
        for attempt in range(3):
            try:
                ref.set(data)
                return True
            except gcp_exceptions.DeadlineExceeded:
                st.caption(f"⏱️ Timeout {desc}, próba {attempt+2}/3...")
                time.sleep(3)
            except Exception as e:
                st.warning(f"Błąd {desc}: {e}")
                return False
        return False

    try_write(db.collection("rentownosc_raporty").document("latest"), {"meta": meta}, "meta")
    for i in range(num_chunks):
        chunk = records[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
        ref = db.collection("rentownosc_raporty").document("latest").collection("chunks").document(f"chunk_{i:04d}")
        try_write(ref, {"data": chunk, "index": i, "size": len(chunk)}, f"chunk {i}")
        progress.progress((i + 1) / num_chunks, text=f"💾 {i+1}/{num_chunks}")

    try:
        try_write(db.collection("rentownosc_raporty").document(snap_id), {"meta": meta}, "snap meta")
        for i in range(num_chunks):
            chunk = records[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
            ref = db.collection("rentownosc_raporty").document(snap_id).collection("chunks").document(f"chunk_{i:04d}")
            try_write(ref, {"data": chunk, "index": i, "size": len(chunk)}, f"snap {i}")
    except Exception as e:
        st.caption(f"Snapshot pominięty: {e}")

    progress.empty()

    try:
        all_docs = db.collection("rentownosc_raporty").list_documents()
        snapshots = sorted([d.id for d in all_docs if d.id.startswith("snapshot_")], reverse=True)
        for old_id in snapshots[10:]:
            old_chunks = db.collection("rentownosc_raporty").document(old_id).collection("chunks").list_documents()
            for doc in old_chunks:
                doc.delete()
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
        chunks_ref = db.collection("rentownosc_raporty").document(doc_id).collection("chunks")
        chunks = sorted(chunks_ref.stream(), key=lambda d: d.id)
        for chunk_doc in chunks:
            records.extend(chunk_doc.to_dict().get("data", []))

    if not records:
        return None, meta

    df = pd.DataFrame(records)
    # Agresywna konwersja numeryczna — jeśli >70% próbki jest liczbą, konwertuj całą kolumnę
    for col in df.columns:
        if col in ("TworcaMaggo", "NrPartii", "TypRap", "NazwaSerwera", "Nazwa",
                   "prokwident", "grupaIgo", "DataOd", "DataDo", "DataProduktu"):
            continue  # te kolumny zostawiamy jako tekst
        sample = df[col].dropna().head(100)
        if sample.empty:
            continue
        try:
            converted = pd.to_numeric(sample, errors="coerce")
            success_ratio = converted.notna().sum() / len(sample)
            if success_ratio >= 0.7:  # >=70% wartości to liczby
                df[col] = pd.to_numeric(df[col], errors="coerce")
        except:
            pass
    return df, meta


def find_col(df, *candidates):
    df_cols_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in df_cols_lower:
            return df_cols_lower[c.lower()]
    return None


COLORS = {
    "ebay": "#3B82F6", "shopEU": "#10B981", "allegro": "#F59E0B", "shopPL": "#8B5CF6",
    "pobor": "#EF4444", "skup": "#F97316", "tworcy": "#A855F7", "sprzedawcy": "#EC4899",
    "piask": "#6B7280", "spedycja": "#0EA5E9",
    "prowizja_ebay": "#2563EB", "prowizja_allegro": "#D97706",
}


with st.sidebar:
    st.title("📊 Rentowność DSG")
    st.markdown("---")
    st.subheader("🔍 Diagnostyka")
    if st.button("Zbadaj strukturę bazy"):
        st.session_state["diagnostyka_wynik"] = diagnostyka()

    st.markdown("---")
    st.subheader("🔄 Pobieranie danych")

    diag = st.session_state.get("diagnostyka_wynik")
    custom_source = None
    if diag and diag["widoki_rentownosci"]:
        wszystkie = []
        for db_name, widoki in diag["widoki_rentownosci"].items():
            for schema, view in widoki:
                wszystkie.append((db_name, schema, view))
        if wszystkie:
            opcje = [f"{db}.{sch}.{v}" for db, sch, v in wszystkie]
            wybor = st.selectbox("Widok:", opcje, key="wybor_widoku")
            idx = opcje.index(wybor)
            custom_source = wszystkie[idx]

    now = datetime.now()
    is_working_hours = 7 <= now.hour <= 21
    if is_working_hours:
        st.warning(f"⚠️ Godziny produkcyjne ({now.strftime('%H:%M')})")
        can_refresh = st.checkbox("Rozumiem, odpytuję mimo to")
    else:
        st.success(f"🌙 Pora nocna ({now.strftime('%H:%M')})")
        can_refresh = True

    if st.button("⚡ Pobierz dane", type="primary", disabled=not can_refresh):
        with st.spinner("Łączenie z produkcją..."):
            df_new, meta_new = pobierz_z_bazy(full_query=custom_source)
            if df_new is not None:
                try:
                    zapisz_do_firestore(df_new, meta_new)
                    st.cache_data.clear()
                    st.success(f"✅ Zapisano {meta_new['row_count']} wierszy")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Zapis: {e}")

    st.markdown("---")
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
                    display_names[sid] = f"📷 {parts[:4]}-{parts[4:6]}-{parts[6:8]} {parts[9:11]}:{parts[11:13]}"
                else:
                    display_names[sid] = f"📷 {parts}"
        selected_snapshot = st.selectbox("Snapshot:", snapshot_ids, format_func=lambda x: display_names.get(x, x))

    st.markdown("---")
    st.subheader("🔧 Filtry")
    top_n = st.slider("Top N:", 5, 100, 30)
    min_skrzyn = st.number_input("Min. ilość:", value=0, min_value=0)

    st.markdown("---")
    st.subheader("📈 Wykresy (dashboard rentowności)")
    show_zysk = st.checkbox("Zysk per kanał", value=True)
    show_koszty = st.checkbox("Struktura kosztów", value=True)
    show_cena_vs_koszt = st.checkbox("Cena vs Koszt", value=True)
    show_porownanie_cen = st.checkbox("Porównanie cen", value=True)
    show_wolumen = st.checkbox("Wolumen i trend", value=True)
    show_heatmapa = st.checkbox("Heatmapa", value=True)
    show_tabela = st.checkbox("Tabela surowa", value=False)


if st.session_state.get("diagnostyka_wynik"):
    diag = st.session_state["diagnostyka_wynik"]
    with st.expander("🔍 Wynik diagnostyki", expanded=False):
        if diag["bazy"]:
            st.write(f"**Bazy:** {', '.join(diag['bazy'])}")
        if diag["widoki_rentownosci"]:
            st.write("**Widoki:**")
            for db_name, widoki in diag["widoki_rentownosci"].items():
                for schema, view in widoki:
                    st.code(f"{db_name}.{schema}.{view}", language=None)
        if diag["wszystkie_schematy"]:
            st.write("**Schematy:**")
            for db_name, schematy in diag["wszystkie_schematy"].items():
                st.caption(f"• {db_name}: {', '.join(schematy)}")

if not snapshot_ids or selected_snapshot is None:
    st.info("👈 Kliknij **'Zbadaj strukturę bazy'** → wybierz widok → **'Pobierz dane'**")
    st.stop()

with st.spinner("Ładowanie z Firestore..."):
    df, meta = pobierz_z_firestore(selected_snapshot)

if df is None:
    st.warning("⚠️ Snapshot pusty.")
    st.stop()

# Metryki nagłówkowe
c1, c2, c3, c4 = st.columns(4)
c1.metric("Wierszy", meta.get("row_count", len(df)))
c2.metric("Źródło", meta.get("source_view", "?"))
c3.metric("Baza", meta.get("source_db", "?"))
c4.metric("Aktualizacja", meta.get("updated_at", "?")[:16])

# Info o kolumnach (zawsze widoczne)
st.markdown("---")
with st.expander(f"📋 Struktura danych — {len(df.columns)} kolumn", expanded=True):
    col_info = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        non_null = df[col].notna().sum()
        nulls = df[col].isna().sum()
        sample_val = df[col].dropna().head(1)
        sample = str(sample_val.iloc[0])[:60] if len(sample_val) > 0 else "—"
        col_info.append({
            "Kolumna": col,
            "Typ": dtype,
            "Nie-null": non_null,
            "Null": nulls,
            "Przykład": sample,
        })
    st.dataframe(pd.DataFrame(col_info), use_container_width=True, hide_index=True)

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


# ==========================================
# WIDOK GENERYCZNY (brak grupaIgo)
# ==========================================
if not COL_GRUPA:
    st.info(f"ℹ️ Ten widok (**{meta.get('source_view')}**) nie ma kolumny `grupaIgo` — nie jest to raport rentowności. Możesz zrobić własną agregację poniżej.")

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    # Kolumny tekstowe/nienumeryczne
    text_cols = [c for c in df.columns if c not in numeric_cols]

    if not numeric_cols:
        st.error("❌ Brak kolumn numerycznych — nie da się zrobić agregacji.")
        st.subheader("📋 Podgląd danych")
        st.dataframe(df.head(500), use_container_width=True)
        st.stop()

    if not text_cols:
        st.warning("⚠️ Brak kolumn tekstowych do grupowania — pokazuję sumy wszystkich kolumn numerycznych.")
        sumy = df[numeric_cols].sum().reset_index()
        sumy.columns = ["Kolumna", "Suma"]
        st.dataframe(sumy, use_container_width=True, hide_index=True)
        st.stop()

    # Agregacja
    st.subheader("📊 Szybka agregacja")
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        group_col = st.selectbox("Grupuj po:", text_cols)
    with col_b:
        agg_col = st.selectbox("Agreguj:", numeric_cols)
    with col_c:
        agg_func = st.selectbox("Funkcja:", ["sum", "mean", "count", "max", "min"])

    try:
        # Grupowanie
        agg_result = df.groupby(group_col, dropna=True)[agg_col].agg(agg_func).reset_index()
        agg_result = agg_result.sort_values(agg_col, ascending=False).head(top_n)
        agg_result[group_col] = agg_result[group_col].astype(str)

        st.caption(f"**{agg_func.upper()}** od **{agg_col}** per **{group_col}** (Top {top_n})")

        # Wykres
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=agg_result[group_col],
            y=agg_result[agg_col],
            marker_color="#3B82F6",
            hovertemplate=f"{group_col}: %{{x}}<br>{agg_func}({agg_col}): %{{y:,.2f}}<extra></extra>"
        ))
        fig.update_layout(
            title=f"{agg_func.upper()}({agg_col}) per {group_col} — Top {top_n}",
            height=500, template="plotly_white",
            xaxis_tickangle=-45, margin=dict(b=150),
            yaxis_title=f"{agg_func}({agg_col})",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Tabela
        st.dataframe(agg_result, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Błąd agregacji: {e}")

    # Podgląd surowych danych (na samym końcu)
    st.markdown("---")
    with st.expander("📋 Podgląd surowych danych (pierwsze 500)", expanded=False):
        st.dataframe(df.head(500), use_container_width=True)

    st.stop()


# ==========================================
# PEŁNY DASHBOARD RENTOWNOŚCI (gdy jest grupaIgo)
# ==========================================
if COL_ILE:
    df = df.sort_values(COL_ILE, ascending=False, na_position="last")
    if min_skrzyn > 0:
        df = df[df[COL_ILE] >= min_skrzyn]
df_top = df.head(top_n).copy()

if df_top.empty:
    st.warning("⚠️ Brak danych po filtrach.")
    st.stop()

st.subheader("📊 Podsumowanie")
m1, m2, m3, m4 = st.columns(4)
if COL_ILE:
    m1.metric("Łączna ilość skrzyń", f"{df[COL_ILE].sum():,.0f}")
if COL_ZYSK_EBAY:
    v = df[COL_ZYSK_EBAY].mean()
    m2.metric("Śr. zysk eBay", f"{v:,.0f} PLN" if pd.notna(v) else "—")
if COL_ZYSK_SHOPEU:
    v = df[COL_ZYSK_SHOPEU].mean()
    m3.metric("Śr. zysk Shop EU", f"{v:,.0f} PLN" if pd.notna(v) else "—")
if COL_POBOR:
    v = df[COL_POBOR].mean()
    m4.metric("Śr. pobór części", f"{v:,.0f} PLN" if pd.notna(v) else "—")
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
    st.subheader("💰 Zysk per kanał")
    fig = go.Figure()
    for col, name, color in [(COL_ZYSK_EBAY, "eBay", COLORS["ebay"]),
                              (COL_ZYSK_SHOPEU, "Shop EU", COLORS["shopEU"]),
                              (COL_ZYSK_ALLEGRO, "Allegro", COLORS["allegro"]),
                              (COL_ZYSK_SHOPPL, "Shop PL", COLORS["shopPL"])]:
        if col and col in df_top.columns:
            fig.add_trace(go.Bar(name=name, x=df_top[COL_GRUPA], y=df_top[col],
                                marker_color=color, opacity=0.85))
    fig.update_layout(**layout_base("Zysk per kanał (PLN)"), barmode="group", yaxis_title="Zysk (PLN)")
    fig.add_hline(y=0, line_dash="dot", line_color="red", opacity=0.5)
    st.plotly_chart(fig, use_container_width=True)

if show_koszty:
    st.subheader("🔧 Struktura kosztów")
    fig = go.Figure()
    for col, name, color in [(COL_POBOR, "Pobór", COLORS["pobor"]), (COL_SKUP, "Skup", COLORS["skup"]),
                              (COL_PKT_TWORCY, "Pkt twórcy", COLORS["tworcy"]),
                              (COL_PKT_SPRZED, "Pkt sprzedawcy", COLORS["sprzedawcy"]),
                              (COL_PIASK, "Piaskowanie", COLORS["piask"]),
                              (COL_SPED, "Spedycja", COLORS["spedycja"]),
                              (COL_PROW_EBAY, "Prowizja eBay", COLORS["prowizja_ebay"]),
                              (COL_PROW_ALLEGRO, "Prowizja Allegro", COLORS["prowizja_allegro"])]:
        if col and col in df_top.columns:
            fig.add_trace(go.Bar(name=name, x=df_top[COL_GRUPA], y=df_top[col].fillna(0),
                                marker_color=color))
    fig.update_layout(**layout_base("Rozbicie kosztów (PLN)"), barmode="stack", yaxis_title="Koszt (PLN)")
    st.plotly_chart(fig, use_container_width=True)

if show_cena_vs_koszt and COL_CENA_EBAY and COL_KOSZT_EBAY:
    st.subheader("🏷️ eBay: Cena vs Koszt")
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Cena sprzedaży", x=df_top[COL_GRUPA], y=df_top[COL_CENA_EBAY],
                        marker_color=COLORS["ebay"], opacity=0.7))
    fig.add_trace(go.Bar(name="Koszt pozyskania", x=df_top[COL_GRUPA], y=df_top[COL_KOSZT_EBAY],
                        marker_color=COLORS["pobor"], opacity=0.7))
    if COL_BAZA_EBAY and COL_BAZA_EBAY in df_top.columns:
        fig.add_trace(go.Scatter(name="Cena 0-0", x=df_top[COL_GRUPA], y=df_top[COL_BAZA_EBAY],
                                mode="markers+lines", marker=dict(size=8, color="#1D4ED8"), line=dict(dash="dot")))
    fig.update_layout(**layout_base("eBay: Cena vs Koszt (PLN)"), barmode="group", yaxis_title="PLN")
    st.plotly_chart(fig, use_container_width=True)

if show_porownanie_cen:
    st.subheader("📈 Porównanie cen")
    cL, cR = st.columns(2)
    with cL:
        f = go.Figure()
        for col_name, name, color in [(COL_CENA_EBAY, "eBay", COLORS["ebay"]),
                                       (COL_CENA_SHOPEU, "Shop EU", COLORS["shopEU"]),
                                       (COL_CENA_ALLEGRO, "Allegro", COLORS["allegro"]),
                                       (COL_CENA_SHOPPL, "Shop PL", COLORS["shopPL"])]:
            if col_name and col_name in df_top.columns:
                f.add_trace(go.Scatter(name=name, x=df_top[COL_GRUPA], y=df_top[col_name],
                                      mode="markers+lines", marker=dict(size=6, color=color), line=dict(color=color)))
        f.update_layout(**layout_base("Ceny śr. (PLN)", 450), yaxis_title="Cena (PLN)")
        st.plotly_chart(f, use_container_width=True)
    with cR:
        f = go.Figure()
        for col_name, name, color in [(COL_BAZA_EBAY, "eBay 0-0", COLORS["ebay"]),
                                       (COL_BAZA_SHOPEU, "Shop EU 0-0", COLORS["shopEU"]),
                                       (COL_BAZA_ALLEGRO, "Allegro 0-0", COLORS["allegro"]),
                                       (COL_BAZA_SHOPPL, "Shop PL 0-0", COLORS["shopPL"])]:
            if col_name and col_name in df_top.columns:
                f.add_trace(go.Scatter(name=name, x=df_top[COL_GRUPA], y=df_top[col_name],
                                      mode="markers+lines", marker=dict(size=6, color=color), line=dict(color=color, dash="dot")))
        f.update_layout(**layout_base("Ceny 0-0 (PLN)", 450), yaxis_title="Cena (PLN)")
        st.plotly_chart(f, use_container_width=True)

if show_wolumen:
    st.subheader("📦 Wolumen i trend")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if COL_ILE:
        fig.add_trace(go.Bar(name="Ilość skrzyń", x=df_top[COL_GRUPA], y=df_top[COL_ILE],
                            marker_color="#6366F1", opacity=0.7), secondary_y=False)
    if COL_TREND and COL_TREND in df_top.columns and df_top[COL_TREND].notna().any():
        colors_trend = ["#EF4444" if pd.notna(v) and v < 1 else "#10B981" for v in df_top[COL_TREND]]
        fig.add_trace(go.Scatter(name="Trend", x=df_top[COL_GRUPA], y=df_top[COL_TREND],
                                mode="markers+lines", marker=dict(size=9, color=colors_trend),
                                line=dict(color="#6B7280", dash="dot")), secondary_y=True)
    fig.update_layout(**layout_base("Wolumen i trend", 500))
    fig.update_yaxes(title_text="Ilość", secondary_y=False)
    fig.update_yaxes(title_text="Trend", secondary_y=True)
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.3, secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

if show_heatmapa:
    st.subheader("🗺️ Mapa zysków")
    k = [("eBay", COL_ZYSK_EBAY), ("Shop EU", COL_ZYSK_SHOPEU), ("Allegro", COL_ZYSK_ALLEGRO), ("Shop PL", COL_ZYSK_SHOPPL)]
    a = [(kk, c) for kk, c in k if c and c in df_top.columns]
    if a:
        z = [df_top[col].fillna(0).tolist() for _, col in a]
        fig = go.Figure(go.Heatmap(
            z=z, x=df_top[COL_GRUPA].tolist(), y=[kk for kk, _ in a],
            colorscale=[[0, "#EF4444"], [0.35, "#FEF3C7"], [0.5, "#F5F5F5"], [0.65, "#D1FAE5"], [1, "#10B981"]],
            zmid=0, text=[[f"{v:.0f}" for v in row] for row in z],
            texttemplate="%{text}", textfont=dict(size=11),
        ))
        fig.update_layout(**layout_base("Zysk grupa × kanał", 400))
        st.plotly_chart(fig, use_container_width=True)

if show_tabela:
    st.subheader("📋 Dane")
    st.dataframe(df_top.style.format({col: "{:,.0f}" for col in df_top.select_dtypes(include="number").columns}),
                 use_container_width=True, hide_index=True)

st.markdown("---")
st.caption(f"Firestore: `rentownosc_raporty/{selected_snapshot}` | {meta.get('updated_at', '?')} | "
           f"{meta.get('source_view', '?')} | {meta.get('source_db', '?')}")
