"""DASHBOARD RENTOWNOŚCI DSG — Streamlit Cloud"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
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
        st.error(f"❌ Tunel: {e}")
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
    except:
        pass

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
    # Kolumny które zostawiamy jako tekst
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

    # Próba konwersji dat
    for col in df.columns:
        if "data" in col.lower() or "Data" in col:
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


# ==========================================
# WYKRYWANIE TYPU WIDOKU
# ==========================================
def detect_view_type(df, meta):
    """Wykrywa typ widoku na podstawie kolumn i nazwy."""
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


# ==========================================
# GOTOWE WYKRESY
# ==========================================
def layout_base(title, height=500):
    return dict(
        title=title, height=height, template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(b=120, l=60, r=30, t=60),
        xaxis_tickangle=-45,
        font=dict(family="Segoe UI, sans-serif", size=12),
    )


def wykres_ranking_tworcow_sztuki(df, top_n):
    """1. Ranking twórców — ile wyprodukowali sztuk"""
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    if not col_tworca:
        st.warning("Brak kolumny TworcaMaggo")
        return
    agg = df.groupby(col_tworca, dropna=True).size().reset_index(name="Liczba sztuk")
    agg = agg.sort_values("Liczba sztuk", ascending=False).head(top_n)
    agg[col_tworca] = agg[col_tworca].astype(str)
    fig = go.Figure(go.Bar(
        x=agg[col_tworca], y=agg["Liczba sztuk"],
        marker_color="#3B82F6",
        hovertemplate="Twórca: %{x}<br>Sztuk: %{y:,}<extra></extra>"
    ))
    fig.update_layout(**layout_base(f"👷 Ranking twórców — ilość sztuk (Top {top_n})"),
                     yaxis_title="Liczba wyprodukowanych sztuk")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane w tabeli"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_top_produkty(df, top_n):
    """2. Top produkty — co najczęściej produkowane"""
    col_nazwa = find_col(df, "Nazwa", "nazwa_produktu", "artNazwa", "IndeksMag")
    if not col_nazwa:
        st.warning("Brak kolumny Nazwa/IndeksMag")
        return
    agg = df.groupby(col_nazwa, dropna=True).size().reset_index(name="Liczba sztuk")
    agg = agg.sort_values("Liczba sztuk", ascending=False).head(top_n)
    agg[col_nazwa] = agg[col_nazwa].astype(str)
    fig = go.Figure(go.Bar(
        x=agg[col_nazwa], y=agg["Liczba sztuk"],
        marker_color="#10B981",
        hovertemplate="Produkt: %{x}<br>Sztuk: %{y:,}<extra></extra>"
    ))
    fig.update_layout(**layout_base(f"📦 Top produkty (Top {top_n})"),
                     yaxis_title="Liczba wystąpień")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane w tabeli"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_nowa_vs_reklamacja(df):
    """3. Nowa vs Reklamacja — proporcje"""
    col_typrap = find_col(df, "TypRap")
    if not col_typrap:
        st.warning("Brak kolumny TypRap")
        return
    agg = df.groupby(col_typrap, dropna=True).size().reset_index(name="Liczba")
    agg[col_typrap] = agg[col_typrap].astype(str)

    col1, col2 = st.columns(2)
    with col1:
        fig = go.Figure(go.Pie(
            labels=agg[col_typrap], values=agg["Liczba"],
            hole=0.4,
            marker=dict(colors=["#10B981", "#EF4444", "#F59E0B", "#8B5CF6"]),
        ))
        fig.update_layout(title="🔄 Nowa vs Reklamacja (proporcje)", height=400,
                         legend=dict(orientation="h", y=-0.1))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = go.Figure(go.Bar(
            x=agg[col_typrap], y=agg["Liczba"],
            marker_color=["#10B981" if "now" in str(v).lower() else "#EF4444" for v in agg[col_typrap]],
        ))
        fig2.update_layout(**layout_base("Rozkład typów", 400))
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Dane"):
        agg["Udział %"] = (agg["Liczba"] / agg["Liczba"].sum() * 100).round(2)
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_produkcja_w_czasie(df):
    """4. Produkcja w czasie"""
    col_data = find_col(df, "DataProduktu_date", "DataProduktu", "dataTwZlecProd", "pakDataWyd")
    if not col_data:
        st.warning("Brak kolumny z datą produkcji")
        return

    df_time = df.copy()
    df_time[col_data] = pd.to_datetime(df_time[col_data], errors="coerce")
    df_time = df_time.dropna(subset=[col_data])

    if df_time.empty:
        st.warning("Brak prawidłowych dat")
        return

    okres = st.radio("Grupowanie:", ["Dzień", "Tydzień", "Miesiąc"], horizontal=True, key="okres_czasu")
    freq = {"Dzień": "D", "Tydzień": "W", "Miesiąc": "M"}[okres]

    df_time["_period"] = df_time[col_data].dt.to_period(freq).dt.to_timestamp()
    agg = df_time.groupby("_period").size().reset_index(name="Liczba sztuk")

    fig = go.Figure(go.Scatter(
        x=agg["_period"], y=agg["Liczba sztuk"],
        mode="lines+markers", fill="tozeroy",
        marker=dict(size=6, color="#6366F1"),
        line=dict(color="#6366F1"),
    ))
    fig.update_layout(**layout_base(f"📅 Produkcja w czasie (per {okres.lower()})"),
                     yaxis_title="Liczba sztuk", xaxis_title=okres)
    st.plotly_chart(fig, use_container_width=True)


def wykres_heatmapa_tworcy_produkty(df, top_n):
    """5. Twórcy × Produkty — heatmapa"""
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    col_nazwa = find_col(df, "Nazwa", "IndeksMag")
    if not col_tworca or not col_nazwa:
        st.warning("Brak TworcaMaggo lub Nazwa")
        return

    # Top N twórców i produktów
    top_tworcy = df[col_tworca].value_counts().head(min(top_n, 20)).index
    top_prod = df[col_nazwa].value_counts().head(min(top_n, 20)).index

    df_f = df[df[col_tworca].isin(top_tworcy) & df[col_nazwa].isin(top_prod)]
    pivot = df_f.groupby([col_tworca, col_nazwa]).size().unstack(fill_value=0)

    fig = go.Figure(go.Heatmap(
        z=pivot.values, x=[str(x) for x in pivot.columns], y=[str(y) for y in pivot.index],
        colorscale="Blues",
        hovertemplate="Twórca: %{y}<br>Produkt: %{x}<br>Sztuk: %{z}<extra></extra>",
        text=pivot.values, texttemplate="%{text}", textfont=dict(size=10),
    ))
    fig.update_layout(**layout_base(f"🏢 Twórcy × Produkty (Top {min(top_n, 20)}×{min(top_n, 20)})", 600))
    st.plotly_chart(fig, use_container_width=True)


def wykres_top_prokwident(df, top_n):
    """6. Top prokwident"""
    col = find_col(df, "prokwident")
    if not col:
        st.warning("Brak kolumny prokwident")
        return
    agg = df.groupby(col, dropna=True).size().reset_index(name="Liczba")
    agg = agg.sort_values("Liczba", ascending=False).head(top_n)
    agg[col] = agg[col].astype(str)
    fig = go.Figure(go.Bar(x=agg[col], y=agg["Liczba"], marker_color="#8B5CF6"))
    fig.update_layout(**layout_base(f"💪 Top prokwident (Top {top_n})"), yaxis_title="Liczba")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_ranking_zarobkow(df, top_n):
    """7. Ranking twórców — zarobki (punkty)"""
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    col_punkty = find_col(df, "PunktyProd", "Maggo_razem", "SumPunktyProd", "PunktyMaggo")
    if not col_tworca or not col_punkty:
        st.warning(f"Brak kolumn TworcaMaggo lub PunktyProd/Maggo_razem")
        return
    agg = df.groupby(col_tworca, dropna=True)[col_punkty].sum().reset_index()
    agg = agg.sort_values(col_punkty, ascending=False).head(top_n)
    agg[col_tworca] = agg[col_tworca].astype(str)
    fig = go.Figure(go.Bar(
        x=agg[col_tworca], y=agg[col_punkty],
        marker_color="#F59E0B",
        hovertemplate=f"Twórca: %{{x}}<br>{col_punkty}: %{{y:,.2f}}<extra></extra>"
    ))
    fig.update_layout(**layout_base(f"💰 Ranking twórców — suma {col_punkty} (Top {top_n})"),
                     yaxis_title=col_punkty)
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_ranking_reklamacji(df, top_n):
    """8. Ranking reklamacji"""
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    col_typrap = find_col(df, "TypRap")
    if not col_tworca:
        st.warning("Brak kolumny TworcaMaggo")
        return

    if col_typrap:
        df_rekla = df[df[col_typrap].astype(str).str.lower().str.contains("rekla", na=False)]
        if df_rekla.empty:
            st.info("Brak reklamacji w danych")
            return
        agg = df_rekla.groupby(col_tworca, dropna=True).size().reset_index(name="Reklamacje")
    else:
        col_rekla = find_col(df, "IleReklamacji", "IleProdRekla", "SumPunktyProdRekla")
        if not col_rekla:
            st.warning("Brak kolumny z reklamacjami")
            return
        agg = df.groupby(col_tworca, dropna=True)[col_rekla].sum().reset_index()
        agg = agg.rename(columns={col_rekla: "Reklamacje"})

    agg = agg.sort_values("Reklamacje", ascending=False).head(top_n)
    agg[col_tworca] = agg[col_tworca].astype(str)
    fig = go.Figure(go.Bar(
        x=agg[col_tworca], y=agg["Reklamacje"],
        marker_color="#EF4444",
    ))
    fig.update_layout(**layout_base(f"⚠️ Ranking reklamacji per twórca (Top {top_n})"),
                     yaxis_title="Liczba reklamacji")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_stosunek_nowa_rekla(df, top_n):
    """9. Stosunek nowa/reklamacja per twórca"""
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    col_typrap = find_col(df, "TypRap")
    if not col_tworca or not col_typrap:
        st.warning("Brak TworcaMaggo lub TypRap")
        return

    pivot = df.groupby([col_tworca, col_typrap]).size().unstack(fill_value=0)
    if pivot.empty:
        return

    # Znajdź kolumny nowa i rekla
    nowa_col = next((c for c in pivot.columns if "now" in str(c).lower() and "rekla" not in str(c).lower()), None)
    rekla_col = next((c for c in pivot.columns if "rekla" in str(c).lower()), None)

    if not nowa_col:
        st.warning("Nie znaleziono typu 'nowa'")
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
    fig.update_layout(**layout_base(f"📊 Nowa vs Reklamacja per twórca (Top {top_n})"),
                     barmode="stack", yaxis_title="Liczba")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Dane z % reklamacji"):
        st.dataframe(pivot, use_container_width=True, hide_index=True)


def wykres_top_grupy_igorowe(df, top_n):
    """10. Top grupy igorowe (kolektorka)"""
    col = find_col(df, "grupaIgo", "grupa_agg", "grupa")
    if not col:
        st.warning("Brak kolumny grupaIgo")
        return
    agg = df.groupby(col, dropna=True).size().reset_index(name="Liczba")
    agg = agg.sort_values("Liczba", ascending=False).head(top_n)
    agg[col] = agg[col].astype(str)
    fig = go.Figure(go.Bar(x=agg[col], y=agg["Liczba"], marker_color="#EC4899"))
    fig.update_layout(**layout_base(f"🛒 Top grupy igorowe (Top {top_n})"), yaxis_title="Liczba")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_ranking_kolektorowcow(df, top_n):
    """11. Ranking kolektorowców"""
    col = find_col(df, "Uzytkownik", "user", "UserKoleki", "TworcaKolektora", "TworcaMaggo")
    if not col:
        st.warning("Brak kolumny z użytkownikiem kolektora")
        return
    agg = df.groupby(col, dropna=True).size().reset_index(name="Liczba")
    agg = agg.sort_values("Liczba", ascending=False).head(top_n)
    agg[col] = agg[col].astype(str)
    fig = go.Figure(go.Bar(x=agg[col], y=agg["Liczba"], marker_color="#0EA5E9"))
    fig.update_layout(**layout_base(f"👥 Ranking kolektorowców (Top {top_n})"), yaxis_title="Liczba skupów")
    st.plotly_chart(fig, use_container_width=True)


def wykres_wartosc_skupow_czasie(df):
    """12. Wartość skupów w czasie"""
    col_data = find_col(df, "DataProduktu_date", "DataProduktu", "przDatePrzyjecia", "dataSkupu", "Data")
    col_wartosc = find_col(df, "Wartosc", "CenaLaczna", "przKwotaLacznaTransport", "przICenaZaSztuke")
    if not col_data or not col_wartosc:
        st.warning("Brak kolumny z datą lub wartością")
        return

    df_time = df.copy()
    df_time[col_data] = pd.to_datetime(df_time[col_data], errors="coerce")
    df_time[col_wartosc] = pd.to_numeric(df_time[col_wartosc], errors="coerce")
    df_time = df_time.dropna(subset=[col_data, col_wartosc])

    if df_time.empty:
        st.warning("Brak prawidłowych danych")
        return

    df_time["_period"] = df_time[col_data].dt.to_period("W").dt.to_timestamp()
    agg = df_time.groupby("_period")[col_wartosc].sum().reset_index()
    fig = go.Figure(go.Scatter(
        x=agg["_period"], y=agg[col_wartosc],
        mode="lines+markers", fill="tozeroy",
        marker=dict(size=6, color="#F97316"),
    ))
    fig.update_layout(**layout_base("📈 Wartość skupów w czasie (tygodniowo)"),
                     yaxis_title=f"Wartość ({col_wartosc})")
    st.plotly_chart(fig, use_container_width=True)


def wykres_pie_udzial(df, top_n):
    """15. Pie chart udziału procentowego"""
    non_num = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c]) and not c.endswith("_date")]
    if not non_num:
        st.warning("Brak kolumn tekstowych")
        return

    col = st.selectbox("Kolumna do pie chart:", non_num, key="pie_col")
    agg = df.groupby(col, dropna=True).size().reset_index(name="Liczba")
    agg = agg.sort_values("Liczba", ascending=False)

    # Top N + "Inne"
    if len(agg) > top_n:
        top = agg.head(top_n)
        inne_sum = agg.tail(len(agg) - top_n)["Liczba"].sum()
        top = pd.concat([top, pd.DataFrame({col: ["Inne"], "Liczba": [inne_sum]})], ignore_index=True)
        agg = top

    agg[col] = agg[col].astype(str)
    fig = go.Figure(go.Pie(
        labels=agg[col], values=agg["Liczba"],
        hole=0.3,
        textposition="inside", textinfo="percent+label",
    ))
    fig.update_layout(title=f"🥧 Udział procentowy: {col} (Top {top_n})", height=500)
    st.plotly_chart(fig, use_container_width=True)


# ==========================================
# SIDEBAR
# ==========================================
COLORS = {
    "ebay": "#3B82F6", "shopEU": "#10B981", "allegro": "#F59E0B", "shopPL": "#8B5CF6",
    "pobor": "#EF4444", "skup": "#F97316", "tworcy": "#A855F7", "sprzedawcy": "#EC4899",
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


# ==========================================
# GŁÓWNY INTERFEJS
# ==========================================

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

if not snapshot_ids or selected_snapshot is None:
    st.info("👈 Kliknij **'Zbadaj strukturę bazy'** → wybierz widok → **'Pobierz dane'**")
    st.stop()

with st.spinner("Ładowanie z Firestore..."):
    df, meta = pobierz_z_firestore(selected_snapshot)

if df is None:
    st.warning("⚠️ Snapshot pusty.")
    st.stop()

# Metryki
c1, c2, c3, c4 = st.columns(4)
c1.metric("Wierszy", meta.get("row_count", len(df)))
c2.metric("Źródło", meta.get("source_view", "?"))
c3.metric("Baza", meta.get("source_db", "?"))
c4.metric("Aktualizacja", meta.get("updated_at", "?")[:16])

# Struktura
with st.expander(f"📋 Struktura danych — {len(df.columns)} kolumn", expanded=False):
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

# Wykrywanie typu widoku
view_types = detect_view_type(df, meta)

st.markdown("---")
st.header("📊 Gotowe wykresy")
st.caption(f"Wykryte typy danych: **{', '.join(view_types) if view_types else 'nieznany'}**")

# Konfiguracja buttonów — (id, emoji+nazwa, wymaga_typu_lub_None, funkcja)
WYKRESY = [
    ("w1",  "👷 Ranking twórców — sztuki",       "produkcja",     lambda: wykres_ranking_tworcow_sztuki(df, top_n)),
    ("w2",  "📦 Top produkty",                    "produkcja",     lambda: wykres_top_produkty(df, top_n)),
    ("w3",  "🔄 Nowa vs Reklamacja",              "z_reklamacjami", lambda: wykres_nowa_vs_reklamacja(df)),
    ("w4",  "📅 Produkcja w czasie",              None,            lambda: wykres_produkcja_w_czasie(df)),
    ("w5",  "🏢 Twórcy × Produkty (heatmapa)",    "produkcja",     lambda: wykres_heatmapa_tworcy_produkty(df, top_n)),
    ("w6",  "💪 Top prokwident",                  None,            lambda: wykres_top_prokwident(df, top_n)),
    ("w7",  "💰 Ranking twórców — zarobki",       "punkty_tworcow", lambda: wykres_ranking_zarobkow(df, top_n)),
    ("w8",  "⚠️ Ranking reklamacji",              None,            lambda: wykres_ranking_reklamacji(df, top_n)),
    ("w9",  "📊 Stosunek nowa/rekla per twórca",  "z_reklamacjami", lambda: wykres_stosunek_nowa_rekla(df, top_n)),
    ("w10", "🛒 Top grupy igorowe",               None,            lambda: wykres_top_grupy_igorowe(df, top_n)),
    ("w11", "👥 Ranking kolektorowców",           "kolektorka",    lambda: wykres_ranking_kolektorowcow(df, top_n)),
    ("w12", "📈 Wartość skupów w czasie",         "kolektorka",    lambda: wykres_wartosc_skupow_czasie(df)),
    ("w15", "🥧 Pie chart udziału",               None,            lambda: wykres_pie_udzial(df, top_n)),
]

# Filtruj wykresy pasujące do typu widoku
available = []
for wid, label, typ, func in WYKRESY:
    if typ is None or typ in view_types:
        available.append((wid, label, func))

if not available:
    st.warning("Nie znaleziono pasujących wykresów dla tego widoku. Spróbuj szybkiej agregacji poniżej.")
else:
    st.caption(f"✨ Dostępne wykresy: **{len(available)}**")

    # Inicjalizuj state
    if "aktywne_wykresy" not in st.session_state:
        st.session_state["aktywne_wykresy"] = set()

    # Przyciski w 3 kolumnach
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

    c1, c2 = st.columns([1, 1])
    if c1.button("Pokaż wszystkie", use_container_width=True):
        st.session_state["aktywne_wykresy"] = {wid for wid, _, _ in available}
        st.rerun()
    if c2.button("Ukryj wszystkie", use_container_width=True):
        st.session_state["aktywne_wykresy"] = set()
        st.rerun()

    st.markdown("---")

    # Renderuj aktywne wykresy
    for wid, label, func in available:
        if wid in st.session_state["aktywne_wykresy"]:
            st.subheader(label)
            try:
                func()
            except Exception as e:
                st.error(f"Błąd wykresu: {e}")
            st.markdown("---")


# ==========================================
# SZYBKA AGREGACJA (zawsze na końcu)
# ==========================================
st.header("📊 Szybka agregacja (własna)")

numeric_cols = df.select_dtypes(include="number").columns.tolist()
numeric_cols = [c for c in numeric_cols if not c.endswith("_date")]
text_cols = [c for c in df.columns if c not in numeric_cols and not c.endswith("_date")]

if numeric_cols and text_cols:
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        group_col = st.selectbox("Grupuj po:", text_cols, key="qa_group")
    with col_b:
        agg_col = st.selectbox("Agreguj:", numeric_cols, key="qa_agg")
    with col_c:
        agg_func = st.selectbox("Funkcja:", ["sum", "mean", "count", "max", "min"], key="qa_func")

    try:
        agg_result = df.groupby(group_col, dropna=True)[agg_col].agg(agg_func).reset_index()
        agg_result = agg_result.sort_values(agg_col, ascending=False).head(top_n)
        agg_result[group_col] = agg_result[group_col].astype(str)

        fig = go.Figure(go.Bar(
            x=agg_result[group_col], y=agg_result[agg_col],
            marker_color="#3B82F6",
        ))
        fig.update_layout(**layout_base(f"{agg_func.upper()}({agg_col}) per {group_col} — Top {top_n}"),
                         yaxis_title=f"{agg_func}({agg_col})")
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Dane"):
            st.dataframe(agg_result, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Błąd: {e}")

st.markdown("---")
st.caption(f"Firestore: `rentownosc_raporty/{selected_snapshot}` | {meta.get('updated_at', '?')} | "
           f"{meta.get('source_view', '?')} | {meta.get('source_db', '?')}")
