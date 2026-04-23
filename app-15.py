"""DASHBOARD RENTOWNOŚCI DSG — Streamlit Cloud (all-in-one + AI query)"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from decimal import Decimal
import json
import re
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
# POŁĄCZENIE
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

    # Zapisz wynik do Firestore żeby nie klikać co chwila
    try:
        db.collection("rentownosc_raporty").document("_diagnostyka").set({
            "wynik": wyniki,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    except:
        pass
    return wyniki


def wczytaj_diagnostyke_z_firestore():
    """Wczytuje cache diagnostyki jeśli istnieje."""
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
    """Pobiera schemat wszystkich tabel w public (nazwy + kolumny + typy)."""
    try:
        from sshtunnel import SSHTunnelForwarder
        import psycopg2
    except ImportError:
        return None

    try:
        with otworz_tunel() as tunnel:
            conn = pg_connect(tunnel, "maggo")
            cur = conn.cursor()

            # Lista tabel i widoków w public i rapdb
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


def schema_do_tekstu_dla_ai(schema, max_tables=80):
    """Konwertuje schemat bazy do zwięzłego tekstu dla AI."""
    if not schema:
        return ""
    lines = []
    # Priorytet: tabele z "wms", "rapdb", "Vw"
    priority_keywords = ["wms", "rapdb", "Vw", "Maggo", "Raport"]

    def priority(key):
        for i, kw in enumerate(priority_keywords):
            if kw.lower() in key.lower():
                return i
        return len(priority_keywords)

    sorted_keys = sorted(schema.keys(), key=priority)[:max_tables]

    for key in sorted_keys:
        info = schema[key]
        type_label = "VIEW" if info["type"] == "VIEW" else "TABLE"
        cols_str = ", ".join([f'{c["name"]} ({c["type"]})' for c in info["columns"][:25]])
        if len(info["columns"]) > 25:
            cols_str += f" ... +{len(info['columns'])-25} więcej"
        lines.append(f'{type_label} "{key}": {cols_str}')
    return "\n".join(lines)


# ==========================================
# AI — NATURAL LANGUAGE → SQL
# ==========================================
def wygeneruj_sql_przez_ai(pytanie_pl, schema_text):
    """Używa Gemini żeby zamienić pytanie po polsku na SQL."""
    try:
        import google.generativeai as genai
    except ImportError:
        return None, "Brak biblioteki google-generativeai. Dodaj do requirements.txt"

    api_key = st.secrets.get("GEMINI_API_KEY", "")
    if not api_key or api_key == "WSTAW_TU_SWOJ_KLUCZ_GEMINI":
        return None, "Brak klucza Gemini w secrets (GEMINI_API_KEY). Dodaj klucz z https://aistudio.google.com/app/apikey"

    genai.configure(api_key=api_key)

    prompt = f"""Jesteś ekspertem SQL dla PostgreSQL. Użytkownik opisuje po polsku co chce zobaczyć, a Ty generujesz odpowiednie zapytanie SQL.

BAZA DANYCH (schemat w bazie 'maggo'):
{schema_text}

ZASADY:
1. Zwracaj TYLKO zapytanie SELECT — nigdy INSERT/UPDATE/DELETE/DROP/TRUNCATE/ALTER.
2. Zawsze używaj cudzysłowów dla nazw z wielkimi literami: "NazwaTabeli", "nazwaKolumny".
3. Zawsze dodawaj LIMIT 1000 na końcu (chyba że użytkownik prosi o zagregowane wyniki).
4. Używaj jawnych nazw schematów: "public"."wmsArtykuly", "rapdb"."VwCos".
5. Preferuj gotowe widoki z rapdb gdy to możliwe.
6. Dla dat używaj formatu 'YYYY-MM-DD'::date.
7. Wynik musi być JEDNYM zapytaniem SQL, bez komentarzy, bez markdown.

PYTANIE UŻYTKOWNIKA:
{pytanie_pl}

Zwróć tylko samo zapytanie SQL, nic więcej."""

    try:
        model = genai.GenerativeModel("gemini-2.0-flash-exp")
        response = model.generate_content(prompt, generation_config={"temperature": 0.1})
        sql = response.text.strip()

        # Usuń ewentualny markdown ```sql ... ```
        sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'^```\s*', '', sql)
        sql = re.sub(r'\s*```\s*$', '', sql)
        sql = sql.strip().rstrip(';').strip()

        # Walidacja bezpieczeństwa — tylko SELECT
        sql_upper = sql.upper()
        forbidden = ["INSERT ", "UPDATE ", "DELETE ", "DROP ", "TRUNCATE ", "ALTER ", "CREATE ", "GRANT ", "REVOKE "]
        for f in forbidden:
            if f in sql_upper:
                return None, f"⚠️ Wygenerowane zapytanie zawiera niedozwolone słowo: {f.strip()}. Zapytanie odrzucone."

        if not sql_upper.lstrip().startswith("SELECT") and not sql_upper.lstrip().startswith("WITH"):
            return None, "⚠️ Wygenerowane zapytanie nie zaczyna się od SELECT. Zapytanie odrzucone."

        return sql, None
    except Exception as e:
        return None, f"Błąd Gemini: {e}"


def odpal_zapytanie_sql(sql, dbname="maggo"):
    """Odpala zapytanie SQL przez tunel SSH, zwraca DataFrame."""
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


# ==========================================
# POBIERANIE WIDOKÓW (oryginalne)
# ==========================================
def pobierz_z_bazy(full_query=None, limit=None):
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
                base_query = f'SELECT * FROM "{schema}"."{view}"'
                if limit:
                    base_query += f" LIMIT {limit}"
                attempts = [(db_name, base_query)]
            else:
                return None, None

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


def pobierz_wszystkie_widoki(widoki_lista, limit=None):
    """Pobiera wszystkie widoki jeden po drugim, zapisuje do Firestore."""
    try:
        from sshtunnel import SSHTunnelForwarder
        import psycopg2
    except ImportError:
        st.error("Brakuje bibliotek.")
        return

    wyniki = []
    progress_main = st.progress(0, text=f"📥 Pobieranie {len(widoki_lista)} widoków...")

    try:
        with otworz_tunel() as tunnel:
            st.success("✅ Tunel SSH otwarty — pobieranie...")
            for i, (db_name, schema, view) in enumerate(widoki_lista):
                status = st.empty()
                status.info(f"📊 [{i+1}/{len(widoki_lista)}] Pobieram: {db_name}.{schema}.{view}")
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
                        # Zapis do Firestore
                        zapisz_do_firestore(df, meta, snapshot_id=f"snapshot_{view}_{datetime.now().strftime('%Y%m%d_%H%M')}")
                        wyniki.append((view, len(df), "OK"))
                        status.success(f"✅ [{i+1}/{len(widoki_lista)}] {view}: {len(df)} wierszy")
                    else:
                        wyniki.append((view, 0, "pusty"))
                        status.warning(f"⚠️ [{i+1}/{len(widoki_lista)}] {view}: pusty")
                except Exception as e:
                    wyniki.append((view, 0, f"błąd: {str(e)[:80]}"))
                    status.error(f"❌ [{i+1}/{len(widoki_lista)}] {view}: {str(e)[:80]}")
                progress_main.progress((i + 1) / len(widoki_lista), text=f"Ukończono {i+1}/{len(widoki_lista)}")
    except Exception as e:
        st.error(f"❌ Tunel: {e}")
        return

    progress_main.empty()
    st.success(f"✅ Ukończono! Pobranych widoków: {sum(1 for _, _, s in wyniki if s == 'OK')}/{len(wyniki)}")
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
    progress = st.progress(0, text=f"💾 Zapis {len(records)} w {num_chunks} częściach...")

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
        for old_id in snapshots[30:]:
            old_chunks = db.collection("rentownosc_raporty").document(old_id).collection("chunks").list_documents()
            for doc in old_chunks:
                doc.delete()
            db.collection("rentownosc_raporty").document(old_id).delete()
    except:
        pass


def wyczysc_firestore():
    """Usuwa wszystko z kolekcji rentownosc_raporty."""
    all_docs = list(db.collection("rentownosc_raporty").list_documents())
    total = len(all_docs)
    if total == 0:
        st.info("Kolekcja już pusta.")
        return
    progress = st.progress(0, text=f"Usuwam {total} dokumentów...")
    deleted = 0
    for i, doc in enumerate(all_docs):
        try:
            chunks = list(doc.collection("chunks").list_documents())
            for chunk in chunks:
                chunk.delete()
            doc.delete()
            deleted += 1
        except Exception as e:
            st.warning(f"Błąd: {e}")
        progress.progress((i + 1) / total, text=f"Usunięto {i+1}/{total}")
    progress.empty()
    st.success(f"✅ Usunięto {deleted} dokumentów")
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


# ==========================================
# WYKRESY
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
    col_tworca = find_col(df, "TworcaMaggo", "tworca_maggo")
    if not col_tworca:
        st.warning("Brak kolumny TworcaMaggo")
        return
    agg = df.groupby(col_tworca, dropna=True).size().reset_index(name="Liczba sztuk")
    agg = agg.sort_values("Liczba sztuk", ascending=False).head(top_n)
    agg[col_tworca] = agg[col_tworca].astype(str)
    fig = go.Figure(go.Bar(x=agg[col_tworca], y=agg["Liczba sztuk"], marker_color="#3B82F6"))
    fig.update_layout(**layout_base(f"👷 Ranking twórców — sztuki (Top {top_n})"), yaxis_title="Sztuki")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Dane"):
        st.dataframe(agg, use_container_width=True, hide_index=True)


def wykres_top_produkty(df, top_n):
    col_nazwa = find_col(df, "Nazwa", "nazwa_produktu", "artNazwa", "IndeksMag")
    if not col_nazwa:
        st.warning("Brak kolumny Nazwa/IndeksMag")
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
        st.warning("Brak kolumny TypRap")
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
        st.warning("Brak daty")
        return
    df_time = df.copy()
    df_time[col_data] = pd.to_datetime(df_time[col_data], errors="coerce")
    df_time = df_time.dropna(subset=[col_data])
    if df_time.empty:
        st.warning("Brak dat")
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
        st.warning("Brak kolumny twórcy lub punktów")
        return
    agg = df.groupby(col_tworca, dropna=True)[col_punkty].sum().reset_index()
    agg = agg.sort_values(col_punkty, ascending=False).head(top_n)
    agg[col_tworca] = agg[col_tworca].astype(str)
    fig = go.Figure(go.Bar(x=agg[col_tworca], y=agg[col_punkty], marker_color="#F59E0B"))
    fig.update_layout(**layout_base(f"💰 Ranking twórców — suma {col_punkty} (Top {top_n})"), yaxis_title=col_punkty)
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
    fig.update_layout(**layout_base(f"⚠️ Ranking reklamacji (Top {top_n})"))
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
    fig.update_layout(**layout_base(f"📊 Nowa vs Reklamacja per twórca (Top {top_n})"), barmode="stack")
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
    fig.update_layout(**layout_base(f"🛒 Top grupy igorowe (Top {top_n})"))
    st.plotly_chart(fig, use_container_width=True)


def wykres_ranking_kolektorowcow(df, top_n):
    col = find_col(df, "Uzytkownik", "user", "UserKoleki", "TworcaKolektora", "TworcaMaggo")
    if not col:
        return
    agg = df.groupby(col, dropna=True).size().reset_index(name="Liczba")
    agg = agg.sort_values("Liczba", ascending=False).head(top_n)
    agg[col] = agg[col].astype(str)
    fig = go.Figure(go.Bar(x=agg[col], y=agg["Liczba"], marker_color="#0EA5E9"))
    fig.update_layout(**layout_base(f"👥 Ranking kolektorowców (Top {top_n})"))
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
    fig.update_layout(**layout_base("📈 Wartość skupów w czasie"))
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
    fig.update_layout(title=f"🥧 Udział: {col} (Top {top_n})", height=500)
    st.plotly_chart(fig, use_container_width=True)


# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.title("📊 Rentowność DSG")
    st.markdown("---")

    # --- Diagnostyka ---
    st.subheader("🔍 Diagnostyka")

    # Sprawdź czy mamy cache
    diag_cached, diag_time = wczytaj_diagnostyke_z_firestore()
    if diag_cached and "diagnostyka_wynik" not in st.session_state:
        st.session_state["diagnostyka_wynik"] = diag_cached

    if diag_time:
        st.caption(f"📌 Cache: {diag_time[:16]}")

    if st.button("Zbadaj strukturę bazy"):
        st.session_state["diagnostyka_wynik"] = diagnostyka()

    st.markdown("---")

    # --- Pobieranie ---
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

    limit_wierszy = st.number_input("Limit wierszy (0 = wszystkie):", value=0, min_value=0, step=1000,
                                    help="0 = pobierz wszystko, inaczej LIMIT n w zapytaniu")

    now = datetime.now()
    is_working_hours = 7 <= now.hour <= 21
    if is_working_hours:
        st.warning(f"⚠️ Godziny produkcyjne ({now.strftime('%H:%M')})")
        can_refresh = st.checkbox("Rozumiem, odpytuję mimo to")
    else:
        st.success(f"🌙 Pora nocna ({now.strftime('%H:%M')})")
        can_refresh = True

    c_btn1, c_btn2 = st.columns(2)
    if c_btn1.button("⚡ Jeden widok", type="primary", disabled=not can_refresh, use_container_width=True):
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

    if c_btn2.button("⚡⚡ Wszystkie", disabled=not can_refresh or not wszystkie_widoki_lista, use_container_width=True,
                    help="Pobiera wszystkie dostępne widoki po kolei. MOŻE DŁUGO TRWAĆ!"):
        if wszystkie_widoki_lista:
            st.warning(f"⏳ Pobieram {len(wszystkie_widoki_lista)} widoków — może to potrwać kilka minut!")
            lim = limit_wierszy if limit_wierszy > 0 else None
            pobierz_wszystkie_widoki(wszystkie_widoki_lista, limit=lim)
            st.cache_data.clear()
            st.rerun()

    st.markdown("---")

    # --- Snapshoty ---
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
                # Format: snapshot_NAZWA_YYYYMMDD_HHMM lub snapshot_YYYYMMDD_HHMM
                if len(parts) >= 13 and parts[:4].isdigit():
                    display_names[sid] = f"📷 {parts[:4]}-{parts[4:6]}-{parts[6:8]} {parts[9:11]}:{parts[11:13]}"
                else:
                    display_names[sid] = f"📷 {parts[:50]}"
        selected_snapshot = st.selectbox("Snapshot:", snapshot_ids, format_func=lambda x: display_names.get(x, x))

    st.markdown("---")
    st.subheader("🔧 Filtry")
    top_n = st.slider("Top N:", 5, 100, 30)

    st.markdown("---")

    # --- Czyszczenie Firestore ---
    st.subheader("🗑️ Firestore")
    all_fs_docs = list(db.collection("rentownosc_raporty").list_documents())
    total_fs_docs = len(all_fs_docs)
    st.caption(f"Dokumentów: {total_fs_docs}")
    if "confirm_clean" not in st.session_state:
        st.session_state["confirm_clean"] = False
    if not st.session_state["confirm_clean"]:
        if st.button("🗑️ Wyczyść Firestore"):
            st.session_state["confirm_clean"] = True
            st.rerun()
    else:
        st.warning("⚠️ Usunie WSZYSTKIE snapshoty!")
        c1, c2 = st.columns(2)
        if c1.button("✅ TAK", type="primary"):
            wyczysc_firestore()
            st.session_state["confirm_clean"] = False
            st.rerun()
        if c2.button("❌ Anuluj"):
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
        st.info("👈 Kliknij **'Zbadaj strukturę bazy'** → wybierz widok → **'Pobierz dane'**")
    else:
        with st.spinner("Ładowanie z Firestore..."):
            df, meta = pobierz_z_firestore(selected_snapshot)

        if df is None:
            st.warning("⚠️ Snapshot pusty.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Wierszy", meta.get("row_count", len(df)))
            c2.metric("Źródło", meta.get("source_view", "?"))
            c3.metric("Baza", meta.get("source_db", "?"))
            c4.metric("Aktualizacja", meta.get("updated_at", "?")[:16])

            with st.expander(f"📋 Struktura — {len(df.columns)} kolumn", expanded=False):
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
            st.caption(f"Typy danych: **{', '.join(view_types) if view_types else 'uniwersalne'}**")

            WYKRESY = [
                ("w1", "👷 Ranking twórców — sztuki", "produkcja", lambda: wykres_ranking_tworcow_sztuki(df, top_n)),
                ("w2", "📦 Top produkty", "produkcja", lambda: wykres_top_produkty(df, top_n)),
                ("w3", "🔄 Nowa vs Reklamacja", "z_reklamacjami", lambda: wykres_nowa_vs_reklamacja(df)),
                ("w4", "📅 Produkcja w czasie", None, lambda: wykres_produkcja_w_czasie(df)),
                ("w5", "🏢 Twórcy × Produkty", "produkcja", lambda: wykres_heatmapa_tworcy_produkty(df, top_n)),
                ("w6", "💪 Top prokwident", None, lambda: wykres_top_prokwident(df, top_n)),
                ("w7", "💰 Ranking twórców — zarobki", "punkty_tworcow", lambda: wykres_ranking_zarobkow(df, top_n)),
                ("w8", "⚠️ Ranking reklamacji", None, lambda: wykres_ranking_reklamacji(df, top_n)),
                ("w9", "📊 Stosunek nowa/rekla", "z_reklamacjami", lambda: wykres_stosunek_nowa_rekla(df, top_n)),
                ("w10", "🛒 Top grupy igorowe", None, lambda: wykres_top_grupy_igorowe(df, top_n)),
                ("w11", "👥 Ranking kolektorowców", "kolektorka", lambda: wykres_ranking_kolektorowcow(df, top_n)),
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

            # Szybka agregacja
            st.header("📊 Szybka agregacja (własna)")
            numeric_cols = [c for c in df.select_dtypes(include="number").columns if not c.endswith("_date")]
            text_cols = [c for c in df.columns if c not in numeric_cols and not c.endswith("_date")]
            if numeric_cols and text_cols:
                c1, c2, c3 = st.columns(3)
                with c1:
                    group_col = st.selectbox("Grupuj po:", text_cols, key="qa_group")
                with c2:
                    agg_col = st.selectbox("Agreguj:", numeric_cols, key="qa_agg")
                with c3:
                    agg_func = st.selectbox("Funkcja:", ["sum", "mean", "count", "max", "min"], key="qa_func")
                try:
                    agg_result = df.groupby(group_col, dropna=True)[agg_col].agg(agg_func).reset_index()
                    agg_result = agg_result.sort_values(agg_col, ascending=False).head(top_n)
                    agg_result[group_col] = agg_result[group_col].astype(str)
                    fig = go.Figure(go.Bar(x=agg_result[group_col], y=agg_result[agg_col], marker_color="#3B82F6"))
                    fig.update_layout(**layout_base(f"{agg_func.upper()}({agg_col}) per {group_col} — Top {top_n}"))
                    st.plotly_chart(fig, use_container_width=True)
                    with st.expander("Dane"):
                        st.dataframe(agg_result, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Błąd: {e}")


# ============ TAB: ZAPYTANIE AI ============
with tab_ai:
    st.header("🤖 Zapytanie do bazy w języku naturalnym")
    st.caption("Opisz po polsku co chcesz zobaczyć. AI wygeneruje zapytanie SQL, pokaże ci je do zatwierdzenia, potem odpali.")

    # Sprawdź klucz Gemini
    gem_key = st.secrets.get("GEMINI_API_KEY", "")
    if not gem_key or gem_key == "WSTAW_TU_SWOJ_KLUCZ_GEMINI":
        st.error("❌ Brak klucza Gemini w secrets.")
        st.info("1. Wejdź na https://aistudio.google.com/app/apikey\n2. Wygeneruj klucz\n3. W Streamlit Cloud → Settings → Secrets → zmień wartość `GEMINI_API_KEY`")
        st.stop()

    # Schema bazy
    schema_col1, schema_col2 = st.columns([3, 1])
    with schema_col1:
        st.markdown("**Schemat bazy:** apka musi znać strukturę tabel żeby AI poprawnie generowało SQL.")
    with schema_col2:
        if st.button("🔄 Odśwież schemat"):
            st.cache_data.clear()
            st.rerun()

    schema = pobierz_schema_bazy()
    if not schema:
        st.error("Nie udało się pobrać schematu bazy.")
        st.stop()

    with st.expander(f"📚 Schemat bazy ({len(schema)} tabel/widoków)", expanded=False):
        for key, info in sorted(schema.items()):
            type_label = "📊 VIEW" if info["type"] == "VIEW" else "📋 TABLE"
            st.caption(f"**{type_label} {key}** — {len(info['columns'])} kolumn")

    schema_text = schema_do_tekstu_dla_ai(schema)

    # Pole tekstowe
    pytanie = st.text_area(
        "Twoje pytanie:",
        placeholder="np. Pokaż które skrzynie twórca MM pobrał w marcu 2026 i jakie części były w nich użyte",
        height=100,
        key="ai_pytanie",
    )

    # Przykładowe pytania
    st.caption("💡 **Przykłady:**")
    ex1, ex2, ex3 = st.columns(3)
    if ex1.button("📦 Co produkuje najwięcej MM", use_container_width=True):
        st.session_state["ai_pytanie_fill"] = "Pokaż produkty które twórca MM produkował najwięcej w ostatnich 30 dniach, sortuj malejąco"
        st.rerun()
    if ex2.button("🔧 Skrzynie i ich części", use_container_width=True):
        st.session_state["ai_pytanie_fill"] = "Dla 10 ostatnio wyprodukowanych skrzyń pokaż numer partii, twórcę i listę części które zostały pobrane z magazynu"
        st.rerun()
    if ex3.button("💸 Top zarobki tygodnia", use_container_width=True):
        st.session_state["ai_pytanie_fill"] = "Top 10 twórców po punktach produkcji w ostatnim tygodniu"
        st.rerun()

    # Auto-fill z przycisków
    if "ai_pytanie_fill" in st.session_state:
        pytanie = st.session_state.pop("ai_pytanie_fill")
        st.session_state["ai_pytanie"] = pytanie

    # Button "Generuj SQL"
    if st.button("🪄 Generuj SQL", type="primary", disabled=not pytanie):
        with st.spinner("AI myśli..."):
            sql, err = wygeneruj_sql_przez_ai(pytanie, schema_text)
            if err:
                st.error(err)
            else:
                st.session_state["ai_sql"] = sql
                st.session_state["ai_sql_pytanie"] = pytanie
                st.rerun()

    # Wyświetl wygenerowany SQL
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
                with st.spinner("Odpalam SQL..."):
                    df_ai, err = odpal_zapytanie_sql(edited_sql)
                    if err:
                        st.error(f"❌ Błąd: {err}")
                    else:
                        st.session_state["ai_df"] = df_ai
                        st.success(f"✅ Pobrano {len(df_ai)} wierszy")

        if c2.button("🔄 Regeneruj"):
            del st.session_state["ai_sql"]
            st.rerun()

        if c3.button("🗑️ Wyczyść"):
            st.session_state.pop("ai_sql", None)
            st.session_state.pop("ai_df", None)
            st.rerun()

    # Wyświetl wyniki
    if "ai_df" in st.session_state:
        df_ai = st.session_state["ai_df"]
        st.markdown("---")
        st.subheader(f"📊 Wynik ({len(df_ai)} wierszy)")

        # Metryki
        c1, c2 = st.columns(2)
        c1.metric("Wiersze", len(df_ai))
        c2.metric("Kolumny", len(df_ai.columns))

        # Tabela
        st.dataframe(df_ai, use_container_width=True)

        # Szybka wizualizacja
        numeric_cols = df_ai.select_dtypes(include="number").columns.tolist()
        text_cols = [c for c in df_ai.columns if c not in numeric_cols]

        if numeric_cols and text_cols and len(df_ai) > 1:
            st.markdown("---")
            st.subheader("📊 Szybka wizualizacja")
            c1, c2, c3 = st.columns(3)
            with c1:
                x_col = st.selectbox("X (etykiety):", text_cols, key="ai_x")
            with c2:
                y_col = st.selectbox("Y (wartości):", numeric_cols, key="ai_y")
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
                st.warning(f"Nie udało się narysować: {e}")

        # Export CSV
        csv = df_ai.to_csv(index=False).encode('utf-8-sig')
        st.download_button("⬇️ Pobierz CSV", csv, f"ai_wynik_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", "text/csv")


st.markdown("---")
st.caption("Dashboard Rentowności DSG | SSH tunel → PostgreSQL → Firestore")
