# Alivvia Gestão – app.py
# Versão: v0.2.7
# Stack: Streamlit + PostgreSQL (Supabase) via SQLAlchemy
# Observação: exportação é sempre OPCIONAL e reflete o filtro atual na tela.

# --- bootstrap: instala libs em runtime se necessário (para Streamlit Cloud) ---
try:
    import sqlalchemy  # noqa: F401
except ModuleNotFoundError:
    import sys, subprocess
    pkgs = [
        "SQLAlchemy==2.0.31",
        "pandas==2.2.2",
        "numpy==1.26.4",
        "psycopg2-binary==2.9.9",
        "openpyxl==3.1.5",
    ]
    subprocess.check_call([sys.executable, "-m", "pip", "install", *pkgs])

from sqlalchemy import create_engine, text


import os
import io
import hashlib
import datetime as dt
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =========================
# Configuração básica
# =========================
APP_VERSION = "v0.2.7"
st.set_page_config(page_title=f"Alivvia Gestão {APP_VERSION}", layout="wide")

# Secrets (Streamlit Cloud ou .streamlit/secrets.toml)
DATABASE_URL = st.secrets["DATABASE_URL"]
APP_SECRET = st.secrets.get("APP_SECRET", "alivvia-local-secret")
DEFAULT_TOLERANCIA = 0.10  # R$ 0,10 (podemos mover para tabela/Config depois)

# =========================
# Conexão DB
# =========================
@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return engine

engine = get_engine()

# =========================
# Utilitários
# =========================
def sha1_row(values: List[str]) -> str:
    """Gera hash estável para idempotência (importação)."""
    m = hashlib.sha1()
    for v in values:
        m.update((str(v) if v is not None else "").encode("utf-8"))
        m.update(b"|")
    return m.hexdigest()

def to_date(x):
    if pd.isna(x) or x == "":
        return None
    if isinstance(x, (dt.date, dt.datetime)):
        return x.date() if isinstance(x, dt.datetime) else x
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(str(x), fmt).date()
        except Exception:
            continue
    return None

def to_decimal(x):
    if pd.isna(x) or x == "":
        return None
    try:
        if isinstance(x, str):
            x = x.replace(".", "").replace(",", ".")
        return round(float(x), 2)
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def load_dim_tables() -> Dict[str, pd.DataFrame]:
    with engine.begin() as con:
        companies = pd.read_sql("select id, name, color from companies order by name", con)
        accounts  = pd.read_sql("""
            select a.id, a.name, a.company_id, c.name as company, c.color
            from bank_accounts a
            join companies c on c.id=a.company_id
            order by c.name, a.name
        """, con)
        cats = pd.read_sql("select id, name from categories where is_active=true order by name", con)
        sups = pd.read_sql("select id, name from suppliers where is_active=true order by name", con)
        rules = pd.read_sql("select id, token, category_id, supplier_id from rules", con)
    return {"companies": companies, "accounts": accounts, "categories": cats,
            "suppliers": sups, "rules": rules}

def refresh_dims():
    load_dim_tables.clear()

def color_pill(company_name: str, color_hex: str):
    st.markdown(
        f"""<div style="display:inline-block;padding:4px 10px;border-radius:12px;background:{color_hex};color:#fff;font-weight:600">
        {company_name}
        </div>""",
        unsafe_allow_html=True
    )

# =========================
# Auto-classificação (regras)
# =========================
def suggest_category_supplier(desc: str, doc: str, rules_df: pd.DataFrame) -> Tuple[str, str]:
    if rules_df is None or rules_df.empty:
        return (None, None)
    text = f"{desc or ''} {doc or ''}".lower()
    hit = None
    for _, r in rules_df.iterrows():
        token = str(r["token"]).lower().strip()
        if token and token in text:
            hit = r
            break
    if hit is None:
        return (None, None)
    return (hit.get("category_id"), hit.get("supplier_id"))

# =========================
# Preset de mapeamento - Mercado Pago
# =========================
MP_MAP = {
    "RELEASE_DATE": "date",
    "TRANSACTION_TYPE": "description",
    "REFERENCE_ID": "doc",
    "TRANSACTION_NET_AMOUNT": "amount",
    "PARTIAL_BALANCE": "balance",
}

# =========================
# Importação (CSV/XLSX)
# =========================
def render_import():
    st.subheader("📥 Importar Extrato (CSV/XLSX)")
    st.info("Primeiro alvo: **Mercado Pago**. Depois adicionaremos outros bancos.")

    dims = load_dim_tables()
    companies = dims["companies"]
    accounts = dims["accounts"]
    rules = dims["rules"]
    cats = dims["categories"]
    sups = dims["suppliers"]

    # Selecionar empresa e conta
    col1, col2, col3 = st.columns([1.2, 1, 1])
    with col1:
        company_name = st.selectbox("Empresa", companies["name"].tolist(), index=0)
    company_row = companies[companies["name"] == company_name].iloc[0]
    color_pill(company_row["name"], company_row["color"])

    acc_opts = accounts[accounts["company_id"] == company_row["id"]]
    with col2:
        account_name = st.selectbox("Conta bancária", acc_opts["name"].tolist(), index=0)
    account_row = acc_opts[acc_opts["name"] == account_name].iloc[0]

    with col3:
        tolerancia = st.number_input("Tolerância (R$)", min_value=0.0, value=DEFAULT_TOLERANCIA, step=0.01)

    st.markdown("#### Arquivo do extrato")
    file = st.file_uploader("Arraste o arquivo CSV ou XLSX", type=["csv", "xlsx"])

    st.markdown("##### Mapeamento de colunas")
    st.caption("Preset Mercado Pago sugerido; pode ajustar se o layout mudar.")

    # tabela de mapeamento
    if file is not None:
        # Lê pequeno sample só para exibir colunas
        try:
            if file.name.lower().endswith(".csv"):
                sample = pd.read_csv(file, nrows=5)
                file.seek(0)
            else:
                sample = pd.read_excel(file, nrows=5)
                file.seek(0)
        except Exception as e:
            st.error(f"Falha ao ler arquivo: {e}")
            return
        st.write("Colunas detectadas:", list(sample.columns))
    else:
        sample = None

    # UI para mapeamento
    def pick(colname):
        options = ["--"] + (list(sample.columns) if sample is not None else [])
        default = "--"
        if sample is not None and colname in MP_MAP:
            # tentar achar por chave
            if MP_MAP[colname] in sample.columns:
                default = MP_MAP[colname]
            elif colname in sample.columns:
                default = colname
        return st.selectbox(colname, options, index=options.index(default) if default in options else 0)

    with st.form("map_form"):
        st.write("**Selecione as colunas do arquivo para cada campo interno:**")
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: m_date = pick("RELEASE_DATE")
        with c2: m_desc = pick("TRANSACTION_TYPE")
        with c3: m_doc  = pick("REFERENCE_ID")
        with c4: m_amount = pick("TRANSACTION_NET_AMOUNT")
        with c5: m_balance = pick("PARTIAL_BALANCE")
        submitted = st.form_submit_button("Pré-visualizar e validar")

    if not file or not submitted:
        return

    # Ler arquivo completo
    try:
        if file.name.lower().endswith(".csv"):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
    except Exception as e:
        st.error(f"Falha ao ler arquivo: {e}")
        return

    # Renomear para nosso padrão
    rename_map = {}
    if m_date != "--":   rename_map[m_date] = "date"
    if m_desc != "--":   rename_map[m_desc] = "description"
    if m_doc != "--":    rename_map[m_doc]  = "doc"
    if m_amount != "--": rename_map[m_amount] = "amount"
    if m_balance != "--":rename_map[m_balance] = "balance"
    df = df.rename(columns=rename_map)

    # Validar campos mínimos
    missing = [c for c in ["date", "description", "doc", "amount"] if c not in df.columns]
    if missing:
        st.error(f"Colunas obrigatórias ausentes: {missing}")
        return

    # Normalização
    df["date"] = df["date"].apply(to_date)
    df["amount"] = df["amount"].apply(to_decimal)
    if "balance" in df.columns:
        df["balance"] = df["balance"].apply(to_decimal)
    else:
        df["balance"] = None
    df["description"] = df["description"].fillna("").astype(str).str.strip()
    df["doc"] = df["doc"].fillna("").astype(str).str.strip()

    # Sugerir categoria/fornecedor pelas regras
    cat_map = {r["id"]: r["name"] for _, r in cats.iterrows()}
    sup_map = {r["id"]: r["name"] for _, r in sups.iterrows()}

    df["category_id_sug"] = None
    df["supplier_id_sug"] = None
    for i, row in df.iterrows():
        cat_id, sup_id = suggest_category_supplier(row["description"], row["doc"], rules)
        df.at[i, "category_id_sug"] = cat_id
        df.at[i, "supplier_id_sug"] = sup_id

    # Apenas para preview na tela
    df_preview = df.copy()
    df_preview["category_sug"] = df_preview["category_id_sug"].map(cat_map)
    df_preview["supplier_sug"] = df_preview["supplier_id_sug"].map(sup_map)
    st.write("Pré-visualização (amostra):")
    st.dataframe(df_preview.head(25), use_container_width=True)

    # Construir hashes
    comp_id = company_row["id"]
    acc_id = account_row["id"]

    rows = []
    for _, r in df.iterrows():
        date = r["date"]
        desc = r["description"]
        doc  = r["doc"]
        amt  = r["amount"]
        bal  = r.get("balance", None)
        h = sha1_row([comp_id, acc_id, date, desc, doc, amt])
        rows.append({
            "company_id": comp_id,
            "account_id": acc_id,
            "date": date,
            "description": desc,
            "doc": doc,
            "amount": amt,
            "balance": bal,
            "category_id": r["category_id_sug"],
            "supplier_id": r["supplier_id_sug"],
            "nf": None,
            "parcela": None,
            "free_dates": "[]",
            "match_status": "N.A.",
            "pay_status": "N.A.",
            "origin": "EXTRATO",
            "hash": h,
            "created_by": "import",
            "updated_by": "import",
        })
    df_up = pd.DataFrame(rows)

    # Checar duplicidades (já existentes no banco)
    with engine.begin() as con:
        hs = tuple(df_up["hash"].unique().tolist())
        if len(hs) == 1:
            sql = text("select hash from transactions where hash = :h")
            exists = pd.read_sql(sql, con, params={"h": hs[0]})
        else:
            sql = text(f"select hash from transactions where hash in :h")
            exists = pd.read_sql(sql, con, params={"h": hs})

    existing_hashes = set(exists["hash"].tolist()) if not exists.empty else set()
    df_insert = df_up[~df_up["hash"].isin(existing_hashes)].copy()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total no arquivo", len(df_up))
    c2.metric("Já existentes (ignorados)", len(existing_hashes))
    c3.metric("Novos a inserir", len(df_insert))

    if len(df_insert) == 0:
        st.warning("Nenhum novo lançamento para inserir (arquivo já importado).")
        return

    if st.button("✅ Confirmar importação (gravar no banco)", type="primary"):
        # Inserir em batch
        inserted = 0
        with engine.begin() as con:
            for _, r in df_insert.iterrows():
                con.execute(text("""
                    insert into transactions
                    (id, company_id, account_id, date, description, doc, amount, balance,
                     category_id, supplier_id, nf, parcela, free_dates, match_status, pay_status, pay_date,
                     origin, hash, created_by, updated_by)
                    values (gen_random_uuid(), :company_id, :account_id, :date, :description, :doc, :amount, :balance,
                            :category_id, :supplier_id, :nf, :parcela, :free_dates::jsonb, :match_status, :pay_status, :pay_date,
                            :origin, :hash, :created_by, :updated_by)
                    on conflict (hash) do nothing
                """), {
                    **{k: r[k] for k in ["company_id","account_id","date","description","doc","amount","balance",
                                         "category_id","supplier_id","nf","parcela","match_status","pay_status",
                                         "origin","hash","created_by","updated_by"]},
                    "free_dates": r["free_dates"],
                    "pay_date": None,
                })
                inserted += 1
        refresh_dims()  # dimensões podem mudar (regras futuras)
        st.success(f"Importação concluída. Inseridos: {inserted} novos.")
        st.balloons()

# =========================
# Lançamentos (CRUD simples)
# =========================
def render_lancamentos():
    st.subheader("🧾 Lançamentos (manuais)")
    dims = load_dim_tables()
    companies, cats, sups = dims["companies"], dims["categories"], dims["suppliers"]

    with st.expander("➕ Novo lançamento", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            comp = st.selectbox("Empresa", companies["name"].tolist())
            comp_id = companies.loc[companies["name"]==comp, "id"].iloc[0]
        with c2:
            date = st.date_input("Data", dt.date.today())
        with c3:
            valor = st.number_input("Valor (positivo=entrada, negativo=saída)", step=0.01, format="%.2f")
        with c4:
            nf = st.text_input("NF (opcional)")

        c5, c6, c7, c8 = st.columns(4)
        with c5: descricao = st.text_input("Descrição")
        with c6: doc = st.text_input("Documento/Ref.")
        with c7: cat_name = st.selectbox("Categoria", cats["name"].tolist())
        with c8: sup_name = st.selectbox("Fornecedor (opcional)", ["--"] + sups["name"].tolist())

        c9, c10, c11 = st.columns(3)
        with c9: parcela = st.text_input("Parcela (ex.: 1/3)")
        with c10: pay_status = st.selectbox("Status", ["Previsto","Pago","Estornado","N.A."], index=1 if valor<0 else 3)
        with c11: pay_date = st.date_input("Data Pagto (se pago)", value=dt.date.today() if pay_status=="Pago" else None)

        if st.button("Salvar lançamento"):
            cat_id = cats.loc[cats["name"]==cat_name, "id"].iloc[0]
            sup_id = None
            if sup_name != "--":
                sup_id = sups.loc[sups["name"]==sup_name, "id"].iloc[0]
            # conta opcional (None)
            h = sha1_row([comp_id, None, date, descricao, doc, valor])
            with engine.begin() as con:
                con.execute(text("""
                    insert into transactions
                    (id, company_id, account_id, date, description, doc, amount, balance,
                     category_id, supplier_id, nf, parcela, free_dates, match_status, pay_status, pay_date,
                     origin, hash, created_by, updated_by)
                    values (gen_random_uuid(), :company_id, null, :date, :description, :doc, :amount, null,
                            :category_id, :supplier_id, :nf, :parcela, '[]'::jsonb, 'N.A.', :pay_status, :pay_date,
                            'MANUAL', :hash, 'manual', 'manual')
                    on conflict (hash) do nothing
                """), {
                    "company_id": comp_id, "date": date, "description": descricao, "doc": doc, "amount": valor,
                    "category_id": cat_id, "supplier_id": sup_id, "nf": nf, "parcela": parcela,
                    "pay_status": pay_status, "pay_date": pay_date if pay_status=="Pago" else None, "hash": h
                })
            st.success("Lançamento salvo")

    # Lista com filtros
    st.markdown("### Lista")
    colf1, colf2, colf3, colf4, colf5 = st.columns([1,1,1,1,2])
    with colf1:
        emp = st.selectbox("Empresa", ["Todas"] + companies["name"].tolist(), index=0)
    with colf2:
        dt_ini = st.date_input("De", dt.date.today() - dt.timedelta(days=30))
    with colf3:
        dt_fim = st.date_input("Até", dt.date.today())
    with colf4:
        cat_filter = st.selectbox("Categoria", ["Todas"] + cats["name"].tolist(), index=0)
    with colf5:
        q = st.text_input("Busca (descrição/doc)", "")

    sql = """
        select t.id, c.name as empresa, t.date, t.description as descricao, t.doc,
               t.amount as valor, coalesce(cat.name,'') as categoria,
               coalesce(s.name,'') as fornecedor, coalesce(t.nf,'S/NF') as nf,
               coalesce(t.parcela,'') as parcela, t.match_status, t.pay_status
        from transactions t
        join companies c on c.id=t.company_id
        left join categories cat on cat.id=t.category_id
        left join suppliers s on s.id=t.supplier_id
        where t.date between :dini and :dfim
    """
    params = {"dini": dt_ini, "dfim": dt_fim}
    if emp != "Todas":
        sql += " and c.name = :emp"
        params["emp"] = emp
    if cat_filter != "Todas":
        sql += " and cat.name = :cat"
        params["cat"] = cat_filter
    if q:
        sql += " and (lower(t.description) like :q or lower(t.doc) like :q)"
        params["q"] = f"%{q.lower()}%"
    sql += " order by t.date desc limit 500"
    with engine.begin() as con:
        df = pd.read_sql(text(sql), con, params=params)
    st.dataframe(df, use_container_width=True, height=420)

    # Export do filtro atual
    if not df.empty:
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Exportar (CSV – filtro atual)", data=csv, file_name="lancamentos.csv", mime="text/csv")

# =========================
# Conciliação
# =========================
def render_conciliacao():
    st.subheader("🔗 Conciliação (somente saídas)")

    dims = load_dim_tables()
    companies, cats, sups = dims["companies"], dims["categories"], dims["suppliers"]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        emp = st.selectbox("Empresa", companies["name"].tolist())
    with col2:
        dt_ini = st.date_input("De", dt.date.today() - dt.timedelta(days=30))
    with col3:
        dt_fim = st.date_input("Até", dt.date.today())
    with col4:
        tol = st.number_input("Tolerância (R$)", min_value=0.0, value=DEFAULT_TOLERANCIA, step=0.01)

    # Saídas pendentes
    sql = """
    select t.id, t.date, t.description as descricao, t.doc, t.amount as valor,
           coalesce(cat.name,'') as categoria, coalesce(s.name,'') as fornecedor,
           coalesce(t.nf,'S/NF') as nf, coalesce(t.parcela,'') as parcela
    from transactions t
    join companies c on c.id=t.company_id
    left join categories cat on cat.id=t.category_id
    left join suppliers s on s.id=t.supplier_id
    where c.name=:emp
      and t.date between :dini and :dfim
      and t.amount < 0
      and (t.match_status='Pendente' or t.match_status='N.A.')
    order by t.date asc
    """
    with engine.begin() as con:
        pend = pd.read_sql(text(sql), con, params={"emp": emp, "dini": dt_ini, "dfim": dt_fim})
    st.write("Pendentes (saídas):", len(pend))
    st.dataframe(pend, use_container_width=True, height=360)

    with st.expander("Fazer Match (uma saída)"):
        row_id = st.text_input("ID do lançamento (copie da lista acima)")
        c1, c2, c3 = st.columns(3)
        with c1: cat_name = st.selectbox("Categoria", cats["name"].tolist())
        with c2: sup_name = st.selectbox("Fornecedor", ["--"] + sups["name"].tolist())
        with c3: nf = st.text_input("NF (opcional)")

        c4, c5 = st.columns(2)
        with c4: parcela = st.text_input("Parcela (ex.: 1/3)")
        with c5: datas_livres = st.text_input("Datas livres (ex.: 13/10, 15/11)")

        if st.button("✅ Match"):
            if not row_id:
                st.error("Informe o ID do lançamento.")
            else:
                cat_id = cats.loc[cats["name"]==cat_name, "id"].iloc[0]
                sup_id = None
                if sup_name != "--":
                    sup_id = sups.loc[sups["name"]==sup_name, "id"].iloc[0]

                # validação mínima: saídas
                with engine.begin() as con:
                    row = con.execute(text("select amount from transactions where id=:id"), {"id": row_id}).fetchone()
                    if row is None:
                        st.error("ID não encontrado.")
                        return
                    if float(row[0]) >= 0:
                        st.error("Só conciliamos saídas (valor negativo).")
                        return

                    # atualizar
                    free_dates_json = "[]"
                    if datas_livres.strip():
                        # salva como texto de string; validação simplificada
                        arr = [x.strip() for x in datas_livres.split(",")]
                        free_dates_json = pd.Series(arr).to_json(orient="values")

                    con.execute(text("""
                        update transactions
                        set category_id=:cat, supplier_id=:sup, nf=:nf, parcela=:parc,
                            free_dates=:fd::jsonb, match_status='Conciliado', updated_by='match'
                        where id=:id
                    """), {"cat": cat_id, "sup": sup_id, "nf": nf if nf else None,
                           "parc": parcela if parcela else None, "fd": free_dates_json, "id": row_id})
                st.success("Match aplicado.")

    # Exportar pendências (opcional)
    if not pend.empty:
        csv = pend.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Exportar pendências (CSV – filtro atual)", data=csv, file_name="pendencias_conc.csv", mime="text/csv")

# =========================
# Relatórios
# =========================
def render_relatorios():
    st.subheader("📊 Relatórios (em tela)")

    dims = load_dim_tables()
    companies, cats = dims["companies"], dims["categories"]

    col1, col2 = st.columns(2)
    with col1:
        emp = st.selectbox("Empresa", companies["name"].tolist())
    with col2:
        ref_mes = st.date_input("Mês da DRE", dt.date.today().replace(day=1))

    # DRE do mês
    mes_ini = ref_mes.replace(day=1)
    if mes_ini.month == 12:
        mes_fim = mes_ini.replace(year=mes_ini.year+1, month=1)
    else:
        mes_fim = mes_ini.replace(month=mes_ini.month+1)

    with engine.begin() as con:
        # Receita Bruta (repasse marketplace) – entradas > 0
        rec_bruta = pd.read_sql(text("""
            select coalesce(sum(t.amount),0) as valor
            from transactions t
            join companies c on c.id=t.company_id
            join categories cat on cat.id=t.category_id
            where c.name=:emp
              and t.date>=:ini and t.date<:fim
              and t.amount>0
              and cat.name='Receita > Vendas (repasse marketplace)'
        """), con, params={"emp": emp, "ini": mes_ini, "fim": mes_fim}).iloc[0]["valor"]

        devol = pd.read_sql(text("""
            select coalesce(sum(t.amount),0) as valor
            from transactions t
            join companies c on c.id=t.company_id
            join categories cat on cat.id=t.category_id
            where c.name=:emp
              and t.date>=:ini and t.date<:fim
              and cat.name='Receita > Estorno/Devolução'
        """), con, params={"emp": emp, "ini": mes_ini, "fim": mes_fim}).iloc[0]["valor"]

        despesas = pd.read_sql(text("""
            select cat.name as categoria, sum(t.amount) as total
            from transactions t
            join companies c on c.id=t.company_id
            left join categories cat on cat.id=t.category_id
            where c.name=:emp
              and t.date>=:ini and t.date<:fim
              and t.amount<0
            group by cat.name
            order by cat.name
        """), con, params={"emp": emp, "ini": mes_ini, "fim": mes_fim})

    rec_liq = float(rec_bruta) + float(devol) + 0.0  # devoluções tipicamente negativas
    desp_total = float(despesas["total"].sum() if not despesas.empty else 0.0)
    resultado = rec_liq + desp_total

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Receita Bruta", f"R$ {rec_bruta:,.2f}")
    c2.metric("Devoluções", f"R$ {devol:,.2f}")
    c3.metric("Despesas (soma)", f"R$ {desp_total:,.2f}")
    c4.metric("Resultado", f"R$ {resultado:,.2f}")

    with st.expander("Despesas por categoria (mês)"):
        st.dataframe(despesas, use_container_width=True)

    st.markdown("---")
    st.markdown("### Histórico (Últimos 6 meses)")
    dt_ref = dt.date.today() - dt.timedelta(days=180)
    with engine.begin() as con:
        hist = pd.read_sql(text("""
            select to_char(date_trunc('month', t.date), 'YYYY-MM') as mes,
                   sum(case when t.amount>0 then t.amount else 0 end) as entradas,
                   sum(case when t.amount<0 then t.amount else 0 end) as saidas
            from transactions t
            join companies c on c.id=t.company_id
            where c.name=:emp
              and t.date>=:ini
            group by 1
            order by 1
        """), con, params={"emp": emp, "ini": dt_ref})
    st.dataframe(hist, use_container_width=True)

    # Export do histórico
    if not hist.empty:
        st.download_button("⬇️ Exportar histórico (CSV – filtro atual)",
                           data=hist.to_csv(index=False).encode("utf-8"),
                           file_name="historico_6m.csv", mime="text/csv")

# =========================
# Configurações (Categorias, Fornecedores, Regras)
# =========================
def render_config():
    st.subheader("⚙️ Configurações")
    dims = load_dim_tables()
    cats, sups, rules = dims["categories"], dims["suppliers"], dims["rules"]

    tab1, tab2, tab3 = st.tabs(["Categorias", "Fornecedores", "Regras (auto)"])

    with tab1:
        st.write("Categorias ativas:", len(cats))
        st.dataframe(cats, use_container_width=True, height=320)
        with st.form("new_cat"):
            name = st.text_input("Nova categoria")
            if st.form_submit_button("Adicionar") and name.strip():
                with engine.begin() as con:
                    con.execute(text("insert into categories(id,name) values (gen_random_uuid(), :n) on conflict(name) do nothing"),
                                {"n": name.strip()})
                refresh_dims()
                st.success("Categoria adicionada")

    with tab2:
        st.write("Fornecedores ativos:", len(sups))
        st.dataframe(sups, use_container_width=True, height=320)
        with st.form("new_sup"):
            name = st.text_input("Novo fornecedor")
            if st.form_submit_button("Adicionar") and name.strip():
                with engine.begin() as con:
                    con.execute(text("insert into suppliers(id,name) values (gen_random_uuid(), :n) on conflict(name) do nothing"),
                                {"n": name.strip()})
                refresh_dims()
                st.success("Fornecedor adicionado")

    with tab3:
        st.write("Regras de auto-classificação (por token):")
        st.dataframe(rules, use_container_width=True, height=320)
        with st.form("new_rule"):
            token = st.text_input("Token (palavra a buscar na descrição/doc)")
            colr1, colr2 = st.columns(2)
            with colr1:
                cat_name = st.selectbox("Categoria", cats["name"].tolist())
            with colr2:
                sup_name = st.selectbox("Fornecedor (opcional)", ["--"] + sups["name"].tolist())
            if st.form_submit_button("Adicionar regra") and token.strip():
                cat_id = cats.loc[cats["name"]==cat_name, "id"].iloc[0]
                sup_id = None
                if sup_name != "--":
                    sup_id = sups.loc[sups["name"]==sup_name, "id"].iloc[0]
                with engine.begin() as con:
                    con.execute(text("""
                        insert into rules(id, token, category_id, supplier_id)
                        values (gen_random_uuid(), :t, :c, :s)
                    """), {"t": token.strip().lower(), "c": cat_id, "s": sup_id})
                refresh_dims()
                st.success("Regra adicionada")

# =========================
# Barra lateral / login simples (senha única)
# =========================
def sidebar_login():
    st.sidebar.header("🔐 Acesso")
    pwd = st.sidebar.text_input("Senha Única (temporário)", type="password")
    ok = st.sidebar.button("Entrar")
    if ok and pwd:
        st.session_state["auth_ok"] = True  # placeholder simples
    # Para POC, consideramos liberado (já que app é interno)
    st.session_state.setdefault("auth_ok", True)
    if st.session_state["auth_ok"]:
        st.sidebar.success("Acesso liberado.")

    st.sidebar.markdown("---")
    st.sidebar.header("🧭 Navegação")
    return st.sidebar.radio("Ir para", ["Importar", "Lançamentos", "Conciliação", "Relatórios", "Configurações"])

# =========================
# App
# =========================
def main():
    page = sidebar_login()
    st.markdown(f"<div style='text-align:right;color:#999'>Versão {APP_VERSION}</div>", unsafe_allow_html=True)

    if page == "Importar":
        render_import()
    elif page == "Lançamentos":
        render_lancamentos()
    elif page == "Conciliação":
        render_conciliacao()
    elif page == "Relatórios":
        render_relatorios()
    elif page == "Configurações":
        render_config()

if __name__ == "__main__":
    main()
