# Alivvia Gestão - app.py (COMPLETO)
# Autor: Paulo Piva + ChatGPT
# Objetivo: Sistema financeiro simples (Importar → Lançamentos → Conciliação → Relatórios) com persistência local.
# Rodar: streamlit run app.py
# Dependências: streamlit, pandas, numpy, openpyxl (para .xlsx)

import streamlit as st
import pandas as pd
import numpy as np
import json
import hashlib
from datetime import datetime, date
from pathlib import Path
import uuid
import re

# ==============================
# Config & Constantes
# ==============================

st.set_page_config(page_title="Alivvia Gestão", layout="wide")

DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)

TX_FILE = DATA_DIR / "transactions.csv"         # base principal (extrato + lançamentos + status de conciliação)
RULES_FILE = DATA_DIR / "rules.json"            # regras automáticas (aprendizado)
CATS_FILE = DATA_DIR / "categories.json"        # categorias
SUPP_FILE = DATA_DIR / "suppliers.json"         # fornecedores

TOLERANCIA_MATCH = 0.10  # R$ 0,10 (informativa por enquanto)

# Cores por empresa
COMPANY_COLORS = {
    "Alivvia": "#15A34A",  # verde
    "JCA": "#7C3AED"       # roxo
}

# Categorias base (DRE)
DEFAULT_CATEGORIES = [
    "Receita > Vendas (repasse marketplace)",
    "Receita > Estorno/Devolução",
    "Deduções > Taxas/Chargebacks (marketplace)",
    "Custo > Frete",
    "Custo > Embalagens/Envio",
    "Custo > Aquisição/Fornecedores",
    "Despesas > Marketing",
    "Despesas > Salários/Encargos",
    "Despesas > Aluguel",
    "Despesas > Utilidades",
    "Despesas > Administrativas",
    "Despesas > Impostos/Taxas",
    "Despesas > Tarifas bancárias",
    "Financeiro > Juros/Multas",
    "Investimento > Imobilizado/Equipamentos",
    "Transferência intra-grupo",
    "Retirada Social",
    "Outros"
]

# Regras de auto-classificação (descricao + doc)
DEFAULT_RULES = [
    # ENTRADAS (repasse marketplace)
    {"contains": ["entrada de dinheiro", "pix recebido", "pix credito", "qrcode pix", "pix mercado"],
     "in_doc": [], "category": "Receita > Vendas (repasse marketplace)", "supplier": ""},
    # DEVOLUÇÕES
    {"contains": ["estorno", "reembolso", "devolu"], "in_doc": [],
     "category": "Receita > Estorno/Devolução", "supplier": ""},
    # TARIFAS/CHARGEBACKS (auto: não exige match)
    {"contains": ["tarifa", "iof", "chargeback", "debito"], "in_doc": [],
     "category": "Deduções > Taxas/Chargebacks (marketplace)", "supplier": ""},
    {"contains": ["tarifa", "iof", "taxa"], "in_doc": [],
     "category": "Despesas > Tarifas bancárias", "supplier": ""},
    # FRETE
    {"contains": ["jadlog", "correios", "total express"], "in_doc": [],
     "category": "Custo > Frete", "supplier": ""},
    # FORNECEDOR
    {"contains": ["thor", "fornecedor"], "in_doc": [],
     "category": "Custo > Aquisição/Fornecedores", "supplier": "Thor"},
]

# Campos padrão do modelo interno
TX_COLUMNS = [
    "tx_id", "empresa", "conta", "data", "descricao", "doc", "valor", "saldo",
    "categoria", "fornecedor", "nf", "parcela", "datas_livres", "status_match",
    "origem", "created_at", "updated_at"
]

# ==============================
# Helpers de Persistência
# ==============================

def load_transactions():
    if TX_FILE.exists():
        df = pd.read_csv(TX_FILE, dtype=str)
        # normalizações de tipos
        for col in ["valor", "saldo"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        if "data" in df.columns:
            df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
        # listas serializadas (datas_livres)
        if "datas_livres" in df.columns:
            df["datas_livres"] = df["datas_livres"].apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith("[") else [])
        # garantir colunas
        missing = [c for c in TX_COLUMNS if c not in df.columns]
        for c in missing:
            if c in ["valor", "saldo"]:
                df[c] = 0.0
            elif c == "datas_livres":
                df[c] = [[] for _ in range(len(df))]
            else:
                df[c] = ""
        return df[TX_COLUMNS]
    else:
        return pd.DataFrame(columns=TX_COLUMNS)

def save_transactions(df: pd.DataFrame):
    df = df.copy()
    if "datas_livres" in df.columns:
        df["datas_livres"] = df["datas_livres"].apply(lambda x: json.dumps(x if isinstance(x, list) else []))
    df.to_csv(TX_FILE, index=False)

def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default

def save_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def load_rules():
    rules = load_json(RULES_FILE, DEFAULT_RULES)
    for r in rules:
        r.setdefault("contains", [])
        r.setdefault("in_doc", [])
        r.setdefault("category", "")
        r.setdefault("supplier", "")
    return rules

def load_categories():
    cats = load_json(CATS_FILE, DEFAULT_CATEGORIES)
    return sorted(list(dict.fromkeys(cats)))

def load_suppliers():
    sups = load_json(SUPP_FILE, ["", "Thor"])
    return sorted(list(dict.fromkeys(sups)))

# ==============================
# Classificação automática
# ==============================

AUTO_NO_MATCH_CATS = {
    "Deduções > Taxas/Chargebacks (marketplace)",
    "Despesas > Tarifas bancárias"
}

def apply_auto_classification(df: pd.DataFrame, rules):
    if df.empty: return df
    df = df.copy()
    df["descricao_low"] = df["descricao"].fillna("").str.lower()
    df["doc_low"] = df["doc"].fillna("").str.lower()

    for r in rules:
        terms = r.get("contains", [])
        terms_doc = r.get("in_doc", [])
        cat = r.get("category", "")
        sup = r.get("supplier", "")

        mask = False
        for t in terms:
            mask = mask | df["descricao_low"].str.contains(t, na=False)
        for d in terms_doc:
            mask = mask | df["doc_low"].str.contains(d, na=False)

        if cat:
            df.loc[mask & (df["categoria"].isna() | (df["categoria"] == "")), "categoria"] = cat
        if sup:
            df.loc[mask & (df["fornecedor"].isna() | (df["fornecedor"] == "")), "fornecedor"] = sup

        # saídas reconhecidas como tarifas/chargebacks não exigem match
        df.loc[mask & (df["valor"] < 0) & (df["categoria"].isin(AUTO_NO_MATCH_CATS)), "status_match"] = "N/A"

    df.drop(columns=["descricao_low","doc_low"], inplace=True)
    return df

def learn_rule_from_match(descricao: str, categoria: str, fornecedor: str):
    desc_low = (descricao or "").strip().lower()
    if not desc_low or not categoria: return None
    tokens = re.findall(r"[a-zA-Z0-9]{4,}", desc_low)
    if not tokens: return None
    token = tokens[0]
    return {"contains": [token], "in_doc": [], "category": categoria, "supplier": fornecedor or ""}

# ==============================
# UI Helpers
# ==============================

def pill(text, color="#334155", text_color="#fff"):
    st.markdown(f"""
        <span class="pill" style="background:{color};color:{text_color};padding:6px 10px;border-radius:999px;font-size:12px;margin-left:8px">{text}</span>
    """, unsafe_allow_html=True)

def header(title, company=None):
    color = COMPANY_COLORS.get(company, None) if company else None
    st.markdown(f"### {title}")
    if company and color:
        st.markdown(f"<div style='height:4px;background:{color};border-radius:4px;margin:-10px 0 10px 0;'></div>", unsafe_allow_html=True)

# ==============================
# Tema claro / Visual moderno
# ==============================

GLOBAL_CSS = """
<style>
:root{
  --bg:#ffffff;
  --surface:#f8fafc;
  --card:#ffffff;
  --text:#0f172a;
  --muted:#64748b;
  --border:#e5e7eb;
  --primary:#0ea5e9;
  --success:#16a34a;
  --warning:#f59e0b;
  --danger:#ef4444;
  --radius:14px;
  --shadow:0 10px 20px rgba(2,6,23,0.06);
}
html, body, [data-testid="stAppViewContainer"]{ background:var(--bg)!important; color:var(--text)!important; }
[data-testid="stSidebar"]{ background:#ffffff!important; border-right:1px solid var(--border); }
.stButton>button{ border-radius:12px!important; padding:8px 14px!important; border:1px solid var(--border)!important; box-shadow:var(--shadow)!important; }
.stButton>button:hover{ transform:translateY(-1px); }
.stSelectbox>div>div, .stTextInput>div>div, .stDateInput>div>div, .stNumberInput>div>div{
  border-radius:10px!important; border:1px solid var(--border)!important; box-shadow:none!important;
}
.pill{ display:inline-block; padding:6px 10px; border-radius:999px; font-size:12px; color:#fff; }
</style>
"""

# ==============================
# Login simples (senha única)
# ==============================

def simple_login():
    if "auth" not in st.session_state:
        st.session_state.auth = False
    if st.session_state.auth:
        return True
    with st.sidebar:
        st.subheader("🔐 Acesso")
        pwd = st.text_input("Senha única (temporário)", type="password", help="Definir auth melhor depois. Padrão: alivvia2025")
        if st.button("Entrar"):
            if pwd == "alivvia2025":
                st.session_state.auth = True
                st.success("Acesso liberado.")
            else:
                st.error("Senha incorreta.")
    return st.session_state.auth

# ==============================
# Importação (CSV/XLSX + Mapeamento)
# ==============================

def importar_extrato_view():
    st.sidebar.header("⚙️ Configurações rápidas")
    empresa = st.sidebar.selectbox("Empresa", ["Alivvia", "JCA"])
    conta = st.sidebar.selectbox("Conta bancária", ["Mercado Pago", "Banco do Brasil", "Itaú", "Outra"])
    header("📥 Importar Extrato (CSV/XLSX)", empresa)

    st.info("Primeiro alvo: **Mercado Pago**. Depois adicionamos outros bancos.", icon="ℹ️")

    up = st.file_uploader("Selecione o arquivo (.csv ou .xlsx)", type=["csv", "xlsx"])

    col_map_defaults = {
        "data": "RELEASE_DATE",
        "descricao": "TRANSACTION_TYPE",
        "doc": "REFERENCE_ID",
        "valor": "TRANSACTION_NET_AMOUNT",
        "saldo": "PARTIAL_BALANCE",
    }

    if up is not None:
        try:
            if up.name.endswith(".csv"):
                raw = pd.read_csv(up, dtype=str)
            else:
                raw = pd.read_excel(up, dtype=str)
        except Exception as e:
            st.error(f"Erro ao ler arquivo: {e}")
            return

        st.write("Prévia do arquivo:")
        st.dataframe(raw.head(30), use_container_width=True)

        # Mapeamento de Colunas
        st.subheader("Mapeamento de Colunas")
        cols = list(raw.columns)
        map_data = {}
        c1, c2 = st.columns(2)
        with c1:
            map_data["data"] = st.selectbox("Campo interno: data", options=["(vazio)"] + cols, index=(cols.index(col_map_defaults["data"]) + 1) if col_map_defaults["data"] in cols else 0)
            map_data["descricao"] = st.selectbox("Campo interno: descricao", options=["(vazio)"] + cols, index=(cols.index(col_map_defaults["descricao"]) + 1) if col_map_defaults["descricao"] in cols else 0)
            map_data["doc"] = st.selectbox("Campo interno: doc", options=["(vazio)"] + cols, index=(cols.index(col_map_defaults["doc"]) + 1) if col_map_defaults["doc"] in cols else 0)
        with c2:
            map_data["valor"] = st.selectbox("Campo interno: valor (R$)", options=["(vazio)"] + cols, index=(cols.index(col_map_defaults["valor"]) + 1) if col_map_defaults["valor"] in cols else 0)
            map_data["saldo"] = st.selectbox("Campo interno: saldo (R$)", options=["(vazio)"] + cols, index=(cols.index(col_map_defaults["saldo"]) + 1) if col_map_defaults["saldo"] in cols else 0)

        if st.button("Processar e Importar"):
            tmp = pd.DataFrame()
            for k, src in map_data.items():
                if src != "(vazio)" and src in raw.columns:
                    tmp[k] = raw[src].copy()
                else:
                    tmp[k] = ""

            # normalizações
            tmp["data"] = pd.to_datetime(tmp["data"], errors="coerce").dt.date
            for col in ["valor", "saldo"]:
                v = tmp[col].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
                tmp[col] = pd.to_numeric(v, errors="coerce").fillna(0.0)

            # campos fixos
            tmp["empresa"] = empresa
            tmp["conta"] = conta
            tmp["categoria"] = ""
            tmp["fornecedor"] = ""
            tmp["nf"] = ""
            tmp["parcela"] = ""
            tmp["datas_livres"] = [[] for _ in range(len(tmp))]
            tmp["status_match"] = np.where(tmp["valor"] < 0, "Pendente", "N/A")  # só match em saídas
            now = datetime.utcnow().isoformat()
            tmp["created_at"] = now
            tmp["updated_at"] = now
            tmp["origem"] = "EXTRATO"

            # gerar tx_id
            tmp["tx_id"] = tmp.apply(lambda r: hashlib.md5(f"{empresa}-{conta}-{r['data']}-{r['descricao']}-{r['doc']}-{r['valor']}".encode("utf-8")).hexdigest(), axis=1)

            # juntar com base
            base = load_transactions()
            before = len(base)
            merged = pd.concat([base, tmp], ignore_index=True)
            merged.drop_duplicates(subset=["tx_id"], keep="first", inplace=True)

            # regras automáticas
            rules = load_rules()
            merged = apply_auto_classification(merged, rules)

            save_transactions(merged)
            st.success(f"Importação concluída. {len(merged) - before} novos lançamentos adicionados.")

    # Export base completa (amostra)
    st.divider()
    base = load_transactions()
    st.caption("Base atual (amostra)")
    st.dataframe(base.tail(50), use_container_width=True)
    st.download_button("⬇️ Exportar base completa (CSV)", data=base.to_csv(index=False).encode("utf-8"), file_name="transactions_base.csv", mime="text/csv")

# ==============================
# Lançamentos Manuais (Boletos/PIX/Transf.)
# ==============================

def lancamentos_view():
    empresa = st.sidebar.selectbox("Empresa", ["Alivvia", "JCA"], key="empresa_lanc")
    header("🧾 Lançamentos Manuais", empresa)

    df = load_transactions()

    st.subheader("Novo Lançamento")
    with st.form("form_lanc"):
        c1, c2, c3 = st.columns([1,1,1])
        with c1:
            data_l = st.date_input("Data", value=date.today())
            valor_l = st.number_input("Valor (negativo=saída, positivo=entrada)", value=0.0, step=0.01, format="%.2f")
        with c2:
            conta_l = st.selectbox("Conta", ["Mercado Pago","Banco do Brasil","Itaú","Outra"])
            doc_l = st.text_input("Doc/Referência", "")
        with c3:
            descricao_l = st.text_input("Descrição", "")

        cats = load_categories()
        sups = load_suppliers()

        c4, c5, c6 = st.columns([1,1,1])
        with c4:
            categoria_l = st.selectbox("Categoria", options=cats + ["+ Adicionar nova..."])
            if categoria_l == "+ Adicionar nova...":
                nova = st.text_input("Nova categoria")
                if nova:
                    cats.append(nova); save_json(CATS_FILE, cats); categoria_l = nova
        with c5:
            fornecedor_l = st.selectbox("Fornecedor", options=sups + ["+ Adicionar novo..."])
            if fornecedor_l == "+ Adicionar novo...":
                novo = st.text_input("Novo fornecedor")
                if novo:
                    sups.append(novo); save_json(SUPP_FILE, sups); fornecedor_l = novo
        with c6:
            nf_l = st.text_input("NF (opcional, sai nos relatórios)","")
            parcela_l = st.text_input("Parcela (ex: 1/3)","")

        st.caption("Datas livres (ex.: vencimentos)")
        d1, d2, d3 = st.columns(3)
        dl1 = d1.date_input("Data 1", value=None, key="l_dl1")
        dl2 = d2.date_input("Data 2", value=None, key="l_dl2")
        dl3 = d3.date_input("Data 3", value=None, key="l_dl3")
        datas_livres_l = [d for d in [dl1, dl2, dl3] if d]

        submit = st.form_submit_button("💾 Salvar lançamento")
        if submit:
            now = datetime.utcnow().isoformat()
            base = {
                "empresa": empresa,
                "conta": conta_l,
                "data": data_l,
                "descricao": descricao_l,
                "doc": doc_l,
                "valor": float(valor_l),
                "saldo": 0.0,
                "categoria": categoria_l or "",
                "fornecedor": fornecedor_l or "",
                "nf": nf_l or "",
                "parcela": parcela_l or "",
                "datas_livres": datas_livres_l,
                "status_match": "Pendente" if float(valor_l) < 0 else "N/A",
                "origem": "MANUAL",
                "created_at": now,
                "updated_at": now
            }
            # tx_id único p/ manuais
            base["tx_id"] = hashlib.md5(f"{empresa}-{conta_l}-{data_l}-{descricao_l}-{doc_l}-{valor_l}-{uuid.uuid4()}".encode("utf-8")).hexdigest()

            df2 = pd.concat([df, pd.DataFrame([base])], ignore_index=True)
            save_transactions(df2)
            st.success("Lançamento criado com sucesso.")

    st.markdown("---")
    st.subheader("Lançamentos Manuais - lista e edição")

    cA, cB = st.columns(2)
    with cA:
        mostrarmes = st.checkbox("Mostrar apenas mês atual", value=True)
    with cB:
        so_man = st.checkbox("Somente origem MANUAL", value=True)

    df = load_transactions()
    mask = (df["empresa"] == empresa)
    if so_man:
        mask = mask & (df["origem"] == "MANUAL")
    if mostrarmes:
        hj = date.today()
        mask = mask & (pd.to_datetime(df["data"]).dt.year == hj.year) & (pd.to_datetime(df["data"]).dt.month == hj.month)

    lista = df[mask].copy().sort_values(by="data", ascending=False)
    st.dataframe(lista[["data","descricao","valor","categoria","fornecedor","nf","parcela","doc","status_match","origem"]], use_container_width=True)

    if not lista.empty:
        options = [(f"{r.data} | {r.descricao} | R$ {r.valor:.2f}", r.tx_id) for r in lista.itertuples()]
        chosen = st.selectbox("Selecionar para editar/excluir", options=options, format_func=lambda x: x[0])

        if chosen:
            _, txid = chosen
            row = df[df["tx_id"] == txid].iloc[0]

            st.write("**Editar Lançamento**")
            with st.form("form_edit"):
                d1, d2, d3 = st.columns(3)
                with d1:
                    e_data = st.date_input("Data", value=pd.to_datetime(row["data"]).date() if row["data"] else date.today())
                    e_valor = st.number_input("Valor", value=float(row["valor"]), step=0.01, format="%.2f")
                    e_conta = st.selectbox("Conta", ["Mercado Pago","Banco do Brasil","Itaú","Outra"],
                                           index=(["Mercado Pago","Banco do Brasil","Itaú","Outra"].index(row["conta"]) if row["conta"] in ["Mercado Pago","Banco do Brasil","Itaú","Outra"] else 0))
                with d2:
                    e_doc = st.text_input("Doc/Referência", value=row["doc"])
                    e_desc = st.text_input("Descrição", value=row["descricao"])
                with d3:
                    e_nf = st.text_input("NF", value=row["nf"])
                    e_parc = st.text_input("Parcela", value=row["parcela"])

                e_cat = st.selectbox("Categoria", options=load_categories(),
                                     index=(load_categories().index(row["categoria"]) if row["categoria"] in load_categories() else 0))
                e_sup = st.selectbox("Fornecedor", options=load_suppliers(),
                                     index=(load_suppliers().index(row["fornecedor"]) if row["fornecedor"] in load_suppliers() else 0))

                ok_save = st.form_submit_button("💾 Salvar alterações")
                if ok_save:
                    df.loc[df["tx_id"] == txid, ["data","valor","conta","doc","descricao","nf","parcela","categoria","fornecedor","updated_at"]] = [
                        e_data, float(e_valor), e_conta, e_doc, e_desc, e_nf, e_parc, e_cat, e_sup, datetime.utcnow().isoformat()
                    ]
                    if float(e_valor) < 0 and df.loc[df["tx_id"]==txid,"status_match"].iloc[0] == "N/A":
                        df.loc[df["tx_id"]==txid,"status_match"] = "Pendente"
                    save_transactions(df)
                    st.success("Alterações salvas.")

            if st.button("🗑️ Excluir lançamento"):
                df = df[df["tx_id"] != txid].copy()
                save_transactions(df)
                st.warning("Lançamento excluído.")

# ==============================
# Conciliação (Match somente saídas)
# ==============================

def conciliacao_view():
    empresa = st.sidebar.selectbox("Empresa", ["Alivvia", "JCA"], key="empresa_conc")
    header("🔗 Conciliação Bancária (somente saídas)", empresa)

    df = load_transactions()
    if df.empty:
        st.info("Nenhum lançamento. Importe um extrato primeiro.")
        return

    # Filtros
    c1, c2, c3 = st.columns(3)
    with c1:
        status = st.selectbox("Status", ["Pendente", "Todos (saídas)"])
    with c2:
        dt_ini = st.date_input("Data inicial", value=None)
    with c3:
        dt_fim = st.date_input("Data final", value=None)

    c4, c5 = st.columns(2)
    with c4:
        cat_filtro = st.selectbox("Categoria (opcional)", options=["(todas)"] + load_categories())
    with c5:
        busca = st.text_input("Buscar por texto (descrição/doc)", "")

    mask = (df["empresa"] == empresa) & (df["valor"] < 0)
    if status == "Pendente":
        mask = mask & (df["status_match"] == "Pendente")
    if dt_ini:
        mask = mask & (pd.to_datetime(df["data"]) >= pd.to_datetime(dt_ini))
    if dt_fim:
        mask = mask & (pd.to_datetime(df["data"]) <= pd.to_datetime(dt_fim))
    if cat_filtro != "(todas)":
        mask = mask & (df["categoria"] == cat_filtro)
    if busca:
        low = busca.lower()
        mask = mask & (df["descricao"].fillna("").str.lower().str.contains(low) |
                       df["doc"].fillna("").str.lower().str.contains(low))

    view = df[mask].copy().sort_values(by=["data"], ascending=False)
    view["nf_view"] = view["nf"].apply(lambda x: "S/NF" if (pd.isna(x) or str(x).strip()=="") else str(x).strip())

    st.write(f"**{len(view)}** lançamentos filtrados.")
    st.dataframe(view[["data", "descricao", "doc", "valor", "categoria", "fornecedor", "nf_view", "parcela", "status_match"]].head(300), use_container_width=True)

    st.download_button(
        "⬇️ Exportar Conciliação (CSV - filtro atual)",
        data=view.to_csv(index=False).encode("utf-8"),
        file_name="conciliacao_filtro.csv",
        mime="text/csv"
    )

    st.subheader("Fazer Match")
    st.caption("Selecione um lançamento de saída e preencha os campos.")

    options = [(f"{r.data} | {r.descricao} | R$ {r.valor:.2f}", r.tx_id) for r in view.itertuples()]
    if not options:
        st.info("Nada para conciliar no filtro atual.")
        return

    selected = st.selectbox("Lançamento", options=options, format_func=lambda x: x[0])
    if selected:
        _, tx_id = selected
        row = df[df["tx_id"] == tx_id].iloc[0]

        cats = load_categories()
        sups = load_suppliers()

        st.write("**Detalhes do lançamento**")
        c1, c2, c3, c4 = st.columns(4)
        with c1: st.write("Data:", row["data"])
        with c2: st.write("Descrição:", row["descricao"])
        with c3: st.write("Doc:", row["doc"])
        with c4: st.write("Valor (R$):", f"{row['valor']:.2f}")

        st.markdown("---")
        st.write("**Preencha o Match**")

        col1, col2 = st.columns([2,1])
        with col1:
            categoria = st.selectbox("Categoria", options=cats + ["+ Adicionar nova..."], index=(cats.index(row["categoria"]) if row["categoria"] in cats else 0) if cats else 0)
            if categoria == "+ Adicionar nova...":
                nova = st.text_input("Nova categoria")
                if nova:
                    cats.append(nova); save_json(CATS_FILE, cats); categoria = nova

            fornecedor = st.selectbox("Fornecedor", options=sups + ["+ Adicionar novo..."], index=(sups.index(row["fornecedor"]) if row["fornecedor"] in sups else 0) if sups else 0)
            if fornecedor == "+ Adicionar novo...":
                novo = st.text_input("Novo fornecedor")
                if novo:
                    sups.append(novo); save_json(SUPP_FILE, sups); fornecedor = novo
        with col2:
            nf = st.text_input("NF (opcional, sai no relatório)", value=row["nf"] or "")
            parcela = st.text_input("Parcela (ex: 1/3)", value=row["parcela"] or "")

        st.write("**Datas livres (ex: vencimentos)**")
        dcol1, dcol2, dcol3 = st.columns(3)
        d1 = dcol1.date_input("Data 1", value=None, key="dl1")
        d2 = dcol2.date_input("Data 2", value=None, key="dl2")
        d3 = dcol3.date_input("Data 3", value=None, key="dl3")
        datas_livres = [d for d in [d1, d2, d3] if d]

        cA, cB, cC = st.columns(3)
        with cA:
            if st.button("✅ Salvar Match"):
                df.loc[df["tx_id"] == tx_id, ["categoria","fornecedor","nf","parcela","datas_livres","status_match","updated_at"]] = [
                    categoria, fornecedor, nf, parcela, datas_livres, "Conciliado", datetime.utcnow().isoformat()
                ]
                save_transactions(df)
                # aprendizado
                rule = learn_rule_from_match(row["descricao"], categoria, fornecedor)
                if rule:
                    rules = load_rules()
                    rules.insert(0, rule)
                    save_json(RULES_FILE, rules)
                st.success("Match salvo e regra aprendida (quando possível).")
        with cB:
            if st.button("✏️ Editar (reabrir) Match"):
                df.loc[df["tx_id"] == tx_id, ["status_match","updated_at"]] = ["Pendente", datetime.utcnow().isoformat()]
                save_transactions(df)
                st.warning("Match reaberto para edição.")
        with cC:
            if st.button("↩️ Desfazer (limpar campos)"):
                df.loc[df["tx_id"] == tx_id, ["categoria","fornecedor","nf","parcela","datas_livres","status_match","updated_at"]] = [
                    "", "", "", "", [], "Pendente", datetime.utcnow().isoformat()
                ]
                save_transactions(df)
                st.info("Match desfeito.")

# ==============================
# Relatórios (DRE + Contábil)
# ==============================

def relatorios_view():
    empresa = st.sidebar.selectbox("Empresa", ["Alivvia", "JCA"], key="empresa_rep")
    header("📊 Relatórios", empresa)

    df = load_transactions()
    if df.empty:
        st.info("Nenhum lançamento. Importe um extrato primeiro.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        ano = st.number_input("Ano", min_value=2020, max_value=2100, value=datetime.now().year)
    with c2:
        mes = st.number_input("Mês", min_value=1, max_value=12, value=datetime.now().month)
    with c3:
        st.write("Tolerância de match:", f"R$ {TOLERANCIA_MATCH:.2f}")
        pill(f"Empresa: {empresa}", COMPANY_COLORS[empresa])

    dfE = df[(df["empresa"] == empresa) &
             (pd.to_datetime(df["data"]).dt.year == ano) &
             (pd.to_datetime(df["data"]).dt.month == mes)].copy()

    # ---------- DRE ----------
    st.subheader("DRE (mês)")

    # Consolidar ENTRADAS diárias que são repasse marketplace
    repasse_mask = (dfE["categoria"] == "Receita > Vendas (repasse marketplace)") & (dfE["valor"] > 0)
    entradas_por_dia = dfE.loc[repasse_mask].groupby(pd.to_datetime(dfE.loc[repasse_mask, "data"]))["valor"].sum()
    receita_bruta = float(entradas_por_dia.sum())

    devol = dfE.loc[dfE["categoria"] == "Receita > Estorno/Devolução","valor"].sum()
    receita_liq = receita_bruta + devol  # devoluções geralmente negativas → somar ajusta

    despesas = dfE[dfE["valor"] < 0].groupby("categoria", dropna=False)["valor"].sum().sort_values()
    st.write(f"**Receita Bruta (repasse consolidado/dia):** R$ {receita_bruta:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    st.write(f"**(-) Devoluções:** R$ {devol:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    st.write(f"**= Receita Líquida:** R$ {receita_liq:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    st.write("**(-) Despesas por Categoria:**")
    if not despesas.empty:
        st.dataframe(despesas.reset_index().rename(columns={"valor":"Total (R$)"}), use_container_width=True)
    total_desp = despesas.sum()
    resultado = receita_liq + total_desp
    st.write(f"**= Resultado do Período:** R$ {resultado:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    # ---------- Relatório Contábil ----------
    st.subheader("Relatório Contábil (Diário/Mensal)")

    rep = dfE.copy()

    # NF: exibir "S/NF" quando vazio
    rep["nf_out"] = rep["nf"].apply(lambda x: "S/NF" if (pd.isna(x) or str(x).strip() == "") else str(x).strip())

    # Destino (categoria/fornecedor)
    rep["destino"] = rep.apply(lambda r: f"{r['categoria'] or ''} | {r['fornecedor'] or ''}".strip(" |"), axis=1)

    # Colunas Entrada/Saída
    rep["Entrada"] = rep["valor"].apply(lambda v: v if v > 0 else 0.0)
    rep["Saída"] = rep["valor"].apply(lambda v: abs(v) if v < 0 else 0.0)

    # Ordenar e saldo acumulado (por mês/empresa)
    rep = rep.sort_values(by=["data", "descricao"])
    rep["Saldo Acumulado"] = rep["valor"].cumsum()

    # Consolidar ENTRADAS por dia para contábil
    entradas = rep[rep["categoria"] == "Receita > Vendas (repasse marketplace)"].copy()
    saidas = rep[rep["categoria"] != "Receita > Vendas (repasse marketplace)"].copy()

    if not entradas.empty:
        ent_group = entradas.groupby("data", as_index=False).agg({"valor":"sum"})
        ent_group["descricao"] = "Repasse marketplace (consolidado)"
        ent_group["doc"] = ""
        ent_group["fornecedor"] = ""
        ent_group["categoria"] = "Receita > Vendas (repasse marketplace)"
        ent_group["nf_out"] = "S/NF"
        ent_group["destino"] = "Receita > Vendas (repasse marketplace)"
        ent_group["Entrada"] = ent_group["valor"].apply(lambda v: v if v > 0 else 0.0)
        ent_group["Saída"] = 0.0
        entradas_fmt = ent_group[["data","descricao","destino","Entrada","Saída","nf_out","doc","valor","categoria","fornecedor","empresa"]].copy()
    else:
        entradas_fmt = pd.DataFrame(columns=["data","descricao","destino","Entrada","Saída","nf_out","doc","valor","categoria","fornecedor","empresa"])

    saidas["Entrada"] = 0.0
    saidas_fmt = saidas[["data","descricao","destino","Entrada","Saída","nf_out","doc","valor","categoria","fornecedor","empresa"]].copy()

    contabil = pd.concat([entradas_fmt, saidas_fmt], ignore_index=True).sort_values(by=["data","descricao"])
    contabil["Saldo Acumulado"] = contabil["valor"].cumsum()

    contabil_out = contabil.rename(columns={
        "data":"Data",
        "descricao":"Movimentação/Descrição",
        "destino":"Destino (categoria/fornecedor)",
        "nf_out":"NF",
        "empresa":"Empresa"
    })[["Data","Movimentação/Descrição","Destino (categoria/fornecedor)","Entrada","Saída","NF","Saldo Acumulado","Empresa"]]

    st.dataframe(contabil_out, use_container_width=True)

    st.download_button(
        "⬇️ Exportar DRE (CSV)",
        data=despesas.reset_index().to_csv(index=False).encode("utf-8"),
        file_name=f"DRE_{empresa}_{ano}-{mes:02d}.csv",
        mime="text/csv"
    )
    st.download_button(
        "⬇️ Exportar Relatório Contábil (CSV)",
        data=contabil_out.to_csv(index=False).encode("utf-8"),
        file_name=f"RELATORIO_CONTABIL_{empresa}_{ano}-{mes:02d}.csv",
        mime="text/csv"
    )

# ==============================
# Configurações (Categorias/Fornecedores/Regras)
# ==============================

def configuracoes_view():
    header("🔧 Configurações")
    # categorias
    cats = load_categories()
    st.write("**Categorias**")
    new_cat = st.text_input("Adicionar categoria")
    if st.button("Adicionar categoria"):
        if new_cat and new_cat not in cats:
            cats.append(new_cat); save_json(CATS_FILE, cats); st.success("Categoria adicionada.")
    if st.button("Salvar categorias"):
        save_json(CATS_FILE, [c for c in cats if c]); st.success("Categorias salvas.")
    st.dataframe(pd.DataFrame({"categoria": cats}), use_container_width=True)

    st.markdown("---")
    st.write("**Fornecedores**")
    sups = load_suppliers()
    new_sup = st.text_input("Adicionar fornecedor")
    if st.button("Adicionar fornecedor"):
        if new_sup and new_sup not in sups:
            sups.append(new_sup); save_json(SUPP_FILE, sups); st.success("Fornecedor adicionado.")
    if st.button("Salvar fornecedores"):
        save_json(SUPP_FILE, [s for s in sups if s]); st.success("Fornecedores salvos.")
    st.dataframe(pd.DataFrame({"fornecedor": sups}), use_container_width=True)

    st.markdown("---")
    st.write("**Regras automáticas (aprendizado)**")
    rules = load_rules()
    st.caption("As regras são aprendidas ao salvar matches. Você pode revisar/exportar abaixo.")
    st.json(rules)

    st.markdown("---")
    st.write("**Manutenção de base**")
    base = load_transactions()
    st.download_button("⬇️ Baixar base completa (CSV)", data=base.to_csv(index=False).encode("utf-8"), file_name="transactions_base.csv", mime="text/csv")
    if st.button("Limpar base (cuidado!)"):
        TX_FILE.unlink(missing_ok=True)
        st.warning("Base apagada. Reimporte seus extratos.")

# ==============================
# Main
# ==============================

def main():
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

    if not simple_login():
        st.stop()

    with st.sidebar:
        st.header("🧭 Navegação")
        page = st.radio("Ir para", ["Importar Extrato", "Lançamentos", "Conciliação", "Relatórios", "Configurações"])

    if page == "Importar Extrato":
        importar_extrato_view()
    elif page == "Lançamentos":
        lancamentos_view()
    elif page == "Conciliação":
        conciliacao_view()
    elif page == "Relatórios":
        relatorios_view()
    else:
        configuracoes_view()

if __name__ == "__main__":
    main()
