# Alivvia Gestão - app.py
# Autor: Paulo Piva + ChatGPT
# Objetivo: Sistema financeiro simples (Importar → Conciliação → Relatórios) com persistência local.
# Rodar: streamlit run app.py
# Dependências: streamlit, pandas, numpy, openpyxl (para .xlsx)

import streamlit as st
import pandas as pd
import numpy as np
import json
import hashlib
from datetime import datetime, date
from pathlib import Path

# ==============================
# Config & Constantes
# ==============================

st.set_page_config(page_title="Alivvia Gestão", layout="wide")

DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)

TX_FILE = DATA_DIR / "transactions.csv"         # base principal (extrato + campos de conciliação)
RULES_FILE = DATA_DIR / "rules.json"            # regras automáticas (aprendizado)
CATS_FILE = DATA_DIR / "categories.json"        # categorias
SUPP_FILE = DATA_DIR / "suppliers.json"         # fornecedores

TOLERANCIA_MATCH = 0.10  # R$ 0,10

# Cores por empresa
COMPANY_COLORS = {
    "Alivvia": "#15A34A", # verde
    "JCA": "#7C3AED"      # roxo
}

# Categorias iniciais (árvore simples → exibiremos em lista)
DEFAULT_CATEGORIES = [
    "Receita > Vendas (marketplace)",
    "Receita > Estorno/Devolução",
    "Custo > Frete",
    "Custo > Fornecedores",
    "Despesas > Tarifas bancárias",
    "Despesas > Impostos/Taxas",
    "Despesas > Marketing",
    "Despesas > Salários/Encargos",
    "Transferência entre empresas",
    "Retirada dos sócios",
    "Outros"
]

# Auto-classificação por termos (palavras minúsculas)
DEFAULT_RULES = [
    {"contains": ["entrada de dinheiro", "pix recebido", "qris"], "category": "Receita > Vendas (marketplace)", "supplier": ""},
    {"contains": ["devolu", "estorno", "reembolso"], "category": "Receita > Estorno/Devolução", "supplier": ""},
    {"contains": ["tarifa", "iof", "taxa"], "category": "Despesas > Tarifas bancárias", "supplier": ""},
    {"contains": ["jadlog", "correios", "total express"], "category": "Custo > Frete", "supplier": ""},
    {"contains": ["thor"], "category": "Custo > Fornecedores", "supplier": "Thor"}
]

# Campos padrão do modelo interno
TX_COLUMNS = [
    "tx_id", "empresa", "conta", "data", "descricao", "doc", "valor", "saldo",
    "categoria", "fornecedor", "nf", "parcela", "datas_livres", "status_match",
    "created_at", "updated_at"
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
        for col in ["data"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        # arrays armazenados como string → lista
        if "datas_livres" in df.columns:
            df["datas_livres"] = df["datas_livres"].apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith("[") else [])
        missing = [c for c in TX_COLUMNS if c not in df.columns]
        for c in missing:
            df[c] = "" if c not in ["valor", "saldo", "datas_livres"] else ([] if c=="datas_livres" else 0.0)
        return df[TX_COLUMNS]
    else:
        return pd.DataFrame(columns=TX_COLUMNS)

def save_transactions(df: pd.DataFrame):
    df = df.copy()
    # converter listas para json
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
    # garantir chaves
    for r in rules:
        r.setdefault("contains", [])
        r.setdefault("category", "")
        r.setdefault("supplier", "")
    return rules

def load_categories():
    cats = load_json(CATS_FILE, DEFAULT_CATEGORIES)
    return sorted(list(dict.fromkeys(cats)))  # dedup + ordenado

def load_suppliers():
    sups = load_json(SUPP_FILE, ["", "Thor"])
    return sorted(list(dict.fromkeys(sups)))

# ==============================
# Regras de Classificação
# ==============================

def apply_auto_classification(df: pd.DataFrame, rules):
    if df.empty:
        return df
    df = df.copy()
    df["descricao_low"] = df["descricao"].fillna("").str.lower()
    for r in rules:
        terms = r.get("contains", [])
        cat = r.get("category", "")
        sup = r.get("supplier", "")
        if terms:
            mask = False
            for t in terms:
                mask = mask | df["descricao_low"].str.contains(t, na=False)
            # somente preenche categoria/fornecedor quando estiver vazio
            if cat:
                df.loc[mask & (df["categoria"].isna() | (df["categoria"] == "")), "categoria"] = cat
            if sup:
                df.loc[mask & (df["fornecedor"].isna() | (df["fornecedor"] == "")), "fornecedor"] = sup
    df.drop(columns=["descricao_low"], inplace=True)
    return df

def learn_rule_from_match(descricao: str, categoria: str, fornecedor: str):
    desc_low = (descricao or "").strip().lower()
    if not desc_low or not categoria:
        return None
    # cria uma regra simples com uma palavra-chave (primeira palavra relevante)
    # heurística: pega tokens alfanuméricos com 4+ letras
    import re
    tokens = re.findall(r"[a-zA-Z0-9]{4,}", desc_low)
    if not tokens:
        return None
    token = tokens[0]
    return {"contains": [token], "category": categoria, "supplier": fornecedor or ""}

# ==============================
# UI Helpers
# ==============================

def pill(text, color="#334155", text_color="#fff"):
    st.markdown(f"""
        <span style="background:{color};color:{text_color};padding:6px 10px;border-radius:999px;font-size:12px;margin-right:6px;display:inline-block">{text}</span>
    """, unsafe_allow_html=True)

def header(title, company=None):
    color = COMPANY_COLORS.get(company, None) if company else None
    st.markdown(f"### {title}")
    if company and color:
        st.markdown(f"<div style='height:4px;background:{color};border-radius:4px;margin:-10px 0 10px 0;'></div>", unsafe_allow_html=True)

# ==============================
# Sessão / "Login" simples
# ==============================

def simple_login():
    if "auth" not in st.session_state:
        st.session_state.auth = False
    if st.session_state.auth:
        return True
    with st.sidebar:
        st.subheader("🔐 Acesso")
        senha = st.text_input("Senha única (temporário)", type="password", help="Definir mais tarde via auth real. Padrão: alivvia2025")
        ok = st.button("Entrar")
        if ok:
            if senha == "alivvia2025":
                st.session_state.auth = True
                st.success("Acesso liberado.")
            else:
                st.error("Senha incorreta.")
    return st.session_state.auth

# ==============================
# Importador (CSV/XLSX + Mapeamento)
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

        # Mapeamento de colunas
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
            # construir df no formato interno
            tmp = pd.DataFrame()
            for k, src in map_data.items():
                if src != "(vazio)" and src in raw.columns:
                    tmp[k] = raw[src].copy()
                else:
                    tmp[k] = ""

            # normalizações
            # data
            tmp["data"] = pd.to_datetime(tmp["data"], errors="coerce").dt.date
            # valor/saldo numéricos
            for col in ["valor", "saldo"]:
                v = tmp[col].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
                tmp[col] = pd.to_numeric(v, errors="coerce").fillna(0.0)

            # preencher campos fixos
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

            # gerar tx_id
            tmp["tx_id"] = tmp.apply(lambda r: hashlib.md5(f"{empresa}-{conta}-{r['data']}-{r['descricao']}-{r['doc']}-{r['valor']}".encode("utf-8")).hexdigest(), axis=1)

            # carregar base existente
            base = load_transactions()

            # anti-duplicidade por tx_id
            before = len(base)
            merged = pd.concat([base, tmp], ignore_index=True)
            merged.drop_duplicates(subset=["tx_id"], keep="first", inplace=True)

            # regras automáticas
            rules = load_rules()
            merged = apply_auto_classification(merged, rules)

            save_transactions(merged)
            st.success(f"Importação concluída. {len(merged) - before} novos lançamentos adicionados.")

    # Export base completa
    st.divider()
    base = load_transactions()
    st.caption("Base atual (amostra)")
    st.dataframe(base.tail(50), use_container_width=True)
    st.download_button("⬇️ Exportar base completa (CSV)", data=base.to_csv(index=False).encode("utf-8"), file_name="transactions_base.csv", mime="text/csv")

# ==============================
# Conciliação (Match apenas saídas)
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

    mask = (df["empresa"] == empresa) & (df["valor"] < 0)
    if status == "Pendente":
        mask = mask & (df["status_match"] == "Pendente")

    if dt_ini:
        mask = mask & (pd.to_datetime(df["data"]) >= pd.to_datetime(dt_ini))
    if dt_fim:
        mask = mask & (pd.to_datetime(df["data"]) <= pd.to_datetime(dt_fim))

    view = df[mask].copy().sort_values(by=["data"], ascending=False)

    st.write(f"**{len(view)}** lançamentos filtrados.")
    st.dataframe(view[["data", "descricao", "doc", "valor", "categoria", "fornecedor", "nf", "parcela", "status_match"]].head(200), use_container_width=True)

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
        with c1:
            st.write("Data:", row["data"])
        with c2:
            st.write("Descrição:", row["descricao"])
        with c3:
            st.write("Doc:", row["doc"])
        with c4:
            st.write("Valor (R$):", f"{row['valor']:.2f}")

        st.markdown("---")
        st.write("**Preencha o Match**")

        col1, col2 = st.columns([2,1])
        with col1:
            categoria = st.selectbox("Categoria", options=cats + ["+ Adicionar nova..."], index=(cats.index(row["categoria"]) if row["categoria"] in cats else 0) if cats else 0)
            if categoria == "+ Adicionar nova...":
                nova = st.text_input("Nova categoria")
                if nova:
                    cats.append(nova)
                    save_json(CATS_FILE, cats)
                    categoria = nova

            fornecedor = st.selectbox("Fornecedor", options=sups + ["+ Adicionar novo..."], index=(sups.index(row["fornecedor"]) if row["fornecedor"] in sups else 0) if sups else 0)
            if fornecedor == "+ Adicionar novo...":
                novo = st.text_input("Novo fornecedor")
                if novo:
                    sups.append(novo)
                    save_json(SUPP_FILE, sups)
                    fornecedor = novo
        with col2:
            nf = st.text_input("NF (opcional, mas **sai no relatório**)", value=row["nf"] or "")
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

    dfE = df[(df["empresa"] == empresa) & (pd.to_datetime(df["data"]).dt.year == ano) & (pd.to_datetime(df["data"]).dt.month == mes)].copy()

    # ===== DRE (simplificada) =====
    st.subheader("DRE (mês)")
    rec_bruta = dfE.loc[dfE["categoria"] == "Receita > Vendas (marketplace)","valor"].sum()
    devol = dfE.loc[dfE["categoria"] == "Receita > Estorno/Devolução","valor"].sum()
    receita_liq = rec_bruta + devol  # devoluções geralmente negativas → somar ajusta

    despesas = dfE[dfE["valor"] < 0].groupby("categoria", dropna=False)["valor"].sum().sort_values()

    st.write(f"**Receita Bruta:** R$ {rec_bruta:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    st.write(f"**(-) Devoluções:** R$ {devol:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    st.write(f"**= Receita Líquida:** R$ {receita_liq:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    st.write("**(-) Despesas por Categoria:**")
    if not despesas.empty:
        st.dataframe(despesas.reset_index().rename(columns={"valor":"Total (R$)"}), use_container_width=True)
    total_desp = despesas.sum()
    resultado = receita_liq + total_desp  # despesas negativas

    st.write(f"**= Resultado do Período:** R$ {resultado:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    # ===== Relatório Contábil Diário/Mensal =====
    st.subheader("Relatório Contábil (Diário/Mensal)")
    cols_rep = ["data","descricao","valor","fornecedor","nf","parcela","categoria","empresa"]
    rep = dfE[cols_rep].sort_values(by=["data"])
    st.dataframe(rep, use_container_width=True)

    st.download_button("⬇️ Exportar DRE (CSV)", data=despesas.reset_index().to_csv(index=False).encode("utf-8"), file_name=f"DRE_{empresa}_{ano}-{mes:02d}.csv", mime="text/csv")
    st.download_button("⬇️ Exportar Relatório Contábil (CSV)", data=rep.to_csv(index=False).encode("utf-8"), file_name=f"RELATORIO_CONTABIL_{empresa}_{ano}-{mes:02d}.csv", mime="text/csv")

# ==============================
# Sidebar: Navegação
# ==============================

def importar_extrato_guard():
    try:
        importar_extrato_view()
    except Exception as e:
        st.error(f"Erro na tela Importar: {e}")

def conciliacao_guard():
    try:
        conciliacao_view()
    except Exception as e:
        st.error(f"Erro na tela Conciliação: {e}")

def relatorios_guard():
    try:
        relatorios_view()
    except Exception as e:
        st.error(f"Erro na tela Relatórios: {e}")

def main():
    st.markdown("""
        <style>
        .stButton>button { border-radius: 12px; padding: 8px 14px; }
        .stSelectbox, .stTextInput, .stDateInput { border-radius: 10px; }
        </style>
    """, unsafe_allow_html=True)

    if not simple_login():
        st.stop()

    with st.sidebar:
        st.header("🧭 Navegação")
        page = st.radio("Ir para", ["Importar Extrato", "Conciliação", "Relatórios", "Configurações"])

    if page == "Importar Extrato":
        importar_extrato_guard()
    elif page == "Conciliação":
        conciliacao_guard()
    elif page == "Relatórios":
        relatorios_guard()
    else:
        header("🔧 Configurações")
        cats = load_categories()
        st.write("**Categorias**")
        new_cat = st.text_input("Adicionar categoria")
        if st.button("Adicionar categoria"):
            if new_cat and new_cat not in cats:
                cats.append(new_cat)
                save_json(CATS_FILE, cats)
                st.success("Categoria adicionada.")
        if st.button("Salvar categorias"):
            save_json(CATS_FILE, [c for c in cats if c])
            st.success("Categorias salvas.")
        st.dataframe(pd.DataFrame({"categoria": cats}), use_container_width=True)

        st.markdown("---")
        st.write("**Fornecedores**")
        sups = load_suppliers()
        new_sup = st.text_input("Adicionar fornecedor")
        if st.button("Adicionar fornecedor"):
            if new_sup and new_sup not in sups:
                sups.append(new_sup)
                save_json(SUPP_FILE, sups)
                st.success("Fornecedor adicionado.")
        if st.button("Salvar fornecedores"):
            save_json(SUPP_FILE, [s for s in sups if s])
            st.success("Fornecedores salvos.")
        st.dataframe(pd.DataFrame({"fornecedor": sups}), use_container_width=True)

        st.markdown("---")
        st.write("**Regras automáticas (aprendizado)**")
        rules = load_rules()
        st.caption("As regras são aprendidas ao salvar matches. Você pode exportar/importar abaixo.")
        st.json(rules)

        st.markdown("---")
        st.write("**Manutenção de base**")
        base = load_transactions()
        st.download_button("⬇️ Baixar base completa (CSV)", data=base.to_csv(index=False).encode("utf-8"), file_name="transactions_base.csv", mime="text/csv")
        if st.button("Limpar base (cuidado!)"):
            TX_FILE.unlink(missing_ok=True)
            st.warning("Base apagada. Reimporte seus extratos.")

if __name__ == "__main__":
    main()
