import streamlit as st
import pandas as pd
from datetime import datetime
import os
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table

st.set_page_config(page_title="Sistema de Almoxarifado", layout="wide")

PASTA_IMAGENS = r"C:\Users\Dell\Desktop\Sistema Estoque\Imagens Produtos"

# =========================
# CARREGAR DADOS
# =========================
try:
    df_produtos = pd.read_excel("produtos.xlsx")
except:
    df_produtos = pd.DataFrame(columns=[
        "codigo","produto","categoria",
        "estoque_minimo","localizacao","imagem"
    ])

try:
    df_mov = pd.read_excel("movimentacoes.xlsx")
except:
    df_mov = pd.DataFrame(columns=[
        "produto","tipo","quantidade","data"
    ])

# =========================
# MENU
# =========================
st.sidebar.title("MENU")

if "menu" not in st.session_state:
    st.session_state["menu"] = "ESTOQUE"

if st.sidebar.button("📦 ESTOQUE"):
    st.session_state["menu"] = "ESTOQUE"

if st.sidebar.button("🛒 COMPRAS"):
    st.session_state["menu"] = "COMPRAS"

if st.sidebar.button("🔄 MOVIMENTAÇÃO"):
    st.session_state["menu"] = "MOVIMENTAÇÃO"

if st.sidebar.button("📋 CADASTRO DE PRODUTOS"):
    st.session_state["menu"] = "CADASTRO"

menu = st.session_state["menu"]

# =========================
# ESTOQUE
# =========================
def calcular_estoque():
    if df_mov.empty:
        return pd.Series(dtype=float)

    ent = df_mov[df_mov["tipo"]=="Entrada"].groupby("produto")["quantidade"].sum()
    sai = df_mov[df_mov["tipo"]=="Saída"].groupby("produto")["quantidade"].sum()

    return ent.subtract(sai, fill_value=0)

df_produtos["estoque_atual"] = df_produtos["produto"].map(calcular_estoque()).fillna(0)

df_produtos["situacao"] = df_produtos.apply(
    lambda x: "🔴 ESTOQUE BAIXO" if x["estoque_atual"] <= x["estoque_minimo"] else "🟢 OK",
    axis=1
)

# =========================
# ABA ESTOQUE
# =========================
if menu == "ESTOQUE":
    st.title("📦 Estoque Geral")

    headers = st.columns([1,2,2,1,1,2,2,3])
    headers[0].write("Código")
    headers[1].write("Produto")
    headers[2].write("Categoria")
    headers[3].write("Estoque")
    headers[4].write("Mínimo")
    headers[5].write("Localização")
    headers[6].write("Situação")
    headers[7].write("Imagem")

    for i, row in df_produtos.iterrows():
        col = st.columns([1,2,2,1,1,2,2,3])

        col[0].write(row["codigo"])

        if col[1].button(row["produto"], key=i):
            st.session_state["produto"] = row["produto"]

        col[2].write(row["categoria"])
        col[3].write(int(row["estoque_atual"]))
        col[4].write(row["estoque_minimo"])
        col[5].write(row["localizacao"])
        col[6].write(row["situacao"])

        img = os.path.join(PASTA_IMAGENS, str(row["imagem"]))
        if os.path.exists(img):
            col[7].image(img, width=120)

    # HISTÓRICO
    if "produto" in st.session_state:
        produto = st.session_state["produto"]
        st.divider()
        st.subheader(f"📊 Histórico - {produto}")

        hist = df_mov[df_mov["produto"]==produto].copy()
        if not hist.empty:
            hist["data"]=pd.to_datetime(hist["data"]).dt.strftime("%d/%m/%Y %H:%M")
            st.dataframe(hist)
        else:
            st.info("Sem movimentações")

        if st.button("Fechar"):
            del st.session_state["produto"]
            st.rerun()

# =========================
# COMPRAS
# =========================
elif menu == "COMPRAS":
    st.title("🛒 Compras")

    df = df_produtos.copy()
    df["necessita"] = (df["estoque_minimo"] + 5) - df["estoque_atual"]
    df = df[df["necessita"] > 0]

    # BOTÕES
    col1, col2 = st.columns(2)

    with col1:
        if st.button("📄 Gerar PDF"):
            pasta_downloads = os.path.join(os.path.expanduser("~"), "Downloads")
            caminho_pdf = os.path.join(pasta_downloads, "compras.pdf")

            data = [["Código","Produto","Atual","Mínimo","Necessita"]]

            for _, r in df.iterrows():
                data.append([
                    r["codigo"],
                    r["produto"],
                    int(r["estoque_atual"]),
                    int(r["estoque_minimo"]),
                    int(r["necessita"])
                ])

            pdf = SimpleDocTemplate(caminho_pdf, pagesize=letter)
            tabela = Table(data)
            pdf.build([tabela])

            st.success(f"PDF salvo em: {caminho_pdf}")

    with col2:
        if st.button("📂 Selecionar Categoria"):
            st.session_state["mostrar_categoria"] = True

    if "mostrar_categoria" not in st.session_state:
        st.session_state["mostrar_categoria"] = False

    if "categoria_sel" not in st.session_state:
        st.session_state["categoria_sel"] = "GERAL"

    if st.session_state["mostrar_categoria"]:
        categorias = ["GERAL"] + list(df_produtos["categoria"].dropna().unique())
        cols = st.columns(len(categorias))

        for i, cat in enumerate(categorias):
            if cols[i].button(cat):
                st.session_state["categoria_sel"] = cat

    if st.session_state["categoria_sel"] != "GERAL":
        df = df[df["categoria"] == st.session_state["categoria_sel"]]

    st.markdown("<br><br>", unsafe_allow_html=True)

    headers = st.columns([1,2,1,1,1,3])
    headers[0].write("Código")
    headers[1].write("Produto")
    headers[2].write("Atual")
    headers[3].write("Mínimo")
    headers[4].write("Necessita")
    headers[5].write("Imagem")

    for _, row in df.iterrows():
        col = st.columns([1,2,1,1,1,3])

        col[0].write(row["codigo"])
        col[1].write(row["produto"])
        col[2].write(int(row["estoque_atual"]))
        col[3].write(row["estoque_minimo"])
        col[4].write(int(row["necessita"]))

        img = os.path.join(PASTA_IMAGENS, str(row["imagem"]))
        if os.path.exists(img):
            col[5].image(img, width=120)

# =========================
# MOVIMENTAÇÃO
# =========================
elif menu == "MOVIMENTAÇÃO":
    st.title("🔄 Movimentação")

    if "lista_mov" not in st.session_state:
        st.session_state["lista_mov"] = []

    tipo = st.selectbox("Tipo", ["Entrada","Saída"])
    produto = st.selectbox("Produto", df_produtos["produto"])
    qtd = st.number_input("Quantidade",1)

    col1, col2 = st.columns(2)

    if col1.button("➕ Adicionar"):
        st.session_state["lista_mov"].append({
            "produto": produto,
            "tipo": tipo,
            "quantidade": qtd
        })

    if col2.button("💾 Salvar"):
        for item in st.session_state["lista_mov"]:
            nova = pd.DataFrame([{
                "produto": item["produto"],
                "tipo": item["tipo"],
                "quantidade": item["quantidade"],
                "data": datetime.now()
            }])
            df_mov = pd.concat([df_mov, nova])

        df_mov.to_excel("movimentacoes.xlsx", index=False)
        st.session_state["lista_mov"] = []
        st.success("Movimentações salvas")

    st.divider()
    for item in st.session_state["lista_mov"]:
        st.write(f"{item['produto']} | {item['tipo']} | {item['quantidade']}")

# =========================
# CADASTRO
# =========================
elif menu == "CADASTRO":
    st.title("📋 Cadastro")

    categorias = ["MANUTENÇÃO","ELÉTRICA","HIDRAULICA","LIMPEZA","COPA"]

    col1, col2, col3 = st.columns(3)

    if col1.button("➕ Adicionar"):
        st.session_state["acao"] = "Adicionar"

    if col2.button("✏️ Editar"):
        st.session_state["acao"] = "Editar"

    if col3.button("🗑️ Excluir"):
        st.session_state["acao"] = "Excluir"

    acao = st.session_state.get("acao", "Adicionar")

    if acao == "Adicionar":
        codigo = st.text_input("Código")
        produto = st.text_input("Produto")
        categoria = st.selectbox("Categoria", categorias)
        estoque_min = st.number_input("Estoque mínimo",0)
        local = st.text_input("Localização")
        imagem = st.text_input("Imagem")

        if st.button("Salvar"):
            novo = pd.DataFrame([{
                "codigo":codigo,
                "produto":produto,
                "categoria":categoria,
                "estoque_minimo":estoque_min,
                "localizacao":local,
                "imagem":imagem
            }])

            df_produtos = pd.concat([df_produtos, novo])
            df_produtos.to_excel("produtos.xlsx", index=False)
            st.success("Adicionado")

    elif acao == "Editar":
        prod = st.selectbox("Produto", df_produtos["produto"])
        dados = df_produtos[df_produtos["produto"]==prod].iloc[0]

        codigo = st.text_input("Código", dados["codigo"])
        categoria = st.selectbox("Categoria", categorias)
        estoque_min = st.number_input("Estoque mínimo", int(dados["estoque_minimo"]))
        local = st.text_input("Localização", dados["localizacao"])
        imagem = st.text_input("Imagem", dados["imagem"])

        if st.button("Salvar Alteração"):
            df_produtos.loc[df_produtos["produto"]==prod, "codigo"] = codigo
            df_produtos.loc[df_produtos["produto"]==prod, "categoria"] = categoria
            df_produtos.loc[df_produtos["produto"]==prod, "estoque_minimo"] = estoque_min
            df_produtos.loc[df_produtos["produto"]==prod, "localizacao"] = local
            df_produtos.loc[df_produtos["produto"]==prod, "imagem"] = imagem

            df_produtos.to_excel("produtos.xlsx", index=False)
            st.success("Atualizado")

    elif acao == "Excluir":
        prod = st.selectbox("Produto", df_produtos["produto"])

        if st.button("Excluir"):
            df_produtos = df_produtos[df_produtos["produto"]!=prod]
            df_produtos.to_excel("produtos.xlsx", index=False)
            st.success("Excluído")
