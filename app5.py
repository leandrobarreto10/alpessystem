import streamlit as st
import pandas as pd
from datetime import datetime
import os
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, Image, TableStyle
from reportlab.lib import colors

st.set_page_config(page_title="Sistema de Almoxarifado", layout="wide")

PASTA_IMAGENS = r"C:\Users\Dell\Desktop\Sistema Estoque\Imagens Produtos"

# =========================
# CARREGAR DADOS
# =========================
def carregar_dados():
    try:
        produtos = pd.read_excel("produtos.xlsx")
    except:
        produtos = pd.DataFrame(columns=[
            "codigo","produto","categoria",
            "estoque_minimo","localizacao","imagem"
        ])

    try:
        mov = pd.read_excel("movimentacoes.xlsx")
    except:
        mov = pd.DataFrame(columns=[
            "produto","tipo","quantidade","data"
        ])

    return produtos, mov

df_produtos, df_mov = carregar_dados()

# =========================
# SESSION STATE FIX
# =========================
if "df_produtos" not in st.session_state:
    st.session_state.df_produtos = df_produtos

df_produtos = st.session_state.df_produtos

# =========================
# MENU
# =========================
st.sidebar.title("MENU")

if "menu" not in st.session_state:
    st.session_state.menu = "ESTOQUE"

if st.sidebar.button("📦 ESTOQUE"):
    st.session_state.menu = "ESTOQUE"

if st.sidebar.button("🛒 COMPRAS"):
    st.session_state.menu = "COMPRAS"

if st.sidebar.button("🔄 MOVIMENTAÇÃO"):
    st.session_state.menu = "MOVIMENTAÇÃO"

if st.sidebar.button("📋 CADASTRO"):
    st.session_state.menu = "CADASTRO"

menu = st.session_state.menu

# =========================
# TÍTULO (CORRIGIDO DEFINITIVO)
# =========================
st.title(menu)

# =========================
# ESTOQUE
# =========================
def calcular_estoque():
    if df_mov.empty:
        return pd.Series(dtype=float)

    ent = df_mov[df_mov["tipo"] == "Entrada"].groupby("produto")["quantidade"].sum()
    sai = df_mov[df_mov["tipo"] == "Saída"].groupby("produto")["quantidade"].sum()

    return ent.subtract(sai, fill_value=0)

df_produtos["estoque_atual"] = df_produtos["produto"].map(calcular_estoque()).fillna(0)

df_produtos["situacao"] = df_produtos.apply(
    lambda x: "🔴 BAIXO" if x["estoque_atual"] <= x["estoque_minimo"] else "🟢 OK",
    axis=1
)

# =========================
# ESTOQUE + HISTÓRICO
# =========================
if menu == "ESTOQUE":

    cols = st.columns([1,2,2,1,1,2,2,2])
    cols[0].write("CÓDIGO")
    cols[1].write("PRODUTO")
    cols[2].write("CATEGORIA")
    cols[3].write("ESTOQUE")
    cols[4].write("MÍNIMO")
    cols[5].write("LOCAL")
    cols[6].write("STATUS")
    cols[7].write("IMAGEM")

    for i, row in df_produtos.iterrows():
        c = st.columns([1,2,2,1,1,2,2,2])

        c[0].write(row["codigo"])

        if c[1].button(row["produto"], key=i):
            st.session_state.produto_sel = row["produto"]

        c[2].write(row["categoria"])
        c[3].write(int(row["estoque_atual"]))
        c[4].write(row["estoque_minimo"])
        c[5].write(row["localizacao"])
        c[6].write(row["situacao"])

        img = os.path.join(PASTA_IMAGENS, str(row["imagem"]))
        if os.path.exists(img):
            c[7].image(img, width=100)

    # HISTÓRICO
    if "produto_sel" in st.session_state:
        st.divider()
        st.subheader(f"HISTÓRICO - {st.session_state.produto_sel}")

        hist = df_mov[df_mov["produto"] == st.session_state.produto_sel]

        if not hist.empty:
            hist["data"] = pd.to_datetime(hist["data"]).dt.strftime("%d/%m/%Y %H:%M")
            st.dataframe(hist)
        else:
            st.info("SEM MOVIMENTAÇÕES")

        if st.button("FECHAR"):
            del st.session_state.produto_sel
            st.rerun()

# =========================
# COMPRAS (FUNCIONANDO)
# =========================
elif menu == "COMPRAS":

    df = df_produtos.copy()
    df["necessita"] = (df["estoque_minimo"] + 5) - df["estoque_atual"]
    df = df[df["necessita"] > 0]

    if st.button("GERAR PDF"):

        caminho = os.path.join(os.path.expanduser("~"), "Downloads", "compras.pdf")
        pdf = SimpleDocTemplate(caminho, pagesize=letter)

        data = [["COD","PROD","ATUAL","MIN","NECESSITA","IMG"]]

        for _, r in df.iterrows():

            img_path = os.path.join(PASTA_IMAGENS, str(r.get("imagem","")))

            img = Image(img_path, 50, 50) if os.path.exists(img_path) else "SEM"

            data.append([
                str(r["codigo"]),
                str(r["produto"]),
                str(int(r["estoque_atual"])),
                str(int(r["estoque_minimo"])),
                str(int(r["necessita"])),
                img
            ])

        tabela = Table(data)

        tabela.setStyle(TableStyle([
            ("GRID",(0,0),(-1,-1),0.5,colors.black),
            ("BACKGROUND",(0,0),(-1,0),colors.grey),
            ("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("ALIGN",(0,0),(-1,-1),"CENTER")
        ]))

        pdf.build([tabela])
        st.success("PDF GERADO")

# =========================
# MOVIMENTAÇÃO (SEM BUG)
# =========================
elif menu == "MOVIMENTAÇÃO":

    if "lista" not in st.session_state:
        st.session_state.lista = []

    tipo = st.selectbox("TIPO", ["Entrada","Saída"], key="t")
    produto = st.selectbox("PRODUTO", df_produtos["produto"], key="p")
    qtd = st.number_input("QUANTIDADE", 1)

    if st.button("ADICIONAR"):
        st.session_state.lista.append({
            "produto": produto,
            "tipo": tipo,
            "quantidade": qtd
        })

    if st.button("SALVAR"):

        for i in st.session_state.lista:
            novo = pd.DataFrame([{
                "produto": i["produto"],
                "tipo": i["tipo"],
                "quantidade": i["quantidade"],
                "data": datetime.now()
            }])
            df_mov = pd.concat([df_mov, novo])

        df_mov.to_excel("movimentacoes.xlsx", index=False)
        st.session_state.lista = []
        st.success("SALVO")

# =========================
# CADASTRO (FUNCIONANDO)
# =========================
elif menu == "CADASTRO":

    cat = ["MANUTENÇÃO","ELÉTRICA","HIDRÁULICA","LIMPEZA","COPA"]

    op = st.radio("AÇÃO", ["Adicionar","Editar","Excluir"])

    if op == "Adicionar":

        c = st.text_input("CÓDIGO")
        p = st.text_input("PRODUTO")
        catg = st.selectbox("CATEGORIA", cat)
        mino = st.number_input("MÍNIMO",0)
        loc = st.text_input("LOCAL")
        img = st.text_input("IMAGEM")

        if st.button("SALVAR"):

            novo = pd.DataFrame([{
                "codigo":c,
                "produto":p,
                "categoria":catg,
                "estoque_minimo":mino,
                "localizacao":loc,
                "imagem":img
            }])

            st.session_state.df_produtos = pd.concat([df_produtos, novo])
            st.session_state.df_produtos.to_excel("produtos.xlsx", index=False)
            st.success("ADICIONADO")

    elif op == "Editar":

        p = st.selectbox("PRODUTO", df_produtos["produto"])
        row = df_produtos[df_produtos["produto"]==p].iloc[0]

        c = st.text_input("CÓDIGO", row["codigo"])
        catg = st.selectbox("CATEGORIA", cat)
        mino = st.number_input("MÍNIMO", int(row["estoque_minimo"]))
        loc = st.text_input("LOCAL", row["localizacao"])
        img = st.text_input("IMAGEM", row["imagem"])

        if st.button("SALVAR"):

            df_produtos.loc[df_produtos["produto"]==p, "codigo"] = c
            df_produtos.loc[df_produtos["produto"]==p, "categoria"] = catg
            df_produtos.loc[df_produtos["produto"]==p, "estoque_minimo"] = mino
            df_produtos.loc[df_produtos["produto"]==p, "localizacao"] = loc
            df_produtos.loc[df_produtos["produto"]==p, "imagem"] = img

            df_produtos.to_excel("produtos.xlsx", index=False)
            st.success("ATUALIZADO")
            st.rerun()

    elif op == "Excluir":

        p = st.selectbox("PRODUTO", df_produtos["produto"])

        if st.button("EXCLUIR"):

            df_produtos = df_produtos[df_produtos["produto"]!=p]
            df_produtos.to_excel("produtos.xlsx", index=False)
            st.success("EXCLUÍDO")
            st.rerun()
