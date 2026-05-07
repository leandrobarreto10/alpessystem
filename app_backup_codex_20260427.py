import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import io
import json
import hashlib
import shutil
import zipfile
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image as RLImage, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch

st.set_page_config(page_title="Sistema de Almoxarifado", layout="wide")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PASTA_IMAGENS = os.path.join(BASE_DIR, "Imagens Produtos")
PRODUTOS_XLSX = os.path.join(BASE_DIR, "produtos.xlsx")
MOVIMENTACOES_XLSX = os.path.join(BASE_DIR, "movimentacoes.xlsx")
USUARIOS_JSON = os.path.join(BASE_DIR, "usuarios.json")
CONFIG_JSON = os.path.join(BASE_DIR, "configuracoes.json")
CATEGORIAS_JSON = os.path.join(BASE_DIR, "categorias.json")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")
HOME_IMAGE = os.path.join(BASE_DIR, "Desktop 1.jpg")


# =========================
# FUNCOES DE ARQUIVO
# =========================
def carregar_json(caminho, padrao):
    if os.path.exists(caminho):
        try:
            with open(caminho, "r", encoding="utf-8") as arquivo:
                return json.load(arquivo)
        except Exception:
            return padrao
    return padrao


def salvar_json(caminho, dados):
    with open(caminho, "w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, indent=4)


def hash_senha(senha):
    return hashlib.sha256(str(senha).encode("utf-8")).hexdigest()


def garantir_usuario_admin():
    usuarios = carregar_json(USUARIOS_JSON, [])
    if not usuarios:
        usuarios = [{
            "nome": "admin",
            "email": "admin",
            "senha": hash_senha("123"),
            "nivel": "Administrador",
            "criado_em": datetime.now().strftime("%d/%m/%Y %H:%M")
        }]
        salvar_json(USUARIOS_JSON, usuarios)
    return usuarios


def configuracao_padrao():
    return {
        "empresa": "",
        "email": "",
        "telefone": "",
        "endereco": "",
        "logo": "",
        "estoque_minimo_padrao": 1,
        "alerta_estoque": True,
        "permitir_negativo": False,
        "tema": "dark",
        "cor_principal": "#6157ff",
        "fonte": "Inter",
        "ultimo_backup": "Nunca"
    }


def categorias_padrao():
    return [
        {"nome": "MANUTENÇÃO", "cor": "#facc15"},
        {"nome": "ELÉTRICA", "cor": "#fb923c"},
        {"nome": "HIDRÁULICA", "cor": "#38bdf8"},
        {"nome": "LIMPEZA", "cor": "#22c55e"},
        {"nome": "COPA", "cor": "#a78bfa"},
        {"nome": "JARDINAGEM", "cor": "#4ade80"}
    ]


usuarios = garantir_usuario_admin()
config = carregar_json(CONFIG_JSON, configuracao_padrao())
categorias_config = carregar_json(CATEGORIAS_JSON, categorias_padrao())


# =========================
# ESTILO VISUAL
# =========================
cor_principal = config.get("cor_principal", "#6157ff")
fonte = config.get("fonte", "Inter")
tema = config.get("tema", "dark")

if tema == "dark":
    fundo_app = "#0f172a"
    fundo_card = "#111827"
    borda_card = "#273449"
    texto_app = "#e5e7eb"
    texto_suave = "#94a3b8"
else:
    fundo_app = "#f8fafc"
    fundo_card = "#ffffff"
    borda_card = "#e2e8f0"
    texto_app = "#111827"
    texto_suave = "#64748b"

st.markdown(f"""
<style>
    html, body, [class*="css"] {{
        font-family: '{fonte}', Arial, sans-serif;
    }}
    .stApp {{
        background: {fundo_app};
        color: {texto_app};
    }}
    [data-testid="stSidebar"] {{
        background: #0b1220;
    }}
    [data-testid="stSidebar"] * {{
        color: #e5e7eb;
    }}
    [data-testid="stSidebar"] div[role="radiogroup"] label {{
        background: #1f2937;
        border: 1px solid #334155;
        border-radius: 8px;
        padding: 10px 12px;
        margin: 6px 0;
        transition: .15s ease;
    }}
    [data-testid="stSidebar"] div[role="radiogroup"] label:hover {{
        background: #334155;
    }}
    [data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) {{
        background: linear-gradient(135deg, {cor_principal}, #2563eb);
        border-color: {cor_principal};
        box-shadow: 0 10px 26px rgba(37, 99, 235, .22);
    }}
    [data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) * {{
        color: white !important;
        font-weight: 700;
    }}
    .saas-card {{
        background: {fundo_card};
        border: 1px solid {borda_card};
        border-radius: 14px;
        padding: 18px;
        box-shadow: 0 14px 35px rgba(0, 0, 0, .18);
    }}
    .metric-card {{
        background: {fundo_card};
        border: 1px solid {borda_card};
        border-radius: 14px;
        padding: 18px;
        box-shadow: 0 12px 28px rgba(0, 0, 0, .16);
        min-height: 112px;
    }}
    .metric-label {{
        color: {texto_suave};
        font-size: 13px;
        margin-bottom: 8px;
    }}
    .metric-value {{
        color: {texto_app};
        font-size: 30px;
        font-weight: 800;
    }}
    .status-pill {{
        border-radius: 999px;
        padding: 8px 12px;
        background: #12201a;
        border: 1px solid #1f7a45;
        color: #4ade80;
        font-weight: 700;
        display: inline-block;
        width: 100%;
        text-align: center;
    }}
    .home-img {{
        width: 100%;
        height: calc(100vh - 96px);
        object-fit: contain;
        object-position: center;
        border: 0;
        display: block;
    }}
    div[data-testid="stMetricValue"] {{
        color: {texto_app};
    }}
    button[kind="primary"], .stDownloadButton button {{
        border-radius: 8px !important;
    }}
</style>
""", unsafe_allow_html=True)


# =========================
# LOGIN
# =========================
if "autenticado" not in st.session_state:
    st.session_state["autenticado"] = False

if not st.session_state["autenticado"]:
    st.markdown("<br><br>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1.1, 1])
    with c2:
        st.markdown("<div class='saas-card'>", unsafe_allow_html=True)
        st.title("Login")
        usuario_login = st.text_input("Usuário ou email")
        mostrar_senha = st.checkbox("Mostrar senha")
        senha_login = st.text_input("Senha", type="default" if mostrar_senha else "password")
        if st.button("Entrar", use_container_width=True):
            usuarios = garantir_usuario_admin()
            usuario_encontrado = next(
                (
                    u for u in usuarios
                    if str(u.get("nome", "")).lower() == usuario_login.lower()
                    or str(u.get("email", "")).lower() == usuario_login.lower()
                ),
                None
            )
            if usuario_encontrado and usuario_encontrado.get("senha") == hash_senha(senha_login):
                st.session_state["autenticado"] = True
                st.session_state["usuario_logado"] = {
                    "nome": usuario_encontrado.get("nome", ""),
                    "email": usuario_encontrado.get("email", ""),
                    "nivel": usuario_encontrado.get("nivel", "")
                }
                st.rerun()
            else:
                st.error("Login inválido. Verifique usuário/email e senha.")
        st.markdown("</div>", unsafe_allow_html=True)
    st.stop()


# =========================
# CARREGAR DADOS
# =========================
try:
    df_produtos = pd.read_excel(PRODUTOS_XLSX)
except Exception:
    df_produtos = pd.DataFrame(columns=[
        "codigo", "produto", "categoria",
        "estoque_minimo", "localizacao", "imagem"
    ])

try:
    df_mov = pd.read_excel(MOVIMENTACOES_XLSX)
except Exception:
    df_mov = pd.DataFrame(columns=[
        "produto", "tipo", "quantidade", "data"
    ])

for col in ["codigo", "produto", "categoria", "estoque_minimo", "localizacao", "imagem"]:
    if col not in df_produtos.columns:
        df_produtos[col] = ""

for col in ["produto", "tipo", "quantidade", "data"]:
    if col not in df_mov.columns:
        df_mov[col] = ""

df_produtos["estoque_minimo"] = pd.to_numeric(df_produtos["estoque_minimo"], errors="coerce").fillna(0)
df_mov["quantidade"] = pd.to_numeric(df_mov["quantidade"], errors="coerce").fillna(0)


# =========================
# FUNCOES DE APOIO
# =========================
def calcular_estoque():
    if df_mov.empty:
        return pd.Series(dtype=float)

    ent = df_mov[df_mov["tipo"] == "Entrada"].groupby("produto")["quantidade"].sum()
    sai = df_mov[df_mov["tipo"] == "Saída"].groupby("produto")["quantidade"].sum()

    return ent.subtract(sai, fill_value=0)


df_produtos["estoque_atual"] = df_produtos["produto"].map(calcular_estoque()).fillna(0)

df_produtos["situacao"] = df_produtos.apply(
    lambda x: "🔴 ESTOQUE BAIXO" if x["estoque_atual"] <= x["estoque_minimo"] else "🟢 OK",
    axis=1
)


def cor_categoria(cat):
    cat_upper = str(cat).upper()
    for item in categorias_config:
        if item.get("nome", "").upper() == cat_upper:
            return item.get("cor", "white")
    if cat_upper in ["HIDRAULICA", "HIDRÁULICA"]:
        return "#3498db"
    if cat_upper in ["ELETRICA", "ELÉTRICA"]:
        return "#e67e22"
    if cat_upper in ["MANUTENCAO", "MANUTENÇÃO"]:
        return "#f1c40f"
    if cat_upper == "JARDINAGEM":
        return "#2ecc71"
    return "white"


def filtrar_movimentacoes(df_base, periodo="30 dias", tipo="Todos", categoria="Todas", produto="Todos", data_ini=None, data_fim=None):
    df = df_base.copy()
    if df.empty:
        return df

    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df = df.dropna(subset=["data"])
    hoje = datetime.now()

    if periodo == "7 dias":
        df = df[df["data"] >= hoje - timedelta(days=7)]
    elif periodo == "30 dias":
        df = df[df["data"] >= hoje - timedelta(days=30)]
    elif periodo == "Personalizado" and data_ini and data_fim:
        ini = pd.to_datetime(data_ini)
        fim = pd.to_datetime(data_fim) + timedelta(days=1)
        df = df[(df["data"] >= ini) & (df["data"] < fim)]

    if tipo != "Todos":
        df = df[df["tipo"] == tipo]

    if produto != "Todos":
        df = df[df["produto"] == produto]

    if categoria != "Todas" and not df_produtos.empty:
        produtos_categoria = df_produtos[df_produtos["categoria"] == categoria]["produto"].tolist()
        df = df[df["produto"].isin(produtos_categoria)]

    return df


def gerar_pdf_relatorios(df_rel, df_criticos, metricas):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    elementos = [
        Paragraph("Relatórios de Estoque", styles["Title"]),
        Spacer(1, 12)
    ]

    tabela_metricas = Table([
        ["Total de produtos", "Entradas", "Saídas", "Itens críticos"],
        [
            str(metricas["total_produtos"]),
            str(metricas["entradas"]),
            str(metricas["saidas"]),
            str(metricas["criticos"])
        ]
    ], colWidths=[130, 100, 100, 100])
    tabela_metricas.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#eef2ff")),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("BOX", (0, 0), (-1, -1), 0.4, colors.HexColor("#94a3b8")),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#cbd5e1")),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
    ]))
    elementos.extend([tabela_metricas, Spacer(1, 16)])

    mais_mov = df_rel.groupby("produto")["quantidade"].sum().reset_index().sort_values("quantidade", ascending=False).head(10) if not df_rel.empty else pd.DataFrame(columns=["produto", "quantidade"])
    historico = df_rel.sort_values("data", ascending=False).head(15) if not df_rel.empty else pd.DataFrame(columns=["produto", "tipo", "quantidade", "data"])

    for titulo, dados in [
        ("Produtos mais movimentados", mais_mov),
        ("Produtos críticos", df_criticos[["produto", "categoria", "estoque_atual", "estoque_minimo"]].head(15) if not df_criticos.empty else pd.DataFrame(columns=["produto", "categoria", "estoque_atual", "estoque_minimo"])),
        ("Histórico", historico[["produto", "tipo", "quantidade", "data"]] if not historico.empty else pd.DataFrame(columns=["produto", "tipo", "quantidade", "data"]))
    ]:
        elementos.append(Paragraph(titulo, styles["Heading2"]))
        linhas = [list(dados.columns)]
        for _, row in dados.iterrows():
            linhas.append([str(v) for v in row.tolist()])
        tabela = Table(linhas, repeatRows=1)
        tabela.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2563eb")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#94a3b8")),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        elementos.extend([tabela, Spacer(1, 14)])

    doc.build(elementos)
    buffer.seek(0)
    return buffer


def gerar_excel_relatorios(df_rel, df_criticos, metricas):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame([metricas]).to_excel(writer, sheet_name="Resumo", index=False)
        df_rel.to_excel(writer, sheet_name="Historico", index=False)
        df_criticos.to_excel(writer, sheet_name="Produtos Criticos", index=False)
        if not df_rel.empty:
            df_rel.groupby(["produto", "tipo"])["quantidade"].sum().reset_index().to_excel(writer, sheet_name="Mais Movimentados", index=False)
    buffer.seek(0)
    return buffer


def gerar_backup():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    nome = f"backup_estoque_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    pasta_temp = os.path.join(BACKUP_DIR, nome)
    os.makedirs(pasta_temp, exist_ok=True)
    for caminho in [PRODUTOS_XLSX, MOVIMENTACOES_XLSX, USUARIOS_JSON, CONFIG_JSON, CATEGORIAS_JSON]:
        if os.path.exists(caminho):
            shutil.copy2(caminho, os.path.join(pasta_temp, os.path.basename(caminho)))
    zip_path = shutil.make_archive(pasta_temp, "zip", pasta_temp)
    shutil.rmtree(pasta_temp, ignore_errors=True)
    config["ultimo_backup"] = datetime.now().strftime("%d/%m/%Y %H:%M")
    salvar_json(CONFIG_JSON, config)
    return zip_path


# =========================
# MENU
# =========================
st.sidebar.title("MENU")
usuario_logado = st.session_state.get("usuario_logado", {})
st.sidebar.caption(f"Usuário logado: {usuario_logado.get('nome', '')} | {usuario_logado.get('nivel', '')}")

if "menu" not in st.session_state:
    st.session_state["menu"] = "HOME"

menu_opcoes = [
    "HOME",
    "ESTOQUE",
    "COMPRAS",
    "MOVIMENTAÇÃO",
    "CADASTRO DE PRODUTOS",
    "RELATÓRIOS",
    "CONFIGURAÇÕES"
]

menu = st.sidebar.radio(
    "Navegação",
    menu_opcoes,
    index=menu_opcoes.index(st.session_state["menu"]) if st.session_state["menu"] in menu_opcoes else 0,
    label_visibility="collapsed"
)
st.session_state["menu"] = menu

st.sidebar.divider()
total_criticos_sidebar = int((df_produtos["estoque_atual"] <= df_produtos["estoque_minimo"]).sum()) if not df_produtos.empty else 0
st.sidebar.markdown("<span class='status-pill'>Sistema online</span>", unsafe_allow_html=True)
st.sidebar.caption(f"Último backup: {config.get('ultimo_backup', 'Nunca')}")
st.sidebar.caption(f"Itens críticos: {total_criticos_sidebar}")

if st.sidebar.button("Sair", use_container_width=True):
    st.session_state["autenticado"] = False
    st.session_state.pop("usuario_logado", None)
    st.rerun()


# =========================
# HOME
# =========================
if menu == "HOME":
    if os.path.exists(HOME_IMAGE):
        st.markdown(
            f"<img src='data:image/jpeg;base64,{__import__('base64').b64encode(open(HOME_IMAGE, 'rb').read()).decode()}' class='home-img'>",
            unsafe_allow_html=True
        )
    else:
        st.warning(f"Imagem não encontrada: {HOME_IMAGE}. Verifique se o caminho está correto e a imagem existe.")


# =========================
# ABA ESTOQUE
# =========================
elif menu == "ESTOQUE":
    st.title("📦 Estoque Geral")

    total_cadastrados = len(df_produtos)
    total_ok = len(df_produtos[df_produtos["situacao"] == "🟢 OK"])
    total_baixo = len(df_produtos[df_produtos["situacao"] == "🔴 ESTOQUE BAIXO"])

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"<div class='metric-card'><div class='metric-label'>Total de Produtos Cadastrados</div><div class='metric-value'>{total_cadastrados}</div><div class='metric-label'>Todos os itens cadastrados</div></div>", unsafe_allow_html=True)
    with c2:
        st.markdown(f"<div class='metric-card'><div class='metric-label'>Estoque OK</div><div class='metric-value' style='color:#22c55e'>{total_ok}</div><div class='metric-label'>Acima do estoque mínimo</div></div>", unsafe_allow_html=True)
    with c3:
        st.markdown(f"<div class='metric-card'><div class='metric-label'>Estoque Baixo</div><div class='metric-value' style='color:#ef4444'>{total_baixo}</div><div class='metric-label'>Produtos abaixo do mínimo</div></div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    busca = st.text_input("Busca", placeholder="Buscar por código, produto ou categoria", label_visibility="collapsed")

    with st.expander("Filtros Avançados"):
        f_col1, f_col2, f_col3 = st.columns(3)

        if "limpar_filtros" in st.session_state and st.session_state["limpar_filtros"]:
            st.session_state["f_cat"] = "Todas"
            st.session_state["f_sit"] = "Todas"
            st.session_state["f_data"] = "Todas"
            st.session_state["limpar_filtros"] = False

        categorias_lista = ["Todas"] + list(df_produtos["categoria"].dropna().unique())
        f_cat = f_col1.selectbox("Categoria", categorias_lista, key="f_cat")
        f_sit = f_col2.selectbox("Situação", ["Todas", "Estoque OK", "Estoque Baixo"], key="f_sit")
        f_data = f_col3.selectbox("Data de Movimentação", ["Todas", "Últimos 7 dias", "Últimos 30 dias", "Personalizado"], key="f_data")

        f_data_ini, f_data_fim = None, None
        if f_data == "Personalizado":
            c_d1, c_d2 = st.columns(2)
            f_data_ini = c_d1.date_input("Data Início")
            f_data_fim = c_d2.date_input("Data Fim")

        c_ap, c_lim, _ = st.columns([2, 2, 6])
        if c_ap.button("Aplicar filtro"):
            st.session_state["aplicar_filtros"] = True
        if c_lim.button("Limpar tudo"):
            st.session_state["limpar_filtros"] = True
            st.session_state["aplicar_filtros"] = False
            st.rerun()

    df_filtrado = df_produtos.copy()

    if busca:
        termo = str(busca).lower()
        df_filtrado = df_filtrado[
            df_filtrado["codigo"].astype(str).str.lower().str.contains(termo) |
            df_filtrado["produto"].astype(str).str.lower().str.contains(termo) |
            df_filtrado["categoria"].astype(str).str.lower().str.contains(termo)
        ]

    if f_cat != "Todas":
        df_filtrado = df_filtrado[df_filtrado["categoria"] == f_cat]

    if f_sit == "Estoque OK":
        df_filtrado = df_filtrado[df_filtrado["situacao"] == "🟢 OK"]
    elif f_sit == "Estoque Baixo":
        df_filtrado = df_filtrado[df_filtrado["situacao"] == "🔴 ESTOQUE BAIXO"]

    if f_data != "Todas" and not df_mov.empty:
        df_mov_temp = df_mov.copy()
        df_mov_temp["data"] = pd.to_datetime(df_mov_temp["data"], errors="coerce")
        hoje = datetime.now()
        prods_com_mov = []

        if f_data == "Últimos 7 dias":
            limite = hoje - timedelta(days=7)
            prods_com_mov = df_mov_temp[df_mov_temp["data"] >= limite]["produto"].unique()
        elif f_data == "Últimos 30 dias":
            limite = hoje - timedelta(days=30)
            prods_com_mov = df_mov_temp[df_mov_temp["data"] >= limite]["produto"].unique()
        elif f_data == "Personalizado" and f_data_ini and f_data_fim:
            ini = pd.to_datetime(f_data_ini)
            fim = pd.to_datetime(f_data_fim) + timedelta(days=1)
            prods_com_mov = df_mov_temp[(df_mov_temp["data"] >= ini) & (df_mov_temp["data"] < fim)]["produto"].unique()

        df_filtrado = df_filtrado[df_filtrado["produto"].isin(prods_com_mov)]

    st.markdown("<br>", unsafe_allow_html=True)

    headers = st.columns([1, 2, 2, 1, 1, 2, 2, 3])
    headers[0].write("Código")
    headers[1].write("Produto")
    headers[2].write("Categoria")
    headers[3].write("Estoque Atual")
    headers[4].write("Estoque Mínimo")
    headers[5].write("Localização")
    headers[6].write("Situação")
    headers[7].write("Imagem")

    for i, row in df_filtrado.iterrows():
        col = st.columns([1, 2, 2, 1, 1, 2, 2, 3])

        col[0].write(row["codigo"])

        if col[1].button(row["produto"], key=f"prod_{i}"):
            st.session_state["produto"] = row["produto"]

        col[2].markdown(f"<span style='color:{cor_categoria(row['categoria'])}'><b>{row['categoria']}</b></span>", unsafe_allow_html=True)
        col[3].write(int(row["estoque_atual"]))
        col[4].markdown(f"<span style='color:#facc15'><b>{row['estoque_minimo']}</b></span>", unsafe_allow_html=True)
        col[5].write(row["localizacao"])
        col[6].write(row["situacao"])

        img = os.path.join(PASTA_IMAGENS, str(row["imagem"]))
        if os.path.exists(img):
            col[7].image(img, width=120)

    if "produto" in st.session_state:
        produto = st.session_state["produto"]
        st.divider()
        st.subheader(f"📊 Histórico - {produto}")

        hist = df_mov[df_mov["produto"] == produto].copy()
        if not hist.empty:
            hist["data"] = pd.to_datetime(hist["data"]).dt.strftime("%d/%m/%Y %H:%M")
            st.dataframe(hist, use_container_width=True)
        else:
            st.info("Sem movimentações")

        if st.button("Fechar Histórico"):
            del st.session_state["produto"]
            st.rerun()


# =========================
# COMPRAS
# =========================
elif menu == "COMPRAS":
    st.title("COMPRAS")

    df = df_produtos.copy()
    df["necessita"] = (df["estoque_minimo"] + 5) - df["estoque_atual"]
    df = df[df["necessita"] > 0]

    col1, col2 = st.columns(2)

    with col1:
        if st.button("📄 Gerar PDF"):
            pasta_downloads = os.path.join(os.path.expanduser("~"), "Downloads")
            caminho_pdf = os.path.join(pasta_downloads, "compras_relatorio.pdf")

            data_pdf = [["Código", "Produto", "Atual", "Mínimo", "Necessita", "Imagem"]]

            for _, r in df.iterrows():
                img_path = os.path.join(PASTA_IMAGENS, str(r["imagem"]))
                img_rl = ""
                if os.path.exists(img_path):
                    try:
                        img_rl = RLImage(img_path, width=1 * inch, height=1 * inch)
                    except Exception:
                        pass

                data_pdf.append([
                    r["codigo"],
                    r["produto"],
                    str(int(r["estoque_atual"])),
                    str(int(r["estoque_minimo"])),
                    str(int(r["necessita"])),
                    img_rl
                ])

            pdf = SimpleDocTemplate(caminho_pdf, pagesize=letter)
            tabela = Table(data_pdf, colWidths=[60, 150, 50, 60, 60, 100])

            estilo = TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2c3e50")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#ecf0f1")),
                ("TEXTCOLOR", (3, 1), (3, -1), colors.orange),
                ("TEXTCOLOR", (4, 1), (4, -1), colors.red),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.black),
                ("BOX", (0, 0), (-1, -1), 0.25, colors.black),
            ])
            tabela.setStyle(estilo)

            elementos = [tabela]
            pdf.build(elementos)

            st.success(f"PDF profissional salvo com sucesso em: {caminho_pdf}")

    with col2:
        if st.button("📂 Selecionar Categoria"):
            st.session_state["mostrar_categoria"] = True

    if "mostrar_categoria" not in st.session_state:
        st.session_state["mostrar_categoria"] = False

    if "categoria_sel" not in st.session_state:
        st.session_state["categoria_sel"] = "GERAL"

    if st.session_state["mostrar_categoria"]:
        categorias = ["GERAL"] + list(df_produtos["categoria"].dropna().unique())
        cols = st.columns(max(len(categorias), 1))

        for i, cat in enumerate(categorias):
            if cols[i].button(cat):
                st.session_state["categoria_sel"] = cat

    if st.session_state["categoria_sel"] != "GERAL":
        df = df[df["categoria"] == st.session_state["categoria_sel"]]

    st.markdown("<br><br>", unsafe_allow_html=True)

    headers = st.columns([1, 2, 1, 1, 1, 3])
    headers[0].write("Código")
    headers[1].write("Produto")
    headers[2].write("Estoque Atual")
    headers[3].write("Estoque Mínimo")
    headers[4].write("Necessita")
    headers[5].write("Imagem")

    for _, row in df.iterrows():
        col = st.columns([1, 2, 1, 1, 1, 3])

        col[0].write(row["codigo"])
        col[1].write(row["produto"])
        col[2].write(int(row["estoque_atual"]))
        col[3].markdown(f"<span style='color:#facc15'><b>{row['estoque_minimo']}</b></span>", unsafe_allow_html=True)
        col[4].markdown(f"<span style='color:#ef4444'><b>{int(row['necessita'])}</b></span>", unsafe_allow_html=True)

        img = os.path.join(PASTA_IMAGENS, str(row["imagem"]))
        if os.path.exists(img):
            col[5].image(img, width=120)


# =========================
# MOVIMENTACAO
# =========================
elif menu == "MOVIMENTAÇÃO":
    st.title("MOVIMENTAÇÃO")

    if "lista_mov" not in st.session_state:
        st.session_state["lista_mov"] = []

    tipo = st.selectbox("Tipo de Movimentação", ["Entrada", "Saída"])
    produto = st.selectbox("Produto", df_produtos["produto"] if not df_produtos.empty else ["Nenhum produto cadastrado"])
    qtd = st.number_input("Quantidade", 1)

    col1, col2 = st.columns(2)

    if col1.button("➕ Adicionar"):
        if produto != "Nenhum produto cadastrado":
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
            df_mov = pd.concat([df_mov, nova], ignore_index=True)

        df_mov.to_excel(MOVIMENTACOES_XLSX, index=False)
        st.session_state["lista_mov"] = []
        st.success("Movimentações salvas")

    st.divider()
    for item in st.session_state["lista_mov"]:
        st.write(f"{item['produto']} | {item['tipo']} | {item['quantidade']}")


# =========================
# CADASTRO
# =========================
elif menu == "CADASTRO DE PRODUTOS":
    st.title("CADASTRO DE PRODUTOS")

    categorias = [item.get("nome", "") for item in categorias_config] or ["MANUTENÇÃO", "ELÉTRICA", "HIDRÁULICA", "LIMPEZA", "COPA", "JARDINAGEM"]

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
        estoque_min = st.number_input("Estoque mínimo", 0, value=int(config.get("estoque_minimo_padrao", 1)))
        local = st.text_input("Localização")
        imagem = st.text_input("Imagem")

        if st.button("Salvar"):
            novo = pd.DataFrame([{
                "codigo": codigo,
                "produto": produto,
                "categoria": categoria,
                "estoque_minimo": estoque_min,
                "localizacao": local,
                "imagem": imagem
            }])

            df_produtos = pd.concat([df_produtos, novo], ignore_index=True)
            df_produtos.to_excel(PRODUTOS_XLSX, index=False)
            st.success("Adicionado")

    elif acao == "Editar":
        if df_produtos.empty:
            st.info("Nenhum produto cadastrado.")
        else:
            prod = st.selectbox("Produto", df_produtos["produto"])
            dados = df_produtos[df_produtos["produto"] == prod].iloc[0]

            codigo = st.text_input("Código", dados["codigo"])
            categoria = st.selectbox("Categoria", categorias, index=categorias.index(dados["categoria"]) if dados["categoria"] in categorias else 0)
            estoque_min = st.number_input("Estoque mínimo", 0, value=int(dados["estoque_minimo"]))
            local = st.text_input("Localização", dados["localizacao"])
            imagem = st.text_input("Imagem", dados["imagem"])

            if st.button("Salvar Alteração"):
                df_produtos.loc[df_produtos["produto"] == prod, "codigo"] = codigo
                df_produtos.loc[df_produtos["produto"] == prod, "categoria"] = categoria
                df_produtos.loc[df_produtos["produto"] == prod, "estoque_minimo"] = estoque_min
                df_produtos.loc[df_produtos["produto"] == prod, "localizacao"] = local
                df_produtos.loc[df_produtos["produto"] == prod, "imagem"] = imagem

                df_produtos.to_excel(PRODUTOS_XLSX, index=False)
                st.success("Atualizado")

    elif acao == "Excluir":
        if df_produtos.empty:
            st.info("Nenhum produto cadastrado.")
        else:
            prod = st.selectbox("Produto", df_produtos["produto"])

            if st.button("Excluir"):
                df_produtos = df_produtos[df_produtos["produto"] != prod]
                df_produtos.to_excel(PRODUTOS_XLSX, index=False)
                st.success("Excluído")


# =========================
# RELATORIOS
# =========================
elif menu == "RELATÓRIOS":
    st.title("Relatórios de Estoque")

    top1, top2, top3, top4 = st.columns([1, 1, 1, 6])

    with st.expander("Filtros", expanded=True):
        f1, f2, f3, f4 = st.columns(4)
        periodo = f1.selectbox("Período", ["7 dias", "30 dias", "Personalizado"], index=1)
        tipo_rel = f2.selectbox("Tipo", ["Todos", "Entrada", "Saída"])
        categoria_rel = f3.selectbox("Categoria", ["Todas"] + list(df_produtos["categoria"].dropna().unique()))
        produto_rel = f4.selectbox("Produto", ["Todos"] + list(df_produtos["produto"].dropna().unique()))

        data_ini_rel, data_fim_rel = None, None
        if periodo == "Personalizado":
            d1, d2 = st.columns(2)
            data_ini_rel = d1.date_input("Data inicial")
            data_fim_rel = d2.date_input("Data final")

        filtrar = st.button("Filtrar")

    df_rel = filtrar_movimentacoes(df_mov, periodo, tipo_rel, categoria_rel, produto_rel, data_ini_rel, data_fim_rel)
    df_criticos = df_produtos[df_produtos["estoque_atual"] <= df_produtos["estoque_minimo"]].copy()
    metricas = {
        "total_produtos": int(len(df_produtos)),
        "entradas": int(df_rel[df_rel["tipo"] == "Entrada"]["quantidade"].sum()) if not df_rel.empty else 0,
        "saidas": int(df_rel[df_rel["tipo"] == "Saída"]["quantidade"].sum()) if not df_rel.empty else 0,
        "criticos": int(len(df_criticos))
    }

    with top1:
        st.download_button("PDF", data=gerar_pdf_relatorios(df_rel, df_criticos, metricas), file_name="relatorios_estoque.pdf", mime="application/pdf", use_container_width=True)
    with top2:
        st.download_button("Excel", data=gerar_excel_relatorios(df_rel, df_criticos, metricas), file_name="relatorios_estoque.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    with top3:
        if filtrar:
            st.success("Filtros aplicados")

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"<div class='metric-card'><div class='metric-label'>Total de produtos</div><div class='metric-value'>{metricas['total_produtos']}</div></div>", unsafe_allow_html=True)
    c2.markdown(f"<div class='metric-card'><div class='metric-label'>Entradas</div><div class='metric-value' style='color:#22c55e'>{metricas['entradas']}</div></div>", unsafe_allow_html=True)
    c3.markdown(f"<div class='metric-card'><div class='metric-label'>Saídas</div><div class='metric-value' style='color:#ef4444'>{metricas['saidas']}</div></div>", unsafe_allow_html=True)
    c4.markdown(f"<div class='metric-card'><div class='metric-label'>Itens críticos</div><div class='metric-value' style='color:#facc15'>{metricas['criticos']}</div></div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    g1, g2 = st.columns(2)
    with g1:
        st.subheader("Entradas x Saídas")
        barras = pd.DataFrame({
            "Tipo": ["Entrada", "Saída"],
            "Quantidade": [metricas["entradas"], metricas["saidas"]]
        }).set_index("Tipo")
        st.bar_chart(barras)

    with g2:
        st.subheader("Categorias")
        if not df_produtos.empty:
            categorias_pizza = df_produtos["categoria"].value_counts()
            try:
                import matplotlib.pyplot as plt
                fig, ax = plt.subplots()
                ax.pie(categorias_pizza.values, labels=categorias_pizza.index, autopct="%1.1f%%", startangle=90)
                ax.axis("equal")
                st.pyplot(fig)
            except Exception:
                st.dataframe(categorias_pizza.rename("Total"), use_container_width=True)
        else:
            st.info("Sem produtos cadastrados.")

    st.subheader("Produtos mais movimentados")
    if not df_rel.empty:
        mais_mov = df_rel.groupby("produto")["quantidade"].sum().reset_index().sort_values("quantidade", ascending=False)
        st.dataframe(mais_mov, use_container_width=True)
    else:
        st.info("Sem movimentações no período selecionado.")

    st.subheader("Produtos críticos")
    st.dataframe(df_criticos[["codigo", "produto", "categoria", "estoque_atual", "estoque_minimo", "localizacao"]], use_container_width=True)

    st.subheader("Histórico")
    hist_rel = df_rel.copy()
    if not hist_rel.empty:
        hist_rel["data"] = pd.to_datetime(hist_rel["data"], errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
    st.dataframe(hist_rel, use_container_width=True)


# =========================
# CONFIGURACOES
# =========================
elif menu == "CONFIGURAÇÕES":
    st.title("Configurações")

    st.markdown(
        f"""
        <div class='saas-card'>
            <b>Status do sistema</b><br>
            Sistema online &nbsp;|&nbsp; Último backup: {config.get('ultimo_backup', 'Nunca')} &nbsp;|&nbsp; Itens críticos: {total_criticos_sidebar}
        </div>
        """,
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)

    tab_geral, tab_usuarios, tab_estoque, tab_categorias, tab_aparencia, tab_backup = st.tabs([
        "GERAL", "USUÁRIOS", "ESTOQUE", "CATEGORIAS", "APARÊNCIA", "BACKUP"
    ])

    with tab_geral:
        with st.form("form_geral"):
            empresa = st.text_input("Nome empresa", config.get("empresa", ""))
            email = st.text_input("Email", config.get("email", ""))
            telefone = st.text_input("Telefone", config.get("telefone", ""))
            endereco = st.text_area("Endereço", config.get("endereco", ""))
            logo = st.file_uploader("Logo", type=["png", "jpg", "jpeg"])
            salvar_geral = st.form_submit_button("Salvar")
            if salvar_geral:
                config.update({
                    "empresa": empresa,
                    "email": email,
                    "telefone": telefone,
                    "endereco": endereco
                })
                if logo:
                    logo_path = os.path.join(BASE_DIR, f"logo_{logo.name}")
                    with open(logo_path, "wb") as arquivo:
                        arquivo.write(logo.getbuffer())
                    config["logo"] = logo_path
                salvar_json(CONFIG_JSON, config)
                st.success("Configurações gerais salvas.")

    with tab_usuarios:
        usuarios = carregar_json(USUARIOS_JSON, [])
        st.dataframe(pd.DataFrame([{k: v for k, v in u.items() if k != "senha"} for u in usuarios]), use_container_width=True)

        acao_usuario = st.radio("Ação", ["Criar", "Editar", "Excluir"], horizontal=True)
        if acao_usuario == "Criar":
            with st.form("criar_usuario"):
                nome = st.text_input("Nome")
                email_user = st.text_input("Email")
                nivel = st.selectbox("Nível", ["Administrador", "Usuário"])
                senha = st.text_input("Senha", type="password")
                if st.form_submit_button("Criar usuário"):
                    if nome and senha:
                        usuarios.append({
                            "nome": nome,
                            "email": email_user,
                            "nivel": nivel,
                            "senha": hash_senha(senha),
                            "criado_em": datetime.now().strftime("%d/%m/%Y %H:%M")
                        })
                        salvar_json(USUARIOS_JSON, usuarios)
                        st.success("Usuário criado.")
                        st.rerun()
                    else:
                        st.error("Informe nome e senha.")

        elif acao_usuario == "Editar":
            if usuarios:
                nomes = [u["nome"] for u in usuarios]
                selecionado = st.selectbox("Usuário", nomes)
                idx = nomes.index(selecionado)
                with st.form("editar_usuario"):
                    nome = st.text_input("Nome", usuarios[idx].get("nome", ""))
                    email_user = st.text_input("Email", usuarios[idx].get("email", ""))
                    nivel = st.selectbox("Nível", ["Administrador", "Usuário"], index=0 if usuarios[idx].get("nivel") == "Administrador" else 1)
                    nova_senha = st.text_input("Nova senha", type="password")
                    if st.form_submit_button("Salvar usuário"):
                        usuarios[idx]["nome"] = nome
                        usuarios[idx]["email"] = email_user
                        usuarios[idx]["nivel"] = nivel
                        if nova_senha:
                            usuarios[idx]["senha"] = hash_senha(nova_senha)
                        salvar_json(USUARIOS_JSON, usuarios)
                        st.success("Usuário atualizado.")
                        st.rerun()

        elif acao_usuario == "Excluir":
            if usuarios:
                nomes = [u["nome"] for u in usuarios]
                selecionado = st.selectbox("Usuário", nomes, key="excluir_usuario")
                if st.button("Excluir usuário"):
                    usuario = next(u for u in usuarios if u["nome"] == selecionado)
                    admins = [u for u in usuarios if u.get("nivel") == "Administrador"]
                    if usuario.get("nivel") == "Administrador" and len(admins) <= 1:
                        st.error("Não é permitido excluir o último administrador.")
                    else:
                        usuarios = [u for u in usuarios if u["nome"] != selecionado]
                        salvar_json(USUARIOS_JSON, usuarios)
                        st.success("Usuário excluído.")
                        st.rerun()

    with tab_estoque:
        with st.form("form_estoque"):
            estoque_minimo_padrao = st.number_input("Estoque mínimo padrão", 0, value=int(config.get("estoque_minimo_padrao", 1)))
            alerta_estoque = st.toggle("Alerta de estoque", value=bool(config.get("alerta_estoque", True)))
            permitir_negativo = st.toggle("Permitir negativo", value=bool(config.get("permitir_negativo", False)))
            if st.form_submit_button("Salvar estoque"):
                config["estoque_minimo_padrao"] = int(estoque_minimo_padrao)
                config["alerta_estoque"] = bool(alerta_estoque)
                config["permitir_negativo"] = bool(permitir_negativo)
                salvar_json(CONFIG_JSON, config)
                st.success("Configurações de estoque salvas.")

    with tab_categorias:
        st.dataframe(pd.DataFrame(categorias_config), use_container_width=True)
        acao_cat = st.radio("Ação de categoria", ["Adicionar", "Editar", "Excluir"], horizontal=True)

        if acao_cat == "Adicionar":
            nome_cat = st.text_input("Nome da categoria")
            cor_cat = st.color_picker("Cor", "#6157ff")
            if st.button("Adicionar categoria"):
                if nome_cat:
                    categorias_config.append({"nome": nome_cat.upper(), "cor": cor_cat})
                    salvar_json(CATEGORIAS_JSON, categorias_config)
                    st.success("Categoria adicionada.")
                    st.rerun()

        elif acao_cat == "Editar" and categorias_config:
            nomes_cat = [c["nome"] for c in categorias_config]
            selecionada = st.selectbox("Categoria", nomes_cat, key="editar_cat")
            idx = nomes_cat.index(selecionada)
            nome_cat = st.text_input("Nome", categorias_config[idx]["nome"])
            cor_cat = st.color_picker("Cor", categorias_config[idx].get("cor", "#6157ff"))
            if st.button("Salvar categoria"):
                categorias_config[idx] = {"nome": nome_cat.upper(), "cor": cor_cat}
                salvar_json(CATEGORIAS_JSON, categorias_config)
                st.success("Categoria atualizada.")
                st.rerun()

        elif acao_cat == "Excluir" and categorias_config:
            nomes_cat = [c["nome"] for c in categorias_config]
            selecionada = st.selectbox("Categoria", nomes_cat, key="excluir_cat")
            if st.button("Excluir categoria"):
                categorias_config = [c for c in categorias_config if c["nome"] != selecionada]
                salvar_json(CATEGORIAS_JSON, categorias_config)
                st.success("Categoria excluída.")
                st.rerun()

    with tab_aparencia:
        with st.form("form_aparencia"):
            tema_form = st.selectbox("Tema", ["dark", "light"], index=0 if config.get("tema", "dark") == "dark" else 1)
            cor_form = st.color_picker("Cor principal", config.get("cor_principal", "#6157ff"))
            fonte_form = st.selectbox("Fonte", ["Inter", "Arial", "Roboto", "Segoe UI"], index=["Inter", "Arial", "Roboto", "Segoe UI"].index(config.get("fonte", "Inter")) if config.get("fonte", "Inter") in ["Inter", "Arial", "Roboto", "Segoe UI"] else 0)
            if st.form_submit_button("Salvar aparência"):
                config["tema"] = tema_form
                config["cor_principal"] = cor_form
                config["fonte"] = fonte_form
                salvar_json(CONFIG_JSON, config)
                st.success("Aparência salva. A interface será atualizada.")
                st.rerun()

    with tab_backup:
        st.write(f"Último backup: {config.get('ultimo_backup', 'Nunca')}")
        if st.button("Gerar backup"):
            zip_path = gerar_backup()
            st.success(f"Backup gerado: {zip_path}")

        backup_upload = st.file_uploader("Restaurar backup", type=["zip"])
        if backup_upload and st.button("Restaurar backup agora"):
            with zipfile.ZipFile(backup_upload, "r") as zip_ref:
                for nome in zip_ref.namelist():
                    if os.path.basename(nome) in ["produtos.xlsx", "movimentacoes.xlsx", "usuarios.json", "configuracoes.json", "categorias.json"]:
                        zip_ref.extract(nome, BASE_DIR)
                        extraido = os.path.join(BASE_DIR, nome)
                        destino = os.path.join(BASE_DIR, os.path.basename(nome))
                        if extraido != destino:
                            shutil.move(extraido, destino)
            st.success("Backup restaurado.")
            st.rerun()
