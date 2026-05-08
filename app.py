import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import calendar
import os
import io
import json
import hashlib
import shutil
import zipfile
import base64
import mimetypes
try:
    from supabase import create_client
except Exception:
    create_client = None
try:
    from google.auth.transport.requests import Request
    from google.oauth2 import service_account
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
except Exception:
    Request = None
    service_account = None
    Credentials = None
    build = None
    HttpError = None
    MediaFileUpload = None
    MediaIoBaseDownload = None
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image as RLImage, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch

st.set_page_config(page_title="Sistema de Almoxarifado", layout="wide")

_streamlit_dataframe = st.dataframe


def dataframe_sem_indice(*args, **kwargs):
    kwargs.setdefault("hide_index", True)
    return _streamlit_dataframe(*args, **kwargs)


st.dataframe = dataframe_sem_indice

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("ALPES_DATA_DIR", BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
_drive_service_cache = None
_drive_pastas_cache = {}
_drive_arquivos_cache = {}
_supabase_client_cache = None


def obter_config_secreta(nome, padrao=""):
    valor = os.environ.get(nome)
    if valor:
        return valor
    try:
        return st.secrets.get(nome, padrao)
    except Exception:
        return padrao


def supabase_bucket_nome():
    return obter_config_secreta("SUPABASE_BUCKET", "alpes-system")


def supabase_chave():
    return (
        obter_config_secreta("SUPABASE_SERVICE_ROLE_KEY", "")
        or obter_config_secreta("SUPABASE_ANON_KEY", "")
        or obter_config_secreta("SUPABASE_KEY", "")
    )


def supabase_configurado():
    return bool(create_client and obter_config_secreta("SUPABASE_URL", "") and supabase_chave())


def supabase_guardar_erro(erro):
    try:
        st.session_state["ultimo_erro_supabase"] = str(erro)[:700]
    except Exception:
        pass


def obter_supabase_client():
    global _supabase_client_cache
    if _supabase_client_cache is not None:
        return _supabase_client_cache
    if not supabase_configurado():
        return None
    try:
        _supabase_client_cache = create_client(obter_config_secreta("SUPABASE_URL", ""), supabase_chave())
        return _supabase_client_cache
    except Exception as erro:
        supabase_guardar_erro(erro)
        return None


def google_drive_configurado():
    return bool(
        build
        and (service_account or Credentials)
        and MediaFileUpload
        and MediaIoBaseDownload
        and obter_config_secreta("GOOGLE_DRIVE_FOLDER_ID", "")
    )


def obter_google_oauth_info():
    info = {
        "client_id": os.environ.get("GOOGLE_OAUTH_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", ""),
        "refresh_token": os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN", ""),
        "token_uri": os.environ.get("GOOGLE_OAUTH_TOKEN_URI", "https://oauth2.googleapis.com/token"),
    }
    try:
        if "google_oauth" in st.secrets:
            segredos = dict(st.secrets["google_oauth"])
            info.update({chave: segredos.get(chave, valor) for chave, valor in info.items()})
    except Exception:
        pass
    return info if info.get("client_id") and info.get("client_secret") and info.get("refresh_token") else None


def obter_google_service():
    global _drive_service_cache
    if _drive_service_cache is not None:
        return _drive_service_cache
    if not google_drive_configurado():
        return None

    oauth_info = obter_google_oauth_info()
    if oauth_info and Credentials and Request:
        try:
            credenciais = Credentials(
                token=None,
                refresh_token=oauth_info["refresh_token"],
                token_uri=oauth_info.get("token_uri", "https://oauth2.googleapis.com/token"),
                client_id=oauth_info["client_id"],
                client_secret=oauth_info["client_secret"],
                scopes=["https://www.googleapis.com/auth/drive"]
            )
            credenciais.refresh(Request())
            _drive_service_cache = build("drive", "v3", credentials=credenciais, cache_discovery=False)
            return _drive_service_cache
        except Exception as erro:
            drive_guardar_erro(erro)

    info_conta = None
    service_account_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if service_account_json:
        info_conta = json.loads(service_account_json)
    else:
        try:
            if "google_service_account" in st.secrets:
                info_conta = dict(st.secrets["google_service_account"])
            elif "GOOGLE_SERVICE_ACCOUNT_INFO" in st.secrets:
                info_conta = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT_INFO"])
        except Exception:
            info_conta = None

    if not info_conta:
        return None

    try:
        credenciais = service_account.Credentials.from_service_account_info(
            info_conta,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        _drive_service_cache = build("drive", "v3", credentials=credenciais, cache_discovery=False)
        return _drive_service_cache
    except Exception:
        return None


def drive_relativo(caminho):
    return os.path.relpath(caminho, DATA_DIR).replace("\\", "/")


def supabase_upload_arquivo(caminho_local):
    client = obter_supabase_client()
    if not client or not os.path.isfile(caminho_local):
        return False
    caminho_relativo = drive_relativo(caminho_local)
    content_type = mimetypes.guess_type(caminho_local)[0] or "application/octet-stream"
    try:
        with open(caminho_local, "rb") as arquivo:
            dados = arquivo.read()
        bucket = client.storage.from_(supabase_bucket_nome())
        try:
            bucket.upload(
                caminho_relativo,
                dados,
                file_options={"content-type": content_type, "upsert": "true"}
            )
        except Exception:
            bucket.update(
                caminho_relativo,
                dados,
                file_options={"content-type": content_type, "upsert": "true"}
            )
        return True
    except Exception as erro:
        supabase_guardar_erro(erro)
        return False


def supabase_baixar_arquivo(caminho_local, caminho_relativo):
    client = obter_supabase_client()
    if not client:
        return False
    try:
        dados = client.storage.from_(supabase_bucket_nome()).download(caminho_relativo)
        os.makedirs(os.path.dirname(caminho_local), exist_ok=True)
        with open(caminho_local, "wb") as arquivo:
            arquivo.write(dados)
        return True
    except Exception as erro:
        supabase_guardar_erro(erro)
        return False


def sincronizar_supabase_inicio():
    client = obter_supabase_client()
    if not client:
        return
    bucket = client.storage.from_(supabase_bucket_nome())

    def percorrer(pasta=""):
        try:
            itens = bucket.list(pasta, {"limit": 1000, "offset": 0})
        except Exception as erro:
            supabase_guardar_erro(erro)
            return
        for item in itens or []:
            nome = item.get("name", "")
            if not nome:
                continue
            rel_item = f"{pasta}/{nome}".strip("/")
            if item.get("id") is None:
                os.makedirs(os.path.join(DATA_DIR, rel_item), exist_ok=True)
                percorrer(rel_item)
            else:
                supabase_baixar_arquivo(os.path.join(DATA_DIR, rel_item), rel_item)

    percorrer("")


def drive_guardar_erro(erro):
    try:
        if HttpError and isinstance(erro, HttpError):
            conteudo = erro.content.decode("utf-8", errors="ignore") if getattr(erro, "content", None) else str(erro)
            st.session_state["ultimo_erro_google_drive"] = conteudo[:700]
        else:
            st.session_state["ultimo_erro_google_drive"] = str(erro)[:700]
    except Exception:
        pass


def drive_listar_filhos(pasta_id):
    service = obter_google_service()
    if not service:
        return []
    itens = []
    token = None
    try:
        while True:
            resposta = service.files().list(
                q=f"'{pasta_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                pageToken=token,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True
            ).execute()
            itens.extend(resposta.get("files", []))
            token = resposta.get("nextPageToken")
            if not token:
                break
    except Exception as erro:
        drive_guardar_erro(erro)
    return itens


def drive_garantir_pasta(caminho_relativo):
    service = obter_google_service()
    pasta_raiz = obter_config_secreta("GOOGLE_DRIVE_FOLDER_ID", "")
    if not service or not pasta_raiz:
        return None
    if not caminho_relativo:
        return pasta_raiz
    if caminho_relativo in _drive_pastas_cache:
        return _drive_pastas_cache[caminho_relativo]

    pai = pasta_raiz
    partes = [p for p in caminho_relativo.replace("\\", "/").split("/") if p]
    caminho_atual = ""
    for parte in partes:
        caminho_atual = f"{caminho_atual}/{parte}".strip("/")
        if caminho_atual in _drive_pastas_cache:
            pai = _drive_pastas_cache[caminho_atual]
            continue
        existentes = [
            item for item in drive_listar_filhos(pai)
            if item["name"] == parte and item["mimeType"] == "application/vnd.google-apps.folder"
        ]
        if existentes:
            pai = existentes[0]["id"]
        else:
            criado = service.files().create(
                body={
                    "name": parte,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [pai]
                },
                fields="id",
                supportsAllDrives=True
            ).execute()
            pai = criado["id"]
        _drive_pastas_cache[caminho_atual] = pai
    return pai


def drive_encontrar_arquivo(caminho_relativo):
    if caminho_relativo in _drive_arquivos_cache:
        return _drive_arquivos_cache[caminho_relativo]
    pasta_rel = os.path.dirname(caminho_relativo).replace("\\", "/")
    nome = os.path.basename(caminho_relativo)
    pasta_id = drive_garantir_pasta(pasta_rel)
    if not pasta_id:
        return None
    for item in drive_listar_filhos(pasta_id):
        if item["name"] == nome and item["mimeType"] != "application/vnd.google-apps.folder":
            _drive_arquivos_cache[caminho_relativo] = item
            return item
    return None


def drive_baixar_arquivo(caminho_local, caminho_relativo):
    service = obter_google_service()
    item = drive_encontrar_arquivo(caminho_relativo)
    if not service or not item:
        return False
    os.makedirs(os.path.dirname(caminho_local), exist_ok=True)
    try:
        requisicao = service.files().get_media(fileId=item["id"], supportsAllDrives=True)
        with io.FileIO(caminho_local, "wb") as arquivo:
            downloader = MediaIoBaseDownload(arquivo, requisicao)
            concluido = False
            while not concluido:
                _, concluido = downloader.next_chunk()
        return True
    except Exception as erro:
        drive_guardar_erro(erro)
        return False


def drive_upload_arquivo(caminho_local):
    try:
        service = obter_google_service()
        if not service or not os.path.isfile(caminho_local):
            return False
        caminho_relativo = drive_relativo(caminho_local)
        pasta_rel = os.path.dirname(caminho_relativo).replace("\\", "/")
        pasta_id = drive_garantir_pasta(pasta_rel)
        if not pasta_id:
            return False
        media = MediaFileUpload(caminho_local, resumable=False)
        existente = drive_encontrar_arquivo(caminho_relativo)
        if existente:
            atualizado = service.files().update(
                fileId=existente["id"],
                media_body=media,
                fields="id, name, mimeType, modifiedTime",
                supportsAllDrives=True
            ).execute()
            _drive_arquivos_cache[caminho_relativo] = atualizado
        else:
            criado = service.files().create(
                body={"name": os.path.basename(caminho_local), "parents": [pasta_id]},
                media_body=media,
                fields="id, name, mimeType, modifiedTime",
                supportsAllDrives=True
            ).execute()
            _drive_arquivos_cache[caminho_relativo] = criado
        return True
    except Exception as erro:
        drive_guardar_erro(erro)
        return False


def sincronizar_drive_inicio():
    service = obter_google_service()
    pasta_raiz = obter_config_secreta("GOOGLE_DRIVE_FOLDER_ID", "")
    if not service or not pasta_raiz:
        return

    def percorrer(pasta_id, rel_pasta=""):
        for item in drive_listar_filhos(pasta_id):
            rel_item = f"{rel_pasta}/{item['name']}".strip("/")
            if item["mimeType"] == "application/vnd.google-apps.folder":
                _drive_pastas_cache[rel_item] = item["id"]
                os.makedirs(os.path.join(DATA_DIR, rel_item), exist_ok=True)
                percorrer(item["id"], rel_item)
            else:
                _drive_arquivos_cache[rel_item] = item
                drive_baixar_arquivo(os.path.join(DATA_DIR, rel_item), rel_item)

    percorrer(pasta_raiz)


def upload_arquivo_remoto(caminho_local):
    if supabase_configurado():
        return supabase_upload_arquivo(caminho_local)
    return drive_upload_arquivo(caminho_local)


def sincronizar_armazenamento_inicio():
    if supabase_configurado():
        sincronizar_supabase_inicio()
    else:
        sincronizar_drive_inicio()


_pandas_to_excel_original = pd.DataFrame.to_excel


def dataframe_to_excel_com_armazenamento(self, excel_writer, *args, **kwargs):
    resultado = _pandas_to_excel_original(self, excel_writer, *args, **kwargs)
    if isinstance(excel_writer, (str, os.PathLike)):
        caminho_excel = os.path.abspath(os.fspath(excel_writer))
        if caminho_excel.startswith(os.path.abspath(DATA_DIR)):
            upload_arquivo_remoto(caminho_excel)
    return resultado


pd.DataFrame.to_excel = dataframe_to_excel_com_armazenamento


def caminho_dados(nome):
    destino = os.path.join(DATA_DIR, nome)
    origem = os.path.join(BASE_DIR, nome)
    if DATA_DIR != BASE_DIR and not os.path.exists(destino) and os.path.exists(origem):
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        if os.path.isdir(origem):
            shutil.copytree(origem, destino, dirs_exist_ok=True)
        else:
            shutil.copy2(origem, destino)
    return destino


PASTA_IMAGENS = caminho_dados("Imagens Produtos")
PASTA_IMAGENS_SISTEMA = caminho_dados("Imagens Sistema")
PRODUTOS_XLSX = caminho_dados("produtos.xlsx")
MOVIMENTACOES_XLSX = caminho_dados("movimentacoes.xlsx")
CLIENTES_XLSX = caminho_dados("clientes.xlsx")
FORNECEDORES_XLSX = caminho_dados("fornecedores.xlsx")
CONTROLE_FALTAS_XLSX = caminho_dados("controle_faltas.xlsx")
FROTAS_VEICULOS_XLSX = caminho_dados("frotas_veiculos.xlsx")
FROTAS_ABASTECIMENTOS_XLSX = caminho_dados("frotas_abastecimentos.xlsx")
FROTAS_MANUTENCOES_XLSX = caminho_dados("frotas_manutencoes.xlsx")
FROTAS_DOCUMENTOS_XLSX = caminho_dados("frotas_documentos.xlsx")
ORCAMENTOS_XLSX = caminho_dados("orcamentos.xlsx")
PATRIMONIO_XLSX = caminho_dados("patrimonio.xlsx")
PATRIMONIO_CUSTOS_XLSX = caminho_dados("patrimonio_custos.xlsx")
PATRIMONIO_MOVIMENTACOES_XLSX = caminho_dados("patrimonio_movimentacoes.xlsx")
PATRIMONIO_INSUMOS_XLSX = caminho_dados("patrimonio_insumos_base.xlsx")
BASES_MOVIMENTACOES_XLSX = caminho_dados("bases_movimentacoes.xlsx")
BASES_TRANSFERENCIAS_XLSX = caminho_dados("bases_transferencias.xlsx")
PASTA_ANEXOS_ORCAMENTOS = caminho_dados("Anexos Orçamentos")
PASTA_ANEXOS_FROTAS = caminho_dados("Anexos Frotas")
USUARIOS_JSON = caminho_dados("usuarios.json")
CONFIG_JSON = caminho_dados("configuracoes.json")
CATEGORIAS_JSON = caminho_dados("categorias.json")
UNIDADES_JSON = caminho_dados("unidades.json")
BACKUP_DIR = caminho_dados("backups")
HOME_IMAGE = os.path.join(PASTA_IMAGENS_SISTEMA, "inicio.jpg")
LOGIN_IMAGE = os.path.join(PASTA_IMAGENS_SISTEMA, "login.jpg")
HOME_IMAGE_FALLBACK = os.path.join(BASE_DIR, "Desktop 1.jpg")
BASES_FREQUENCIA = ["TMG BASE SORRISO", "TMG BASE RONDONOPOLIS"]
sincronizar_armazenamento_inicio()


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
    upload_arquivo_remoto(caminho)


def garantir_pasta_imagens_sistema():
    os.makedirs(PASTA_IMAGENS, exist_ok=True)
    os.makedirs(PASTA_IMAGENS_SISTEMA, exist_ok=True)
    os.makedirs(PASTA_ANEXOS_FROTAS, exist_ok=True)
    if not os.path.exists(HOME_IMAGE) and os.path.exists(HOME_IMAGE_FALLBACK):
        shutil.copy2(HOME_IMAGE_FALLBACK, HOME_IMAGE)
    if not os.path.exists(LOGIN_IMAGE) and os.path.exists(HOME_IMAGE):
        shutil.copy2(HOME_IMAGE, LOGIN_IMAGE)


def hash_senha(senha):
    return hashlib.sha256(str(senha).encode("utf-8")).hexdigest()


def imagem_base64(caminho):
    with open(caminho, "rb") as arquivo:
        return base64.b64encode(arquivo.read()).decode("utf-8")


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
        "ultimo_backup": "Nunca",
        "supervisores_frequencia": {
            "TMG BASE SORRISO": "",
            "TMG BASE RONDONOPOLIS": ""
        }
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


def unidades_padrao():
    return [
        {"nome": "UN", "cor": "#38bdf8"},
        {"nome": "CX", "cor": "#a78bfa"},
        {"nome": "KG", "cor": "#22c55e"},
        {"nome": "LT", "cor": "#facc15"},
        {"nome": "M", "cor": "#fb923c"}
    ]


usuarios = garantir_usuario_admin()
config = carregar_json(CONFIG_JSON, configuracao_padrao())
if "supervisores_frequencia" not in config or not isinstance(config.get("supervisores_frequencia"), dict):
    config["supervisores_frequencia"] = {}
for base_frequencia in BASES_FREQUENCIA:
    config["supervisores_frequencia"].setdefault(base_frequencia, "")
categorias_config = carregar_json(CATEGORIAS_JSON, categorias_padrao())
unidades_config = carregar_json(UNIDADES_JSON, unidades_padrao())
garantir_pasta_imagens_sistema()


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
        background: linear-gradient(180deg, #263548 0%, #182232 100%);
        border: 1px solid #3f5168;
        border-radius: 8px;
        padding: 10px 12px;
        margin: 6px 0;
        transition: .15s ease;
        box-shadow:
            0 7px 0 #090f1a,
            0 14px 24px rgba(0, 0, 0, .34),
            inset 0 1px 0 rgba(255, 255, 255, .12);
        transform: translateY(0);
    }}
    [data-testid="stSidebar"] div[role="radiogroup"] label:hover {{
        background: linear-gradient(180deg, #31425a 0%, #1d2a3d 100%);
        transform: translateY(-1px);
        box-shadow:
            0 8px 0 #090f1a,
            0 18px 30px rgba(0, 0, 0, .38),
            inset 0 1px 0 rgba(255, 255, 255, .16);
    }}
    [data-testid="stSidebar"] div[role="radiogroup"] label:active {{
        transform: translateY(5px);
        box-shadow:
            0 2px 0 #090f1a,
            0 8px 16px rgba(0, 0, 0, .32),
            inset 0 2px 8px rgba(0, 0, 0, .25);
    }}
    [data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) {{
        background: linear-gradient(135deg, {cor_principal}, #2563eb);
        border-color: {cor_principal};
        box-shadow:
            0 7px 0 #172554,
            0 16px 30px rgba(37, 99, 235, .34),
            inset 0 1px 0 rgba(255, 255, 255, .24);
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
        height: 100vh;
        object-fit: cover;
        object-position: center;
        border: 0;
        display: block;
    }}
    .home-fullscreen-lock {{
        position: relative;
        width: 100%;
        height: 100vh;
        min-height: 100vh;
        overflow: hidden;
        background: #0f172a;
    }}
    .home-fullscreen-lock .home-img {{
        width: 100%;
        height: 100%;
        object-fit: cover;
        object-position: center center;
    }}
    section.main > div:has(.home-img) {{
        padding-top: 0;
        padding-bottom: 0;
        padding-left: 0;
        padding-right: 0;
        max-width: 100%;
    }}
    .block-container:has(.home-fullscreen-lock),
    [data-testid="stMainBlockContainer"]:has(.home-fullscreen-lock) {{
        padding: 0 !important;
        max-width: 100% !important;
    }}
    .login-img {{
        width: 100%;
        max-height: 180px;
        object-fit: contain;
        object-position: center;
        border: 0;
        display: block;
        margin: 0 auto 22px auto;
    }}
    div[data-testid="stMetricValue"] {{
        color: {texto_app};
    }}
    h1, h2, h3, h4, h5, h6,
    p, label, legend,
    [data-testid="stMarkdownContainer"],
    [data-testid="stWidgetLabel"],
    [data-testid="stCaptionContainer"],
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] p,
    .stButton > button,
    .stDownloadButton button,
    button[kind="primary"],
    button[kind="secondary"] {{
        text-transform: capitalize;
    }}
    .metric-value {{
        text-transform: none;
    }}
    .stButton > button, .stDownloadButton button, button[kind="primary"], button[kind="secondary"] {{
        min-height: 44px;
        border-radius: 8px !important;
        border: 1px solid rgba(148, 163, 184, .55) !important;
        background:
            linear-gradient(180deg, rgba(255,255,255,.24) 0%, rgba(255,255,255,.08) 42%, rgba(0,0,0,.18) 100%),
            linear-gradient(135deg, {cor_principal}, #2563eb) !important;
        color: #ffffff !important;
        box-shadow:
            0 7px 0 #020617,
            0 15px 24px rgba(0, 0, 0, .36),
            inset 0 2px 0 rgba(255, 255, 255, .28),
            inset 0 -2px 0 rgba(0, 0, 0, .22) !important;
        transform: translateY(0);
        transition: transform .10s ease, box-shadow .10s ease, filter .12s ease;
        font-weight: 800 !important;
        text-shadow: 0 1px 1px rgba(0,0,0,.34);
    }}
    .stButton > button:hover, .stDownloadButton button:hover, button[kind="primary"]:hover, button[kind="secondary"]:hover {{
        transform: translateY(-2px);
        filter: brightness(1.12);
        box-shadow:
            0 9px 0 #020617,
            0 21px 32px rgba(0, 0, 0, .42),
            inset 0 2px 0 rgba(255, 255, 255, .34),
            inset 0 -2px 0 rgba(0, 0, 0, .22) !important;
    }}
    .stButton > button:active, .stDownloadButton button:active, button[kind="primary"]:active, button[kind="secondary"]:active {{
        transform: translateY(6px);
        box-shadow:
            0 1px 0 #020617,
            0 7px 12px rgba(0, 0, 0, .30),
            inset 0 3px 8px rgba(0, 0, 0, .38) !important;
    }}
</style>
""", unsafe_allow_html=True)


# =========================
# LOGIN
# =========================
if "autenticado" not in st.session_state:
    st.session_state["autenticado"] = False
if "modo_acesso" not in st.session_state:
    st.session_state["modo_acesso"] = "Desktop"

if not st.session_state["autenticado"]:
    st.markdown("<br><br>", unsafe_allow_html=True)
    if st.session_state["modo_acesso"] == "Computador":
        st.session_state["modo_acesso"] = "Desktop"
    elif st.session_state["modo_acesso"] == "Celular":
        st.session_state["modo_acesso"] = "Mobile"
    colunas_login = [0.08, 1, 0.08] if st.session_state["modo_acesso"] == "Mobile" else [1, 1.1, 1]
    c1, c2, c3 = st.columns(colunas_login)
    with c2:
        st.markdown("<div class='saas-card'>", unsafe_allow_html=True)
        imagem_login = LOGIN_IMAGE if os.path.exists(LOGIN_IMAGE) else HOME_IMAGE_FALLBACK
        if os.path.exists(imagem_login):
            extensao = os.path.splitext(imagem_login)[1].lower().replace(".", "")
            mime = "jpeg" if extensao in ["jpg", "jpeg"] else "png"
            st.markdown(
                f"<img src='data:image/{mime};base64,{imagem_base64(imagem_login)}' class='login-img'>",
                unsafe_allow_html=True
            )
        st.title("Login")
        st.write("Forma De Acesso")
        acesso_mobile, acesso_desktop = st.columns(2)
        if acesso_mobile.button(
            "Mobile",
            use_container_width=True,
            type="primary" if st.session_state["modo_acesso"] == "Mobile" else "secondary"
        ):
            st.session_state["modo_acesso"] = "Mobile"
            st.rerun()
        if acesso_desktop.button(
            "Desktop",
            use_container_width=True,
            type="primary" if st.session_state["modo_acesso"] == "Desktop" else "secondary"
        ):
            st.session_state["modo_acesso"] = "Desktop"
            st.rerun()
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
                    "nivel": usuario_encontrado.get("nivel", ""),
                    "veiculo_frota": usuario_encontrado.get("veiculo_frota", ""),
                    "veiculos_frota": usuario_encontrado.get("veiculos_frota", []),
                    "bases_permitidas": usuario_encontrado.get("bases_permitidas", []),
                    "pode_lancar_despesa_frota": usuario_encontrado.get("pode_lancar_despesa_frota", False),
                    "modo_acesso": st.session_state["modo_acesso"]
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
        "estoque_minimo", "localizacao", "imagem",
        "unidade", "valor_unitario", "fornecedor"
    ])

try:
    df_mov = pd.read_excel(MOVIMENTACOES_XLSX)
except Exception:
    df_mov = pd.DataFrame(columns=[
        "produto", "tipo", "quantidade", "data", "cliente", "observacao"
    ])

try:
    df_clientes = pd.read_excel(CLIENTES_XLSX)
except Exception:
    df_clientes = pd.DataFrame(columns=[
        "codigo", "nome_cliente", "telefone", "cidade", "estado",
        "tipo_contrato", "data_inicial", "data_final", "status"
    ])

try:
    df_fornecedores = pd.read_excel(FORNECEDORES_XLSX)
except Exception:
    df_fornecedores = pd.DataFrame(columns=[
        "codigo", "nome_fornecedor", "telefone", "cidade", "estado",
        "tipo_contrato", "data_inicial", "data_final", "status"
    ])

try:
    df_faltas = pd.read_excel(CONTROLE_FALTAS_XLSX)
except Exception:
    df_faltas = pd.DataFrame(columns=[
        "data", "colaborador", "funcao", "presenca", "motivo_falta",
        "almocou_base", "observacoes"
    ])

try:
    df_frotas_veiculos = pd.read_excel(FROTAS_VEICULOS_XLSX)
except Exception:
    df_frotas_veiculos = pd.DataFrame(columns=[
        "placa", "modelo", "marca", "ano", "tipo", "responsavel", "cidade_local", "status", "km_atual"
    ])

try:
    df_frotas_abastecimentos = pd.read_excel(FROTAS_ABASTECIMENTOS_XLSX)
except Exception:
    df_frotas_abastecimentos = pd.DataFrame(columns=[
        "data", "placa", "km", "combustivel", "litros", "valor_litro", "valor_total", "posto", "responsavel_lancamento", "registrado_em", "nota_anexo", "status_conferencia", "observacao_administrativo", "observacoes"
    ])

try:
    df_frotas_manutencoes = pd.read_excel(FROTAS_MANUTENCOES_XLSX)
except Exception:
    df_frotas_manutencoes = pd.DataFrame(columns=[
        "data", "placa", "tipo_manutencao", "km", "servico_executado", "fornecedor", "valor", "manutencao_agendada", "proxima_revisao", "status_manutencao", "responsavel_lancamento", "registrado_em", "nota_anexo", "status_conferencia", "observacao_administrativo", "observacoes"
    ])

try:
    df_frotas_documentos = pd.read_excel(FROTAS_DOCUMENTOS_XLSX)
except Exception:
    df_frotas_documentos = pd.DataFrame(columns=[
        "placa", "documento", "vencimento", "valor", "status", "observacoes"
    ])

try:
    df_orcamentos = pd.read_excel(ORCAMENTOS_XLSX)
except Exception:
    df_orcamentos = pd.DataFrame(columns=[
        "numero", "data", "validade", "cliente", "fornecedor", "veiculo",
        "tipo", "descricao", "quantidade", "valor_unitario", "valor_total",
        "status", "anexo", "observacoes"
    ])

try:
    df_patrimonio = pd.read_excel(PATRIMONIO_XLSX)
except Exception:
    df_patrimonio = pd.DataFrame(columns=[
        "codigo", "nome", "tipo", "marca", "modelo", "serie", "base",
        "local", "responsavel", "data_aquisicao", "valor_compra", "status",
        "observacoes"
    ])

try:
    df_patrimonio_custos = pd.read_excel(PATRIMONIO_CUSTOS_XLSX)
except Exception:
    df_patrimonio_custos = pd.DataFrame(columns=[
        "data", "codigo", "patrimonio", "base", "tipo_custo", "quantidade",
        "unidade", "valor_unitario", "valor_total", "fornecedor",
        "operador", "observacoes"
    ])

try:
    df_patrimonio_movimentacoes = pd.read_excel(PATRIMONIO_MOVIMENTACOES_XLSX)
except Exception:
    df_patrimonio_movimentacoes = pd.DataFrame(columns=[
        "data", "codigo", "patrimonio", "base_origem", "base_destino",
        "responsavel_origem", "responsavel_destino", "tipo_movimentacao",
        "observacoes"
    ])

try:
    df_patrimonio_insumos = pd.read_excel(PATRIMONIO_INSUMOS_XLSX)
except Exception:
    df_patrimonio_insumos = pd.DataFrame(columns=[
        "data", "base", "insumo", "tipo_movimentacao", "quantidade",
        "unidade", "valor_unitario", "valor_total", "codigo", "patrimonio",
        "operador", "observacoes"
    ])

try:
    df_bases_movimentacoes = pd.read_excel(BASES_MOVIMENTACOES_XLSX)
except Exception:
    df_bases_movimentacoes = pd.DataFrame(columns=[
        "data", "base", "produto", "tipo", "quantidade", "responsavel",
        "origem_destino", "observacoes"
    ])

try:
    df_bases_transferencias = pd.read_excel(BASES_TRANSFERENCIAS_XLSX)
except Exception:
    df_bases_transferencias = pd.DataFrame(columns=[
        "data", "produto", "quantidade", "origem", "destino",
        "responsavel_envio", "responsavel_recebimento", "status",
        "observacoes"
    ])

for col in ["codigo", "produto", "categoria", "estoque_minimo", "localizacao", "imagem", "unidade", "valor_unitario", "fornecedor"]:
    if col not in df_produtos.columns:
        df_produtos[col] = ""

for col in ["produto", "tipo", "quantidade", "data", "cliente", "observacao"]:
    if col not in df_mov.columns:
        df_mov[col] = ""

for col in ["codigo", "nome_cliente", "telefone", "cidade", "estado", "tipo_contrato", "data_inicial", "data_final", "status"]:
    if col not in df_clientes.columns:
        df_clientes[col] = "Ativo" if col == "status" else ""
    df_clientes[col] = df_clientes[col].astype("object").fillna("")
df_clientes.loc[df_clientes["status"] == "", "status"] = "Ativo"

for col in ["codigo", "nome_fornecedor", "telefone", "cidade", "estado", "tipo_contrato", "data_inicial", "data_final", "status"]:
    if col not in df_fornecedores.columns:
        df_fornecedores[col] = "Ativo" if col == "status" else ""
    df_fornecedores[col] = df_fornecedores[col].astype("object").fillna("")
df_fornecedores.loc[df_fornecedores["status"] == "", "status"] = "Ativo"

for col in ["data", "colaborador", "funcao", "presenca", "motivo_falta", "almocou_base", "observacoes", "tipo_escala", "data_base_escala", "trabalha_data_base", "status_colaborador"]:
    if col not in df_faltas.columns:
        df_faltas[col] = "SEGUNDA A SEXTA" if col == "tipo_escala" else "Ativo" if col == "status_colaborador" else ""
    df_faltas[col] = df_faltas[col].astype("object").fillna("")
if "base_frequencia" not in df_faltas.columns:
    df_faltas["base_frequencia"] = "TMG BASE SORRISO"
df_faltas["base_frequencia"] = df_faltas["base_frequencia"].astype("object").fillna("")
df_faltas.loc[df_faltas["base_frequencia"].astype(str).str.strip() == "", "base_frequencia"] = "TMG BASE SORRISO"
df_faltas.loc[df_faltas["tipo_escala"] == "", "tipo_escala"] = "SEGUNDA A SEXTA"
df_faltas.loc[df_faltas["trabalha_data_base"] == "", "trabalha_data_base"] = "Sim"
df_faltas.loc[df_faltas["status_colaborador"] == "", "status_colaborador"] = "Ativo"
df_faltas["data"] = pd.to_datetime(df_faltas["data"], errors="coerce").dt.date.astype("object").fillna("")
df_faltas["presenca"] = df_faltas["presenca"].astype(str).str.upper()
df_faltas["presenca"] = df_faltas["presenca"].replace({"APRESENTAR": "PRESENTE"})
df_faltas["almocou_base"] = df_faltas["almocou_base"].astype(str).str.capitalize()

for col in ["placa", "modelo", "marca", "ano", "tipo", "responsavel", "cidade_local", "status", "km_atual"]:
    if col not in df_frotas_veiculos.columns:
        df_frotas_veiculos[col] = "Ativo" if col == "status" else ""
    df_frotas_veiculos[col] = df_frotas_veiculos[col].astype("object").fillna("")
df_frotas_veiculos.loc[df_frotas_veiculos["status"] == "", "status"] = "Ativo"

for col in ["data", "placa", "km", "combustivel", "litros", "valor_litro", "valor_total", "posto", "responsavel_lancamento", "registrado_em", "nota_anexo", "status_conferencia", "observacao_administrativo", "observacoes"]:
    if col not in df_frotas_abastecimentos.columns:
        df_frotas_abastecimentos[col] = 0 if col in ["km", "litros", "valor_litro", "valor_total"] else "Pendente" if col == "status_conferencia" else ""
    df_frotas_abastecimentos[col] = df_frotas_abastecimentos[col].astype("object").fillna("")
df_frotas_abastecimentos.loc[df_frotas_abastecimentos["status_conferencia"] == "", "status_conferencia"] = "Pendente"
for col in ["km", "litros", "valor_litro", "valor_total"]:
    df_frotas_abastecimentos[col] = pd.to_numeric(df_frotas_abastecimentos[col], errors="coerce").fillna(0)

for col in ["data", "placa", "tipo_manutencao", "km", "servico_executado", "fornecedor", "valor", "manutencao_agendada", "proxima_revisao", "status_manutencao", "responsavel_lancamento", "registrado_em", "nota_anexo", "status_conferencia", "observacao_administrativo", "observacoes"]:
    if col not in df_frotas_manutencoes.columns:
        df_frotas_manutencoes[col] = 0 if col in ["km", "valor"] else "Pendente" if col == "status_conferencia" else ""
    df_frotas_manutencoes[col] = df_frotas_manutencoes[col].astype("object").fillna("")
df_frotas_manutencoes.loc[df_frotas_manutencoes["status_manutencao"] == "", "status_manutencao"] = "Executada"
df_frotas_manutencoes.loc[df_frotas_manutencoes["status_conferencia"] == "", "status_conferencia"] = "Pendente"
for col in ["km", "valor"]:
    df_frotas_manutencoes[col] = pd.to_numeric(df_frotas_manutencoes[col], errors="coerce").fillna(0)

for col in ["data", "base", "produto", "tipo", "quantidade", "responsavel", "origem_destino", "observacoes"]:
    if col not in df_bases_movimentacoes.columns:
        df_bases_movimentacoes[col] = 0 if col == "quantidade" else ""
    df_bases_movimentacoes[col] = df_bases_movimentacoes[col].astype("object").fillna("")
df_bases_movimentacoes["quantidade"] = pd.to_numeric(df_bases_movimentacoes["quantidade"], errors="coerce").fillna(0)

for col in ["data", "produto", "quantidade", "origem", "destino", "responsavel_envio", "responsavel_recebimento", "status", "observacoes"]:
    if col not in df_bases_transferencias.columns:
        df_bases_transferencias[col] = 0 if col == "quantidade" else "Enviado" if col == "status" else ""
    df_bases_transferencias[col] = df_bases_transferencias[col].astype("object").fillna("")
df_bases_transferencias.loc[df_bases_transferencias["status"] == "", "status"] = "Enviado"
df_bases_transferencias["quantidade"] = pd.to_numeric(df_bases_transferencias["quantidade"], errors="coerce").fillna(0)


def alertas_manutencao_preventiva(df_manutencoes):
    colunas_alerta = ["placa", "manutencao_agendada", "dias", "status", "servico_executado"]
    if df_manutencoes.empty:
        return pd.DataFrame(columns=colunas_alerta)

    dados = df_manutencoes.copy()
    dados["tipo_normalizado"] = dados["tipo_manutencao"].astype(str).str.upper()
    dados["status_normalizado"] = dados["status_manutencao"].astype(str).str.upper()
    dados = dados[
        (dados["tipo_normalizado"] == "PREVENTIVA")
        & (dados["status_normalizado"] == "PROGRAMADA")
    ].copy()
    if dados.empty:
        return pd.DataFrame(columns=colunas_alerta)

    dados["manutencao_agendada_dt"] = pd.to_datetime(dados["manutencao_agendada"], errors="coerce").dt.date
    dados = dados.dropna(subset=["manutencao_agendada_dt"])
    hoje = datetime.now().date()
    alertas = []

    for _, revisao in dados.iterrows():
        placa = str(revisao.get("placa", "")).strip()
        vencimento = revisao["manutencao_agendada_dt"]

        dias = (vencimento - hoje).days
        if dias < 0:
            status_alerta = "Vencida"
        elif dias <= 10:
            status_alerta = "Vence Em Ate 10 Dias"
        else:
            continue

        alertas.append({
            "placa": placa,
            "manutencao_agendada": vencimento.strftime("%d/%m/%Y"),
            "dias": dias,
            "status": status_alerta,
            "servico_executado": revisao.get("servico_executado", "")
        })

    return pd.DataFrame(alertas, columns=colunas_alerta)


def assinatura_alertas_preventiva(alertas):
    if alertas.empty:
        return ""
    campos = ["placa", "manutencao_agendada", "status", "servico_executado"]
    dados = alertas[campos].astype(str).sort_values(campos).to_dict("records")
    return json.dumps(dados, ensure_ascii=False)


def assinatura_conferencia_frotas(df_abastecimentos, df_manutencoes):
    pendentes = []
    if not df_abastecimentos.empty:
        abastecimentos = df_abastecimentos[df_abastecimentos["status_conferencia"].astype(str) == "Pendente"].copy()
        for idx, row in abastecimentos.iterrows():
            pendentes.append({
                "tipo": "Abastecimento",
                "idx": int(idx),
                "placa": str(row.get("placa", "")),
                "registrado_em": str(row.get("registrado_em", "")),
                "valor": str(row.get("valor_total", ""))
            })
    if not df_manutencoes.empty:
        manutencoes = df_manutencoes[df_manutencoes["status_conferencia"].astype(str) == "Pendente"].copy()
        for idx, row in manutencoes.iterrows():
            pendentes.append({
                "tipo": "Manutenção",
                "idx": int(idx),
                "placa": str(row.get("placa", "")),
                "registrado_em": str(row.get("registrado_em", "")),
                "valor": str(row.get("valor", ""))
            })
    if not pendentes:
        return ""
    return json.dumps(sorted(pendentes, key=lambda item: (item["tipo"], item["idx"])), ensure_ascii=False)


def baixar_manutencoes_programadas(df_manutencoes, placa, data_execucao):
    if df_manutencoes.empty:
        return df_manutencoes

    data_execucao = pd.to_datetime(data_execucao, errors="coerce")
    if pd.isna(data_execucao):
        return df_manutencoes

    datas_programadas = pd.to_datetime(df_manutencoes["manutencao_agendada"], errors="coerce")
    filtro = (
        (df_manutencoes["placa"].astype(str) == str(placa))
        & (df_manutencoes["tipo_manutencao"].astype(str).str.upper() == "PREVENTIVA")
        & (df_manutencoes["status_manutencao"].astype(str).str.upper() == "PROGRAMADA")
        & (datas_programadas <= data_execucao)
    )
    df_manutencoes.loc[filtro, "status_manutencao"] = "Executada"
    return df_manutencoes

for col in ["placa", "documento", "vencimento", "valor", "status", "observacoes"]:
    if col not in df_frotas_documentos.columns:
        df_frotas_documentos[col] = "Ativo" if col == "status" else 0 if col == "valor" else ""
    df_frotas_documentos[col] = df_frotas_documentos[col].astype("object").fillna("")
df_frotas_documentos["valor"] = pd.to_numeric(df_frotas_documentos["valor"], errors="coerce").fillna(0)

for col in ["numero", "data", "validade", "cliente", "fornecedor", "veiculo", "tipo", "descricao", "quantidade", "valor_unitario", "valor_total", "status", "anexo", "observacoes"]:
    if col not in df_orcamentos.columns:
        df_orcamentos[col] = 0 if col in ["quantidade", "valor_unitario", "valor_total"] else "Em Aberto" if col == "status" else ""
    df_orcamentos[col] = df_orcamentos[col].astype("object").fillna("")
for col in ["quantidade", "valor_unitario", "valor_total"]:
    df_orcamentos[col] = pd.to_numeric(df_orcamentos[col], errors="coerce").fillna(0)
df_orcamentos.loc[df_orcamentos["status"] == "", "status"] = "Em Aberto"

for col in ["codigo", "nome", "tipo", "marca", "modelo", "serie", "base", "local", "responsavel", "data_aquisicao", "valor_compra", "status", "observacoes"]:
    if col not in df_patrimonio.columns:
        df_patrimonio[col] = 0 if col == "valor_compra" else "Ativo" if col == "status" else ""
    df_patrimonio[col] = df_patrimonio[col].astype("object").fillna("")
df_patrimonio.loc[df_patrimonio["status"] == "", "status"] = "Ativo"
df_patrimonio.loc[df_patrimonio["base"] == "", "base"] = "TMG BASE SORRISO"
df_patrimonio["valor_compra"] = pd.to_numeric(df_patrimonio["valor_compra"], errors="coerce").fillna(0)

for col in ["data", "codigo", "patrimonio", "base", "tipo_custo", "quantidade", "unidade", "valor_unitario", "valor_total", "fornecedor", "operador", "observacoes"]:
    if col not in df_patrimonio_custos.columns:
        df_patrimonio_custos[col] = 0 if col in ["quantidade", "valor_unitario", "valor_total"] else ""
    df_patrimonio_custos[col] = df_patrimonio_custos[col].astype("object").fillna("")
for col in ["quantidade", "valor_unitario", "valor_total"]:
    df_patrimonio_custos[col] = pd.to_numeric(df_patrimonio_custos[col], errors="coerce").fillna(0)

for col in ["data", "codigo", "patrimonio", "base_origem", "base_destino", "responsavel_origem", "responsavel_destino", "tipo_movimentacao", "observacoes"]:
    if col not in df_patrimonio_movimentacoes.columns:
        df_patrimonio_movimentacoes[col] = ""
    df_patrimonio_movimentacoes[col] = df_patrimonio_movimentacoes[col].astype("object").fillna("")

for col in ["data", "base", "insumo", "tipo_movimentacao", "quantidade", "unidade", "valor_unitario", "valor_total", "codigo", "patrimonio", "operador", "observacoes"]:
    if col not in df_patrimonio_insumos.columns:
        df_patrimonio_insumos[col] = 0 if col in ["quantidade", "valor_unitario", "valor_total"] else ""
    df_patrimonio_insumos[col] = df_patrimonio_insumos[col].astype("object").fillna("")
for col in ["quantidade", "valor_unitario", "valor_total"]:
    df_patrimonio_insumos[col] = pd.to_numeric(df_patrimonio_insumos[col], errors="coerce").fillna(0)

df_produtos["estoque_minimo"] = pd.to_numeric(df_produtos["estoque_minimo"], errors="coerce").fillna(0)
df_produtos["valor_unitario"] = pd.to_numeric(df_produtos["valor_unitario"], errors="coerce").fillna(0)
df_mov["quantidade"] = pd.to_numeric(df_mov["quantidade"], errors="coerce").fillna(0)
df_mov["tipo"] = df_mov["tipo"].astype(str).replace({
    "Saida": "Saída",
    "SaÃ­da": "Saída",
    "saida": "Saída",
    "saída": "Saída",
    "entrada": "Entrada"
})


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


def calcular_menos_movimentados(df_relatorio, produtos_base):
    colunas = ["codigo", "produto", "categoria", "quantidade"]
    if produtos_base.empty:
        return pd.DataFrame(columns=colunas)

    produtos = produtos_base[["codigo", "produto", "categoria"]].copy()
    if df_relatorio.empty:
        produtos["quantidade"] = 0
        return produtos[colunas].sort_values(["quantidade", "produto"], ascending=[True, True])

    mov = df_relatorio.groupby("produto")["quantidade"].sum().reset_index()
    menos_mov = produtos.merge(mov, on="produto", how="left")
    menos_mov["quantidade"] = menos_mov["quantidade"].fillna(0).astype(int)
    return menos_mov[colunas].sort_values(["quantidade", "produto"], ascending=[True, True])


def calcular_gastos_clientes(df_relatorio):
    colunas_detalhe = ["cliente", "produto", "quantidade", "valor_unitario", "total"]
    colunas_resumo = ["cliente", "quantidade", "total"]
    if df_relatorio.empty:
        return pd.DataFrame(columns=colunas_resumo), pd.DataFrame(columns=colunas_detalhe)

    saidas = df_relatorio[df_relatorio["tipo"] == "Saída"].copy()
    if saidas.empty:
        return pd.DataFrame(columns=colunas_resumo), pd.DataFrame(columns=colunas_detalhe)

    saidas["cliente"] = saidas.get("cliente", "").fillna("").astype(str)
    saidas = saidas[saidas["cliente"].str.strip() != ""]
    if saidas.empty:
        return pd.DataFrame(columns=colunas_resumo), pd.DataFrame(columns=colunas_detalhe)

    produtos_valor = df_produtos[["produto", "valor_unitario"]].copy()
    produtos_valor["valor_unitario"] = pd.to_numeric(produtos_valor["valor_unitario"], errors="coerce").fillna(0)
    detalhe = saidas.merge(produtos_valor, on="produto", how="left")
    detalhe["valor_unitario"] = detalhe["valor_unitario"].fillna(0)
    detalhe["quantidade"] = pd.to_numeric(detalhe["quantidade"], errors="coerce").fillna(0)
    detalhe["total"] = detalhe["quantidade"] * detalhe["valor_unitario"]

    detalhe = detalhe.groupby(["cliente", "produto", "valor_unitario"], as_index=False)["quantidade"].sum()
    detalhe["total"] = detalhe["quantidade"] * detalhe["valor_unitario"]
    detalhe = detalhe[colunas_detalhe].sort_values(["cliente", "total"], ascending=[True, False])

    resumo = detalhe.groupby("cliente", as_index=False).agg({
        "quantidade": "sum",
        "total": "sum"
    }).sort_values("total", ascending=False)

    return resumo[colunas_resumo], detalhe[colunas_detalhe]


def produtos_mais_saidas(df_relatorio):
    colunas = ["produto", "quantidade"]
    if df_relatorio.empty:
        return pd.DataFrame(columns=colunas)
    saidas = df_relatorio[df_relatorio["tipo"].astype(str).isin(["Saída", "Saida", "SaÃ­da", "SaÃƒÂ­da"])].copy()
    if saidas.empty:
        return pd.DataFrame(columns=colunas)
    saidas["quantidade"] = pd.to_numeric(saidas["quantidade"], errors="coerce").fillna(0)
    return saidas.groupby("produto", as_index=False)["quantidade"].sum().sort_values("quantidade", ascending=False)


def calcular_estoque_base(df_mov_base, base):
    colunas = ["produto", "entradas", "saidas", "estoque_atual"]
    if df_mov_base.empty:
        return pd.DataFrame(columns=colunas)
    dados = df_mov_base[df_mov_base["base"].astype(str) == str(base)].copy()
    if dados.empty:
        return pd.DataFrame(columns=colunas)
    dados["quantidade"] = pd.to_numeric(dados["quantidade"], errors="coerce").fillna(0)
    entradas = dados[dados["tipo"].astype(str) == "Entrada"].groupby("produto")["quantidade"].sum()
    saidas = dados[dados["tipo"].astype(str) == "Saída"].groupby("produto")["quantidade"].sum()
    produtos = sorted(set(entradas.index.tolist() + saidas.index.tolist()))
    estoque = pd.DataFrame({"produto": produtos})
    estoque["entradas"] = estoque["produto"].map(entradas).fillna(0)
    estoque["saidas"] = estoque["produto"].map(saidas).fillna(0)
    estoque["estoque_atual"] = estoque["entradas"] - estoque["saidas"]
    return estoque[colunas].sort_values("produto")


def estoque_matriz_produto(produto):
    if df_produtos.empty:
        return 0
    linha = df_produtos[df_produtos["produto"].astype(str) == str(produto)]
    if linha.empty:
        return 0
    return float(pd.to_numeric(linha.iloc[0].get("estoque_atual", 0), errors="coerce") or 0)


def usuario_pode_acessar_base(usuario, base):
    if usuario.get("nivel") == "Administrador":
        return True
    bases_permitidas = usuario.get("bases_permitidas", [])
    if isinstance(bases_permitidas, str):
        bases_permitidas = [bases_permitidas] if bases_permitidas.strip() else []
    return str(base) in bases_permitidas


def usuario_pode_lancar_despesa_frota(usuario):
    return usuario.get("nivel") in ["Administrador", "Supervisor Base", "Responsável Frota"] or bool(usuario.get("pode_lancar_despesa_frota", False))


def formatar_colunas_tabela(df):
    df_formatado = df.copy()
    df_formatado.columns = [
        str(col).replace("_", " ").title()
        for col in df_formatado.columns
    ]
    return df_formatado


def proximo_numero_orcamento():
    if df_orcamentos.empty or "numero" not in df_orcamentos.columns:
        return "ORC-001"
    numeros = df_orcamentos["numero"].dropna().astype(str).tolist()
    maior = 0
    for numero in numeros:
        if numero.upper().startswith("ORC-"):
            try:
                maior = max(maior, int(numero.split("-")[-1]))
            except Exception:
                pass
    return f"ORC-{maior + 1:03d}"


def proximo_codigo_patrimonio():
    if df_patrimonio.empty or "codigo" not in df_patrimonio.columns:
        return "PAT-001"
    maior = 0
    for codigo in df_patrimonio["codigo"].dropna().astype(str).tolist():
        if codigo.upper().startswith("PAT-"):
            try:
                maior = max(maior, int(codigo.split("-")[-1]))
            except Exception:
                pass
    return f"PAT-{maior + 1:03d}"


def saldo_insumos_base(df_insumos):
    colunas = ["base", "insumo", "unidade", "entradas", "saidas", "saldo"]
    if df_insumos.empty:
        return pd.DataFrame(columns=colunas)
    dados = df_insumos.copy()
    dados["quantidade"] = pd.to_numeric(dados["quantidade"], errors="coerce").fillna(0)
    entradas = dados[dados["tipo_movimentacao"] == "Entrada"].groupby(["base", "insumo", "unidade"], as_index=False)["quantidade"].sum()
    entradas = entradas.rename(columns={"quantidade": "entradas"})
    saidas = dados[dados["tipo_movimentacao"] == "Saída"].groupby(["base", "insumo", "unidade"], as_index=False)["quantidade"].sum()
    saidas = saidas.rename(columns={"quantidade": "saidas"})
    saldo = pd.merge(entradas, saidas, on=["base", "insumo", "unidade"], how="outer").fillna(0)
    saldo["saldo"] = saldo["entradas"] - saldo["saidas"]
    return saldo[colunas].sort_values(["base", "insumo"]).reset_index(drop=True)


def colaboradores_frequencia(df):
    colunas = ["colaborador", "funcao", "tipo_escala", "data_base_escala", "trabalha_data_base", "status_colaborador"]
    if df.empty:
        return pd.DataFrame(columns=colunas)

    dados = df[df["colaborador"].astype(str).str.strip() != ""].copy()
    if dados.empty:
        return pd.DataFrame(columns=colunas)

    for col in colunas:
        if col not in dados.columns:
            dados[col] = ""
    dados["_data_ordem"] = pd.to_datetime(dados.get("data", ""), errors="coerce")
    dados = dados.sort_values("_data_ordem").drop_duplicates("colaborador", keep="last")
    dados = dados[colunas].fillna("")
    dados.loc[dados["tipo_escala"] == "", "tipo_escala"] = "SEGUNDA A SEXTA"
    dados.loc[dados["trabalha_data_base"] == "", "trabalha_data_base"] = "Sim"
    dados.loc[dados["status_colaborador"] == "", "status_colaborador"] = "Ativo"
    return dados.sort_values("colaborador").reset_index(drop=True)


def status_previsto_escala(data_ref, tipo_escala, data_base_escala="", trabalha_data_base="Sim", feriado=False):
    if feriado and str(tipo_escala).upper() == "SEGUNDA A SEXTA":
        return "FOLGA"
    if str(tipo_escala).upper() == "SEGUNDA A SEXTA":
        return "PRESENTE" if data_ref.weekday() < 5 else "FOLGA"

    data_base = pd.to_datetime(data_base_escala, errors="coerce")
    if pd.isna(data_base):
        return "PRESENTE"

    diferenca = (pd.to_datetime(data_ref).date() - data_base.date()).days
    paridade_trabalho = 0 if str(trabalha_data_base).lower() == "sim" else 1
    return "PRESENTE" if abs(diferenca) % 2 == paridade_trabalho else "FOLGA"


def proximo_codigo_cliente():
    if df_clientes.empty:
        return "001"

    codigos = pd.to_numeric(df_clientes["codigo"].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    maior_codigo = int(codigos.max()) if codigos.notna().any() else 0
    return f"{maior_codigo + 1:03d}"


def proximo_codigo_fornecedor():
    if df_fornecedores.empty:
        return "001"

    codigos = pd.to_numeric(df_fornecedores["codigo"].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    maior_codigo = int(codigos.max()) if codigos.notna().any() else 0
    return f"{maior_codigo + 1:03d}"


def proximo_codigo_produto():
    if df_produtos.empty:
        return "AL-001"

    codigos = pd.to_numeric(df_produtos["codigo"].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    maior_codigo = int(codigos.max()) if codigos.notna().any() else 0
    return f"AL-{maior_codigo + 1:03d}"


def gerar_pdf_relatorios(df_rel, df_criticos, df_menos_mov, df_gastos_clientes, df_gastos_detalhe, metricas):
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
        ("Gasto por cliente", df_gastos_clientes.head(20) if not df_gastos_clientes.empty else pd.DataFrame(columns=["cliente", "quantidade", "total"])),
        ("Produtos por cliente", df_gastos_detalhe.head(20) if not df_gastos_detalhe.empty else pd.DataFrame(columns=["cliente", "produto", "quantidade", "valor_unitario", "total"])),
        ("Produtos mais movimentados", mais_mov),
        ("Produtos menos movimentados", df_menos_mov.head(15) if not df_menos_mov.empty else pd.DataFrame(columns=["codigo", "produto", "categoria", "quantidade"])),
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


def gerar_excel_relatorios(df_rel, df_criticos, df_menos_mov, df_gastos_clientes, df_gastos_detalhe, metricas):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame([metricas]).to_excel(writer, sheet_name="Resumo", index=False)
        df_rel.to_excel(writer, sheet_name="Historico", index=False)
        df_gastos_clientes.to_excel(writer, sheet_name="Gasto por Cliente", index=False)
        df_gastos_detalhe.to_excel(writer, sheet_name="Produtos por Cliente", index=False)
        df_criticos.to_excel(writer, sheet_name="Produtos Criticos", index=False)
        df_menos_mov.to_excel(writer, sheet_name="Menos Movimentados", index=False)
        if not df_rel.empty:
            df_rel.groupby(["produto", "tipo"])["quantidade"].sum().reset_index().to_excel(writer, sheet_name="Mais Movimentados", index=False)
    buffer.seek(0)
    return buffer


def gerar_pdf_relatorio_frequencia(base, data_inicio, data_fim, rel_filtrado, resumo_presenca, resumo_funcao, resumo_dia, resumo_colaborador, resumo_almoco_funcao, metricas, tipo_relatorio="Completo"):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(letter),
        rightMargin=24,
        leftMargin=24,
        topMargin=24,
        bottomMargin=24
    )
    styles = getSampleStyleSheet()
    titulo_style = styles["Title"]
    titulo_style.fontSize = 15
    heading_style = styles["Heading2"]
    heading_style.fontSize = 10
    cell_style = styles["BodyText"]
    cell_style.fontSize = 6.5
    cell_style.leading = 7.5

    def texto_pdf(valor):
        if pd.isna(valor):
            return ""
        texto = str(valor).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return texto

    def tabela_pdf(df, col_widths=None, limite_linhas=None):
        dados = df.copy()
        if limite_linhas:
            dados = dados.head(limite_linhas)
        if dados.empty:
            dados = pd.DataFrame([["Sem registros"]], columns=["informacao"])
        linhas = [[Paragraph(texto_pdf(col).upper(), cell_style) for col in dados.columns]]
        for _, row in dados.iterrows():
            linhas.append([Paragraph(texto_pdf(valor), cell_style) for valor in row.tolist()])
        tabela = Table(linhas, colWidths=col_widths, repeatRows=1, splitByRow=1)
        tabela.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#94a3b8")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        return tabela

    elementos = [
        Paragraph(f"Relatorio De Frequencia - {texto_pdf(tipo_relatorio)}", titulo_style),
        Paragraph(f"Base: {texto_pdf(base)} | Periodo: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}", styles["Normal"]),
        Spacer(1, 10)
    ]

    metricas_df = pd.DataFrame([{
        "Lancamentos": metricas["lancamentos"],
        "Presentes": metricas["presentes"],
        "Faltas": metricas["faltas"],
        "Atestados": metricas["atestados"],
        "Almocos": metricas["almocos"]
    }])
    elementos.extend([tabela_pdf(metricas_df, [110, 110, 110, 110, 110]), Spacer(1, 10)])

    secoes_pdf = [
        ("Por Presenca", "Resumo Por Presenca", resumo_presenca, [220, 80]),
        ("Por Funcao", "Resumo Por Funcao", resumo_funcao, [260, 80]),
        ("Por Dia", "Resumo Por Dia", resumo_dia, [90, 90, 90, 90, 90]),
        ("Por Colaborador", "Resumo Por Colaborador", resumo_colaborador, [200, 70, 70, 70, 70, 70]),
        ("Almoco Por Funcao", "Quantidade De Almoco Por Funcao", resumo_almoco_funcao, [260, 100]),
    ]
    for chave, titulo, dados, larguras in secoes_pdf:
        if tipo_relatorio in ["Completo", chave]:
            elementos.extend([Paragraph(titulo, heading_style), tabela_pdf(dados, larguras), Spacer(1, 10)])

    if tipo_relatorio in ["Completo", "Lancamentos Detalhados"]:
        colunas_detalhe = ["data", "colaborador", "funcao", "presenca", "motivo_falta", "almocou_base", "observacoes"]
        detalhe = rel_filtrado[colunas_detalhe].copy() if not rel_filtrado.empty else pd.DataFrame(columns=colunas_detalhe)
        elementos.extend([
            Paragraph("Lancamentos Do Periodo", heading_style),
            tabela_pdf(detalhe, [58, 145, 120, 80, 130, 62, 170])
        ])

    doc.build(elementos)
    buffer.seek(0)
    return buffer.getvalue()


def gerar_pdf_etiquetas(itens):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=24,
        rightMargin=24,
        topMargin=24,
        bottomMargin=24
    )
    estilos = getSampleStyleSheet()
    elementos = [Paragraph("Etiquetas De Entrada", estilos["Title"]), Spacer(1, 12)]
    linhas = []
    linha = []
    for item in itens:
        etiqueta = [
            Paragraph(f"<b>{item.get('produto', '')}</b>", estilos["Normal"]),
            Paragraph(f"Código: {item.get('codigo', '')}", estilos["Normal"]),
            Paragraph(f"Quantidade: {item.get('quantidade', '')}", estilos["Normal"]),
            Paragraph(f"Data: {item.get('data', '')}", estilos["Normal"]),
        ]
        linha.append(etiqueta)
        if len(linha) == 2:
            linhas.append(linha)
            linha = []
    if linha:
        linha.append("")
        linhas.append(linha)

    tabela = Table(linhas, colWidths=[260, 260], rowHeights=92)
    tabela.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#111827")),
        ("INNERGRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#94a3b8")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    elementos.append(tabela)
    doc.build(elementos)
    buffer.seek(0)
    return buffer


def gerar_backup():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    nome = f"backup_estoque_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    pasta_temp = os.path.join(BACKUP_DIR, nome)
    os.makedirs(pasta_temp, exist_ok=True)
    for caminho in [PRODUTOS_XLSX, MOVIMENTACOES_XLSX, CLIENTES_XLSX, FORNECEDORES_XLSX, CONTROLE_FALTAS_XLSX, FROTAS_VEICULOS_XLSX, FROTAS_ABASTECIMENTOS_XLSX, FROTAS_MANUTENCOES_XLSX, FROTAS_DOCUMENTOS_XLSX, ORCAMENTOS_XLSX, PATRIMONIO_XLSX, PATRIMONIO_CUSTOS_XLSX, PATRIMONIO_MOVIMENTACOES_XLSX, PATRIMONIO_INSUMOS_XLSX, BASES_MOVIMENTACOES_XLSX, BASES_TRANSFERENCIAS_XLSX, USUARIOS_JSON, CONFIG_JSON, CATEGORIAS_JSON, UNIDADES_JSON]:
        if os.path.exists(caminho):
            shutil.copy2(caminho, os.path.join(pasta_temp, os.path.basename(caminho)))
    zip_path = shutil.make_archive(pasta_temp, "zip", pasta_temp)
    shutil.rmtree(pasta_temp, ignore_errors=True)
    config["ultimo_backup"] = datetime.now().strftime("%d/%m/%Y %H:%M")
    salvar_json(CONFIG_JSON, config)
    return zip_path


def nomes_responsaveis_frota(valor):
    nomes = []
    texto = str(valor or "")
    for separador in [";", "/", "|"]:
        texto = texto.replace(separador, ",")
    for nome in texto.split(","):
        nome_limpo = nome.strip().title()
        if nome_limpo:
            nomes.append(nome_limpo)
    return nomes


def salvar_anexo_frota(arquivo, placa, tipo_lancamento):
    if not arquivo:
        return ""
    os.makedirs(PASTA_ANEXOS_FROTAS, exist_ok=True)
    nome_original = os.path.basename(arquivo.name)
    nome_limpo = "".join(caractere for caractere in nome_original if caractere.isalnum() or caractere in "._- ").strip()
    placa_limpa = "".join(caractere for caractere in str(placa) if caractere.isalnum() or caractere in "-_").strip()
    prefixo = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho = os.path.join(PASTA_ANEXOS_FROTAS, f"{prefixo}_{placa_limpa}_{tipo_lancamento}_{nome_limpo}")
    with open(caminho, "wb") as destino:
        destino.write(arquivo.getbuffer())
    upload_arquivo_remoto(caminho)
    return caminho


def salvar_imagem_produto(arquivo, codigo, produto):
    if not arquivo:
        return ""
    os.makedirs(PASTA_IMAGENS, exist_ok=True)
    extensao = os.path.splitext(arquivo.name)[1].lower()
    nome_base = f"{codigo}_{produto}".strip()
    nome_base = "".join(caractere for caractere in nome_base if caractere.isalnum() or caractere in "._- ").strip()
    nome_base = nome_base.replace(" ", "_") or datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"{nome_base}{extensao}"
    caminho = os.path.join(PASTA_IMAGENS, nome_arquivo)
    contador = 1
    while os.path.exists(caminho):
        nome_arquivo = f"{nome_base}_{contador}{extensao}"
        caminho = os.path.join(PASTA_IMAGENS, nome_arquivo)
        contador += 1
    with open(caminho, "wb") as destino:
        destino.write(arquivo.getbuffer())
    if not upload_arquivo_remoto(caminho):
        erro_remoto = st.session_state.get("ultimo_erro_supabase", "") or st.session_state.get("ultimo_erro_google_drive", "")
        detalhe = f" Detalhe: {erro_remoto}" if erro_remoto else ""
        st.warning(f"Imagem salva, mas nao foi enviada ao armazenamento online.{detalhe}")
    return nome_arquivo


def exibir_anexo_nota(caminho, chave):
    caminho = str(caminho or "").strip()
    if not caminho:
        st.caption("Sem nota anexada.")
        return
    if not os.path.exists(caminho):
        st.warning("Nota anexada não encontrada no computador.")
        return

    nome = os.path.basename(caminho)
    extensao = os.path.splitext(caminho)[1].lower()
    with open(caminho, "rb") as arquivo:
        dados = arquivo.read()

    st.download_button(
        "Abrir / Baixar Nota",
        data=dados,
        file_name=nome,
        mime="application/pdf" if extensao == ".pdf" else "image/*",
        key=chave,
        use_container_width=True
    )
    if extensao in [".png", ".jpg", ".jpeg", ".webp"]:
        st.image(caminho, caption=nome, use_container_width=True)


def exibir_consulta_abastecimentos(df_abastecimentos, titulo="Histórico De Abastecimentos"):
    st.subheader(titulo)
    if df_abastecimentos.empty:
        st.info("Nenhum abastecimento registrado.")
        return

    for idx, row in df_abastecimentos.reset_index(drop=True).iterrows():
        data = row.get("data", "")
        placa = row.get("placa", "")
        valor = pd.to_numeric(row.get("valor_total", 0), errors="coerce")
        valor = 0 if pd.isna(valor) else float(valor)
        with st.expander(f"{data} | {placa} | R$ {valor:,.2f}", expanded=False):
            dados = pd.DataFrame([row.to_dict()])
            if "nota_anexo" in dados.columns:
                dados = dados.drop(columns=["nota_anexo"])
            st.dataframe(formatar_colunas_tabela(dados), use_container_width=True, hide_index=True)
            exibir_anexo_nota(row.get("nota_anexo", ""), f"nota_abastecimento_{idx}_{placa}_{data}")


def exibir_conferencia_lancamentos(tipo, df_lancamentos, arquivo_destino):
    global df_frotas_abastecimentos, df_frotas_manutencoes

    if df_lancamentos.empty:
        st.info(f"Nenhum lançamento de {tipo.lower()} encontrado.")
        return

    status_filtro = st.selectbox(
        f"Status De Conferência - {tipo}",
        ["Pendentes", "Todos", "Aprovados", "Reprovados"],
        key=f"filtro_conferencia_{tipo}"
    )
    dados = df_lancamentos.copy()
    if status_filtro == "Pendentes":
        dados = dados[dados["status_conferencia"].astype(str) == "Pendente"]
    elif status_filtro == "Aprovados":
        dados = dados[dados["status_conferencia"].astype(str) == "Aprovado"]
    elif status_filtro == "Reprovados":
        dados = dados[dados["status_conferencia"].astype(str) == "Reprovado"]

    if dados.empty:
        st.info("Nenhum lançamento neste filtro.")
        return

    for idx, row in dados.iterrows():
        valor_coluna = "valor_total" if tipo == "Abastecimento" else "valor"
        valor = pd.to_numeric(row.get(valor_coluna, 0), errors="coerce")
        valor = 0 if pd.isna(valor) else float(valor)
        titulo = f"{row.get('status_conferencia', 'Pendente')} | {row.get('data', '')} | {row.get('placa', '')} | R$ {valor:,.2f}"
        with st.expander(titulo, expanded=str(row.get("status_conferencia", "")) == "Pendente"):
            tabela = pd.DataFrame([row.to_dict()])
            if "nota_anexo" in tabela.columns:
                tabela = tabela.drop(columns=["nota_anexo"])
            st.dataframe(formatar_colunas_tabela(tabela), use_container_width=True, hide_index=True)
            exibir_anexo_nota(row.get("nota_anexo", ""), f"nota_conferencia_{tipo}_{idx}")

            status_atual = str(row.get("status_conferencia", "Pendente"))
            status_opcoes = ["Pendente", "Aprovado", "Reprovado"]
            status_novo = st.selectbox(
                "Status",
                status_opcoes,
                index=status_opcoes.index(status_atual) if status_atual in status_opcoes else 0,
                key=f"status_conf_{tipo}_{idx}"
            )
            observacao_admin = st.text_area(
                "Observação Do Administrativo",
                value=str(row.get("observacao_administrativo", "")),
                key=f"obs_conf_{tipo}_{idx}"
            ).strip()

            if st.button("SALVAR CONFERÊNCIA", type="primary", use_container_width=True, key=f"salvar_conf_{tipo}_{idx}"):
                df_lancamentos.loc[idx, "status_conferencia"] = status_novo
                df_lancamentos.loc[idx, "observacao_administrativo"] = observacao_admin
                df_lancamentos.to_excel(arquivo_destino, index=False)
                if tipo == "Abastecimento":
                    df_frotas_abastecimentos = df_lancamentos
                else:
                    df_frotas_manutencoes = df_lancamentos
                st.success("Conferência salva.")
                st.rerun()


def tela_responsavel_frota():
    global df_frotas_abastecimentos, df_frotas_manutencoes, df_frotas_veiculos

    usuario = st.session_state.get("usuario_logado", {})
    veiculos_permitidos_usuario = usuario.get("veiculos_frota", [])
    if isinstance(veiculos_permitidos_usuario, str):
        veiculos_permitidos_usuario = [veiculos_permitidos_usuario] if veiculos_permitidos_usuario.strip() else []
    veiculo_permitido_antigo = str(usuario.get("veiculo_frota", "")).strip()
    if veiculo_permitido_antigo and veiculo_permitido_antigo not in veiculos_permitidos_usuario:
        veiculos_permitidos_usuario.append(veiculo_permitido_antigo)
    placas_ativas_responsavel = df_frotas_veiculos[df_frotas_veiculos["status"] != "Inativo"]["placa"].dropna().astype(str).tolist()
    placas_ativas_responsavel = [p for p in placas_ativas_responsavel if p.strip()]

    if veiculos_permitidos_usuario:
        placas_permitidas = [p for p in veiculos_permitidos_usuario if p in placas_ativas_responsavel]
    else:
        placas_permitidas = placas_ativas_responsavel

    st.title("Lançamento De Despesa")
    st.caption(f"Usuário: {usuario.get('nome', '')}")
    feedback_lancamento = st.session_state.get("feedback_lancamento")
    if feedback_lancamento:
        if feedback_lancamento.get("tipo") == "sucesso":
            st.success(feedback_lancamento.get("mensagem", "Lançamento salvo com sucesso."))
            if st.button("OK", type="primary", use_container_width=True, key="ok_feedback_lancamento"):
                st.session_state.pop("feedback_lancamento", None)
                st.rerun()
        else:
            st.error(feedback_lancamento.get("mensagem", "Não foi possível salvar o lançamento."))
            if st.button("OK", type="primary", use_container_width=True, key="ok_erro_lancamento"):
                st.session_state.pop("feedback_lancamento", None)
                st.rerun()

    if not placas_permitidas:
        st.error("Nenhum veículo ativo foi liberado para este usuário. Fale com o administrador.")
        return

    tipo_lancamento = st.radio("Tipo De Lançamento", ["Abastecimento", "Manutenção"], horizontal=True)
    placa = st.selectbox("Veículo", placas_permitidas)
    veiculo_lancamento = df_frotas_veiculos[df_frotas_veiculos["placa"].astype(str) == str(placa)]
    responsavel_padrao = str(veiculo_lancamento.iloc[0].get("responsavel", "")).strip().title() if not veiculo_lancamento.empty else ""
    responsavel_lancamento = st.text_input(
        "Responsável Pelo Lançamento",
        value=responsavel_padrao or str(usuario.get("nome", "")).strip().title()
    ).strip().title()

    if tipo_lancamento == "Abastecimento":
        data = st.date_input("Data", value=datetime.now().date(), key="resp_abast_data")
        km = st.number_input("Km", min_value=0, value=0, key="resp_abast_km")
        combustivel = st.selectbox("Combustível", ["Gasolina", "Etanol", "Diesel", "GNV", "Outro"], key="resp_abast_combustivel")
        litros = st.number_input("Litros", min_value=0.0, step=0.01, format="%.2f", key="resp_abast_litros")
        valor_litro = st.number_input("Valor Por Litro", min_value=0.0, step=0.01, format="%.2f", key="resp_abast_valor_litro")
        valor_total = float(litros) * float(valor_litro)
        posto = st.text_input("Posto", key="resp_abast_posto").strip().title()
        observacoes = st.text_area("Observações", key="resp_abast_obs").strip()
        nota_anexo = st.file_uploader(
            "Anexar Nota",
            type=["pdf", "png", "jpg", "jpeg", "webp"],
            key="resp_abast_nota"
        )
        st.metric("Valor Total", f"R$ {valor_total:,.2f}")

        if st.button("SALVAR ABASTECIMENTO", type="primary", use_container_width=True):
            if not responsavel_lancamento:
                st.error("Informe o responsável pelo lançamento.")
            elif not placa:
                st.error("Selecione o veículo.")
            elif km <= 0:
                st.error("Informe o km.")
            elif not combustivel:
                st.error("Informe o combustível.")
            elif litros <= 0:
                st.error("Informe a quantidade de litros.")
            elif valor_litro <= 0:
                st.error("Informe o valor por litro.")
            elif not posto:
                st.error("Informe o posto.")
            elif not observacoes:
                st.error("Informe as observações.")
            elif not nota_anexo:
                st.error("Anexe a nota.")
            else:
                caminho_nota = salvar_anexo_frota(nota_anexo, placa, "abastecimento")
                novo = pd.DataFrame([{
                    "data": data.isoformat(),
                    "placa": placa,
                    "km": int(km),
                    "combustivel": combustivel,
                    "litros": float(litros),
                    "valor_litro": float(valor_litro),
                    "valor_total": float(valor_total),
                    "posto": posto,
                    "responsavel_lancamento": responsavel_lancamento,
                    "registrado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
                    "nota_anexo": caminho_nota,
                    "status_conferencia": "Pendente",
                    "observacao_administrativo": "",
                    "observacoes": observacoes
                }])
                df_frotas_abastecimentos = pd.concat([df_frotas_abastecimentos, novo], ignore_index=True)
                df_frotas_abastecimentos.to_excel(FROTAS_ABASTECIMENTOS_XLSX, index=False)
                df_frotas_veiculos.loc[df_frotas_veiculos["placa"].astype(str) == placa, "km_atual"] = int(km)
                df_frotas_veiculos.to_excel(FROTAS_VEICULOS_XLSX, index=False)
                st.session_state["feedback_lancamento"] = {
                    "tipo": "sucesso",
                    "mensagem": "Abastecimento salvo com sucesso."
                }
                st.rerun()
    else:
        data = st.date_input("Data", value=datetime.now().date(), key="resp_manut_data")
        tipo_manutencao = st.selectbox("Tipo De Manutenção", ["Preventiva", "Corretiva"], key="resp_manut_tipo")
        km = st.number_input("Km", min_value=0, value=0, key="resp_manut_km")
        servico_executado = st.text_input("Serviço Executado", key="resp_manut_servico").strip().title()
        fornecedor = st.text_input("Fornecedor/Oficina", key="resp_manut_fornecedor").strip().title()
        valor = st.number_input("Valor", min_value=0.0, step=0.01, format="%.2f", key="resp_manut_valor")
        observacoes = st.text_area("Observações", key="resp_manut_obs").strip()
        nota_anexo = st.file_uploader(
            "Anexar Nota",
            type=["pdf", "png", "jpg", "jpeg", "webp"],
            key="resp_manut_nota"
        )

        if st.button("SALVAR MANUTENÇÃO", type="primary", use_container_width=True):
            if not responsavel_lancamento:
                st.error("Informe o responsável pelo lançamento.")
            elif not placa:
                st.error("Selecione o veículo.")
            elif not tipo_manutencao:
                st.error("Informe o tipo de manutenção.")
            elif km <= 0:
                st.error("Informe o km.")
            elif not servico_executado:
                st.error("Informe o serviço executado.")
            elif not fornecedor:
                st.error("Informe o fornecedor/oficina.")
            elif valor <= 0:
                st.error("Informe o valor.")
            elif not observacoes:
                st.error("Informe as observações.")
            elif not nota_anexo:
                st.error("Anexe a nota.")
            else:
                if tipo_manutencao == "Preventiva":
                    df_frotas_manutencoes = baixar_manutencoes_programadas(df_frotas_manutencoes, placa, data)
                caminho_nota = salvar_anexo_frota(nota_anexo, placa, "manutencao")
                novo = pd.DataFrame([{
                    "data": data.isoformat(),
                    "placa": placa,
                    "tipo_manutencao": tipo_manutencao,
                    "km": int(km),
                    "servico_executado": servico_executado,
                    "fornecedor": fornecedor,
                    "valor": float(valor),
                    "manutencao_agendada": "",
                    "proxima_revisao": "",
                    "status_manutencao": "Executada",
                    "responsavel_lancamento": responsavel_lancamento,
                    "registrado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
                    "nota_anexo": caminho_nota,
                    "status_conferencia": "Pendente",
                    "observacao_administrativo": "",
                    "observacoes": observacoes
                }])
                df_frotas_manutencoes = pd.concat([df_frotas_manutencoes, novo], ignore_index=True)
                df_frotas_manutencoes.to_excel(FROTAS_MANUTENCOES_XLSX, index=False)
                st.session_state["feedback_lancamento"] = {
                    "tipo": "sucesso",
                    "mensagem": "Manutenção salva com sucesso."
                }
                st.rerun()


# =========================
# MENU
# =========================
st.sidebar.title("MENU")
usuario_logado = st.session_state.get("usuario_logado", {})
st.sidebar.caption(f"Usuário logado: {usuario_logado.get('nome', '')} | {usuario_logado.get('nivel', '')}")

if usuario_logado.get("nivel") == "Responsável Frota":
    st.sidebar.divider()
    st.sidebar.markdown("<span class='status-pill'>Acesso restrito</span>", unsafe_allow_html=True)
    if st.sidebar.button("Sair", use_container_width=True):
        st.session_state["autenticado"] = False
        st.session_state.pop("usuario_logado", None)
        st.rerun()
    tela_responsavel_frota()
    st.stop()

supervisor_base_mode = usuario_logado.get("nivel") == "Supervisor Base"

if supervisor_base_mode:
    bases_supervisor = usuario_logado.get("bases_permitidas", [])
    if isinstance(bases_supervisor, str):
        bases_supervisor = [bases_supervisor] if bases_supervisor.strip() else []
    bases_supervisor = [base for base in bases_supervisor if base in BASES_FREQUENCIA]
    base_supervisor = bases_supervisor[0] if bases_supervisor else ""

    st.session_state["menu"] = "BASES"
    st.session_state["base_faltas_selecionada"] = base_supervisor
    opcoes_supervisor = ["MINHA BASE", "LISTA DE FREQUÊNCIA", "ESTOQUE", "DESPESAS FROTAS"]

    escolha_supervisor = st.sidebar.radio(
        "Menu Supervisor",
        opcoes_supervisor,
        label_visibility="collapsed",
        key="menu_supervisor_base"
    )
    subtela_supervisor_atual = st.session_state.get("subtela_faltas", "")
    if escolha_supervisor == "MINHA BASE":
        st.session_state["subtela_faltas"] = ""
    elif escolha_supervisor == "LISTA DE FREQUÊNCIA" and subtela_supervisor_atual in ["COLABORADORES", "RELATORIOS_FREQUENCIA"]:
        st.session_state["subtela_faltas"] = subtela_supervisor_atual
    else:
        st.session_state["subtela_faltas"] = escolha_supervisor
    menu = "BASES"
else:
    if "menu" not in st.session_state:
        st.session_state["menu"] = "INICIO"
    if st.session_state["menu"] == "CADASTRO DE PRODUTOS":
        st.session_state["menu"] = "PRODUTOS"

    modulos_menu = ["INICIO", "ALMOXARIFADO", "BASES", "FROTAS", "PATRIMÔNIO", "ORÇAMENTOS", "CONFIGURAÇÕES"]
    opcoes_almoxarifado = [
        "ESTOQUE",
        "COMPRAS",
        "MOVIMENTAÇÃO",
        "PRODUTOS",
        "CLIENTES",
        "FORNECEDOR",
        "RELATÓRIOS"
    ]

    if "modulo_menu" not in st.session_state:
        if st.session_state["menu"] in opcoes_almoxarifado:
            st.session_state["modulo_menu"] = "ALMOXARIFADO"
        elif st.session_state["menu"] in ["CONTROLE DE FALTAS", "BASES"]:
            st.session_state["modulo_menu"] = "BASES"
        elif st.session_state["menu"] == "FROTAS":
            st.session_state["modulo_menu"] = "FROTAS"
        elif st.session_state["menu"] == "PATRIMÔNIO":
            st.session_state["modulo_menu"] = "PATRIMÔNIO"
        elif st.session_state["menu"] == "ORÇAMENTOS":
            st.session_state["modulo_menu"] = "ORÇAMENTOS"
        elif st.session_state["menu"] == "CONFIGURAÇÕES":
            st.session_state["modulo_menu"] = "CONFIGURAÇÕES"
        else:
            st.session_state["modulo_menu"] = "INICIO"

    modulo = st.sidebar.radio(
        "Menu principal",
        modulos_menu,
        index=modulos_menu.index(st.session_state["modulo_menu"]) if st.session_state["modulo_menu"] in modulos_menu else 0,
        label_visibility="collapsed"
    )
    st.session_state["modulo_menu"] = modulo

    if modulo == "ALMOXARIFADO":
        st.sidebar.caption("Almoxarifado - Estoque Matriz")
        menu_atual = st.session_state["menu"] if st.session_state["menu"] in opcoes_almoxarifado else "ESTOQUE"
        menu = st.sidebar.radio(
            "Opções do almoxarifado",
            opcoes_almoxarifado,
            index=opcoes_almoxarifado.index(menu_atual),
            label_visibility="collapsed"
        )
    elif modulo == "BASES":
        menu = "BASES"
    elif modulo == "FROTAS":
        menu = "FROTAS"
    elif modulo == "PATRIMÔNIO":
        menu = "PATRIMÔNIO"
    elif modulo == "ORÇAMENTOS":
        menu = "ORÇAMENTOS"
    elif modulo == "CONFIGURAÇÕES":
        menu = "CONFIGURAÇÕES"
    else:
        menu = "INICIO"

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
# INICIO
# =========================
if menu == "INICIO":
    alertas_inicio_preventiva = alertas_manutencao_preventiva(df_frotas_manutencoes)
    assinatura_alerta_inicio = assinatura_alertas_preventiva(alertas_inicio_preventiva)
    alerta_inicio_oculto = (
        assinatura_alerta_inicio
        and st.session_state.get("alerta_preventiva_ok") == assinatura_alerta_inicio
    )
    assinatura_lancamentos_inicio = assinatura_conferencia_frotas(df_frotas_abastecimentos, df_frotas_manutencoes)
    alerta_lancamentos_oculto = (
        assinatura_lancamentos_inicio
        and st.session_state.get("alerta_lancamentos_frota_ok") == assinatura_lancamentos_inicio
    )

    if assinatura_alerta_inicio and not alerta_inicio_oculto:
        vencidas_inicio = alertas_inicio_preventiva[alertas_inicio_preventiva["status"] == "Vencida"]
        vencendo_inicio = alertas_inicio_preventiva[alertas_inicio_preventiva["status"] != "Vencida"]
        if not vencidas_inicio.empty:
            placas_vencidas = ", ".join(vencidas_inicio["placa"].astype(str).tolist())
            st.error(f"Manutenção preventiva vencida: {placas_vencidas}. Registrar execução da preventiva para encerrar o alerta.")
        if not vencendo_inicio.empty:
            placas_vencendo = ", ".join(vencendo_inicio["placa"].astype(str).tolist())
            st.warning(f"Manutenção preventiva vencendo em até 10 dias: {placas_vencendo}.")
        st.dataframe(formatar_colunas_tabela(alertas_inicio_preventiva), use_container_width=True, hide_index=True)
        if st.button("OK", type="primary", key="ok_alerta_preventiva_inicio"):
            st.session_state["alerta_preventiva_ok"] = assinatura_alerta_inicio
            st.rerun()

    if assinatura_lancamentos_inicio and not alerta_lancamentos_oculto:
        pend_abast_inicio = int((df_frotas_abastecimentos["status_conferencia"].astype(str) == "Pendente").sum()) if not df_frotas_abastecimentos.empty else 0
        pend_manut_inicio = int((df_frotas_manutencoes["status_conferencia"].astype(str) == "Pendente").sum()) if not df_frotas_manutencoes.empty else 0
        st.warning(
            f"Novos lançamentos de frota aguardando conferência: "
            f"{pend_abast_inicio} abastecimento(s) e {pend_manut_inicio} manutenção(ões)."
        )

        resumo_lancamentos_inicio = []
        if pend_abast_inicio:
            abast_pend = df_frotas_abastecimentos[df_frotas_abastecimentos["status_conferencia"].astype(str) == "Pendente"].copy()
            abast_pend["tipo"] = "Abastecimento"
            abast_pend["valor"] = abast_pend["valor_total"]
            resumo_lancamentos_inicio.append(abast_pend[["tipo", "placa", "responsavel_lancamento", "registrado_em", "valor"]])
        if pend_manut_inicio:
            manut_pend = df_frotas_manutencoes[df_frotas_manutencoes["status_conferencia"].astype(str) == "Pendente"].copy()
            manut_pend["tipo"] = "Manutenção"
            resumo_lancamentos_inicio.append(manut_pend[["tipo", "placa", "responsavel_lancamento", "registrado_em", "valor"]])
        if resumo_lancamentos_inicio:
            resumo_lancamentos_inicio = pd.concat(resumo_lancamentos_inicio, ignore_index=True)
            st.dataframe(formatar_colunas_tabela(resumo_lancamentos_inicio), use_container_width=True, hide_index=True)

        if st.button("OK", type="primary", key="ok_alerta_lancamentos_frota_inicio"):
            st.session_state["alerta_lancamentos_frota_ok"] = assinatura_lancamentos_inicio
            st.rerun()

    imagem_inicio = HOME_IMAGE if os.path.exists(HOME_IMAGE) else HOME_IMAGE_FALLBACK
    if os.path.exists(imagem_inicio):
        st.markdown(
            f"""
            <style>
                body {{
                    overflow: hidden;
                }}
                [data-testid="stAppViewContainer"] {{
                    overflow: hidden;
                }}
                section.main {{
                    overflow: hidden;
                }}
                section.main > div {{
                    padding: 0 !important;
                    max-width: 100% !important;
                }}
                .block-container,
                [data-testid="stMainBlockContainer"] {{
                    padding: 0 !important;
                    max-width: 100% !important;
                }}
            </style>
            <div class="home-fullscreen-lock">
                <img src='data:image/jpeg;base64,{imagem_base64(imagem_inicio)}' class='home-img'>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.warning(f"Imagem não encontrada: {HOME_IMAGE}. Verifique se o caminho está correto e a imagem existe.")


# =========================
# ABA ESTOQUE
# =========================
elif menu == "ESTOQUE":
    st.title("ESTOQUE")

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

        categorias_cadastradas = [item.get("nome", "") for item in categorias_config if item.get("nome", "")]
        categorias_em_produtos = list(df_produtos["categoria"].dropna().unique())
        categorias_lista = ["Todas"] + list(dict.fromkeys(categorias_cadastradas + categorias_em_produtos))
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
            col[7].image(img, use_container_width=True)

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

    for i, row in df.iterrows():
        col = st.columns([1, 2, 1, 1, 1, 3])

        col[0].write(row["codigo"])
        col[1].write(row["produto"])
        col[2].write(int(row["estoque_atual"]))
        col[3].markdown(f"<span style='color:#facc15'><b>{row['estoque_minimo']}</b></span>", unsafe_allow_html=True)
        col[4].markdown(f"<span style='color:#ef4444'><b>{int(row['necessita'])}</b></span>", unsafe_allow_html=True)

        img = os.path.join(PASTA_IMAGENS, str(row["imagem"]))
        if os.path.exists(img):
            col[5].image(img, use_container_width=True)


# =========================
# MOVIMENTACAO
# =========================
elif menu == "MOVIMENTAÇÃO":
    st.title("MOVIMENTAÇÃO")

    if "lista_mov" not in st.session_state:
        st.session_state["lista_mov"] = []

    if "tipo_movimentacao" not in st.session_state:
        st.session_state["tipo_movimentacao"] = "Entrada"
    if "etiquetas_entrada" not in st.session_state:
        st.session_state["etiquetas_entrada"] = []

    st.write("Tipo de Movimentação")
    tipo_col1, tipo_col2, _ = st.columns([1, 1, 6])
    if tipo_col1.button(
        "ENTRADA",
        use_container_width=True,
        type="primary" if st.session_state["tipo_movimentacao"] == "Entrada" else "secondary"
    ):
        st.session_state["tipo_movimentacao"] = "Entrada"
    if tipo_col2.button(
        "SAIDA",
        use_container_width=True,
        type="primary" if st.session_state["tipo_movimentacao"] == "Saída" else "secondary"
    ):
        st.session_state["tipo_movimentacao"] = "Saída"
    tipo = st.session_state["tipo_movimentacao"]
    tipo_e_saida = str(tipo).lower().startswith("sa")
    st.caption(f"Selecionado: {tipo}")
    produtos_opcoes = df_produtos["produto"].dropna().tolist() if not df_produtos.empty else ["Nenhum produto cadastrado"]
    produto_index = 0
    if tipo_e_saida:
        codigo_bipado = st.text_input("Bipar Produto", key="mov_codigo_bipado").strip()
        if codigo_bipado and not df_produtos.empty:
            produto_encontrado = df_produtos[df_produtos["codigo"].astype(str).str.upper() == codigo_bipado.upper()]
            if not produto_encontrado.empty:
                produto_bipado = str(produto_encontrado.iloc[0]["produto"])
                if produto_bipado in produtos_opcoes:
                    produto_index = produtos_opcoes.index(produto_bipado)
                st.success(f"Produto bipado: {produto_bipado}")
            else:
                st.error("Produto nÃ£o encontrado para o cÃ³digo bipado.")
    produto = st.selectbox("Produto", produtos_opcoes, index=produto_index)
    cliente_destino = ""
    observacao_saida = ""
    if tipo_e_saida:
        clientes_ativos = df_clientes[df_clientes["status"] != "Inativo"]["nome_cliente"].dropna().tolist()
        if clientes_ativos:
            cliente_destino = st.selectbox("Cliente de destino", clientes_ativos)
        else:
            st.warning("Cadastre ou ative um cliente antes de registrar uma saída.")
        observacao_saida = st.text_area("Observação", key="mov_saida_observacao").strip()
    qtd = st.number_input("Quantidade", 1)

    col1, col2 = st.columns(2)

    if col1.button("➕ Adicionar"):
        if produto != "Nenhum produto cadastrado":
            if tipo_e_saida and not cliente_destino:
                st.error("Selecione um cliente para registrar a saída.")
            else:
                st.session_state["lista_mov"].append({
                    "produto": produto,
                    "tipo": tipo,
                    "quantidade": qtd,
                    "cliente": cliente_destino,
                    "observacao": observacao_saida,
                    "codigo": str(df_produtos.loc[df_produtos["produto"] == produto, "codigo"].iloc[0]) if not df_produtos[df_produtos["produto"] == produto].empty else ""
                })

    if col2.button("💾 Salvar"):
        if not st.session_state["lista_mov"]:
            st.warning("Adicione pelo menos uma movimentação antes de salvar.")
        else:
            for item in st.session_state["lista_mov"]:
                tipo_salvo = "Saída" if str(item["tipo"]).lower().startswith("sa") else "Entrada"
                nova = pd.DataFrame([{
                    "produto": item["produto"],
                    "tipo": tipo_salvo,
                    "quantidade": item["quantidade"],
                    "data": datetime.now(),
                    "cliente": item.get("cliente", ""),
                    "observacao": item.get("observacao", "")
                }])
                df_mov = pd.concat([df_mov, nova], ignore_index=True)
                if tipo_salvo == "Entrada":
                    st.session_state["etiquetas_entrada"].append({
                        "produto": item["produto"],
                        "codigo": item.get("codigo", ""),
                        "quantidade": item["quantidade"],
                        "data": datetime.now().strftime("%d/%m/%Y %H:%M")
                    })

            df_mov["tipo"] = df_mov["tipo"].astype(str).replace({
                "Saida": "Saída",
                "SaÃ­da": "Saída",
                "saida": "Saída",
                "saída": "Saída",
                "entrada": "Entrada"
            })
            df_mov.to_excel(MOVIMENTACOES_XLSX, index=False)
            st.session_state["lista_mov"] = []
            st.success("Movimentações salvas. Estoque e histórico atualizados.")
            st.rerun()

    st.divider()
    if st.session_state.get("etiquetas_entrada"):
        st.download_button(
            "Gerar Etiquetas Das Entradas",
            data=gerar_pdf_etiquetas(st.session_state["etiquetas_entrada"]),
            file_name="etiquetas_entrada.pdf",
            mime="application/pdf",
            use_container_width=True
        )
        if st.button("Limpar Etiquetas Geradas"):
            st.session_state["etiquetas_entrada"] = []
            st.rerun()

    for item in st.session_state["lista_mov"]:
        destino = f" | Cliente: {item.get('cliente', '')}" if item.get("cliente") else ""
        observacao = f" | Observação: {item.get('observacao', '')}" if item.get("observacao") else ""
        st.write(f"{item['produto']} | {item['tipo']} | {item['quantidade']}{destino}{observacao}")


# =========================
# CADASTRO
# =========================
elif menu == "PRODUTOS":
    st.title("PRODUTOS")

    categorias = [item.get("nome", "") for item in categorias_config] or ["MANUTENÇÃO", "ELÉTRICA", "HIDRÁULICA", "LIMPEZA", "COPA", "JARDINAGEM"]
    unidades = [item.get("nome", "") for item in unidades_config if item.get("nome", "")] or ["UN"]
    fornecedores = df_fornecedores[df_fornecedores["status"] != "Inativo"]["nome_fornecedor"].dropna().tolist()
    fornecedores_opcoes = ["Não informado"] + fornecedores

    if "acao" not in st.session_state:
        st.session_state["acao"] = "Adicionar"

    col1, col2, col3 = st.columns(3)

    if col1.button("➕ Adicionar", type="primary" if st.session_state["acao"] == "Adicionar" else "secondary"):
        st.session_state["acao"] = "Adicionar"

    if col2.button("✏️ Editar", type="primary" if st.session_state["acao"] == "Editar" else "secondary"):
        st.session_state["acao"] = "Editar"

    if col3.button("🗑️ Excluir", type="primary" if st.session_state["acao"] == "Excluir" else "secondary"):
        st.session_state["acao"] = "Excluir"

    acao = st.session_state["acao"]

    if acao == "Adicionar":
        codigo = proximo_codigo_produto()
        st.text_input("Código", value=codigo, disabled=True)
        produto = st.text_input("Produto")
        categoria = st.selectbox("Categoria", categorias)
        estoque_min = st.number_input("Estoque mínimo", 0, value=int(config.get("estoque_minimo_padrao", 1)))
        unidade = st.selectbox("Unidade", unidades)
        valor_unitario = st.number_input("Valor unitário", min_value=0.0, step=0.01, format="%.2f")
        fornecedor = st.selectbox("FORNECEDOR", fornecedores_opcoes, key="produto_fornecedor_add")
        local = st.text_input("Localização")
        imagem_upload = st.file_uploader("Anexar Imagem Do Produto", type=["png", "jpg", "jpeg", "webp"], key="produto_imagem_add")
        imagem_manual = st.text_input("Imagem Atual / Nome Do Arquivo", help="Opcional. Use somente se a imagem já estiver na pasta Imagens Produtos.")

        if st.button("Salvar"):
            imagem = salvar_imagem_produto(imagem_upload, codigo, produto) if imagem_upload else imagem_manual.strip()
            novo = pd.DataFrame([{
                "codigo": codigo,
                "produto": produto,
                "categoria": categoria,
                "estoque_minimo": estoque_min,
                "unidade": unidade,
                "valor_unitario": valor_unitario,
                "fornecedor": "" if fornecedor == "Não informado" else fornecedor,
                "localizacao": local,
                "imagem": imagem
            }])

            df_produtos = pd.concat([df_produtos, novo], ignore_index=True)
            df_produtos.to_excel(PRODUTOS_XLSX, index=False)
            st.success("Adicionado")
            st.rerun()

    elif acao == "Editar":
        if df_produtos.empty:
            st.info("Nenhum produto cadastrado.")
        else:
            prod = st.selectbox("Produto", df_produtos["produto"])
            dados = df_produtos[df_produtos["produto"] == prod].iloc[0]

            codigo = st.text_input("Código", dados["codigo"])
            categoria = st.selectbox("Categoria", categorias, index=categorias.index(dados["categoria"]) if dados["categoria"] in categorias else 0)
            estoque_min = st.number_input("Estoque mínimo", 0, value=int(dados["estoque_minimo"]))
            unidade_atual = dados["unidade"] if dados["unidade"] in unidades else unidades[0]
            unidade = st.selectbox("Unidade", unidades, index=unidades.index(unidade_atual))
            valor_unitario = st.number_input("Valor unitário", min_value=0.0, step=0.01, format="%.2f", value=float(dados["valor_unitario"]) if pd.notna(dados["valor_unitario"]) else 0.0)
            fornecedor_atual = dados["fornecedor"] if dados["fornecedor"] in fornecedores_opcoes else "Não informado"
            fornecedor = st.selectbox("FORNECEDOR", fornecedores_opcoes, index=fornecedores_opcoes.index(fornecedor_atual), key="produto_fornecedor_edit")
            local = st.text_input("Localização", dados["localizacao"])
            imagem_atual = str(dados["imagem"])
            if imagem_atual:
                caminho_imagem_atual = os.path.join(PASTA_IMAGENS, imagem_atual)
                if os.path.exists(caminho_imagem_atual):
                    st.image(caminho_imagem_atual, caption="Imagem atual", width=180)
            imagem_upload = st.file_uploader("Trocar Imagem Do Produto", type=["png", "jpg", "jpeg", "webp"], key="produto_imagem_edit")
            imagem_manual = st.text_input("Imagem Atual / Nome Do Arquivo", imagem_atual)

            if st.button("Salvar Alteração"):
                imagem = salvar_imagem_produto(imagem_upload, codigo, prod) if imagem_upload else imagem_manual.strip()
                df_produtos.loc[df_produtos["produto"] == prod, "codigo"] = codigo
                df_produtos.loc[df_produtos["produto"] == prod, "categoria"] = categoria
                df_produtos.loc[df_produtos["produto"] == prod, "estoque_minimo"] = estoque_min
                df_produtos.loc[df_produtos["produto"] == prod, "unidade"] = unidade
                df_produtos.loc[df_produtos["produto"] == prod, "valor_unitario"] = valor_unitario
                df_produtos.loc[df_produtos["produto"] == prod, "fornecedor"] = "" if fornecedor == "Não informado" else fornecedor
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
# CLIENTES
# =========================
elif menu == "CLIENTES":
    st.title("CLIENTES")

    if "acao_cliente" not in st.session_state:
        st.session_state["acao_cliente"] = "Adicionar"
    if "cliente_selecionado_codigo" not in st.session_state:
        st.session_state["cliente_selecionado_codigo"] = ""

    col1, col2, col3, col4 = st.columns(4)

    if col1.button("➕ Adicionar", key="cliente_adicionar", type="primary" if st.session_state["acao_cliente"] == "Adicionar" else "secondary"):
        st.session_state["acao_cliente"] = "Adicionar"

    if col2.button("✏️ Editar", key="cliente_editar", type="primary" if st.session_state["acao_cliente"] == "Editar" else "secondary"):
        st.session_state["acao_cliente"] = "Editar"

    if col3.button("🗑️ Excluir", key="cliente_excluir", type="primary" if st.session_state["acao_cliente"] == "Excluir" else "secondary"):
        st.session_state["acao_cliente"] = "Excluir"

    if col4.button("⛔ Inativar", key="cliente_inativar", type="primary" if st.session_state["acao_cliente"] == "Inativar" else "secondary"):
        st.session_state["acao_cliente"] = "Inativar"

    acao_cliente = st.session_state["acao_cliente"]

    if acao_cliente == "Adicionar":
        codigo = proximo_codigo_cliente()
        st.text_input("CÓDIGO", value=codigo, disabled=True)
        nome_cliente = st.text_input("NOME DO CLIENTE")
        telefone = st.text_input("TELEFONE")
        cidade = st.text_input("CIDADE")
        estado = st.text_input("ESTADO")
        tipo_contrato = st.radio("TEMPO DE CONTRATO", ["Período definido", "Prazo indeterminado"], horizontal=True)

        data_inicial, data_final = "", ""
        if tipo_contrato == "Período definido":
            c_data1, c_data2 = st.columns(2)
            data_inicial = c_data1.date_input("DATA INICIAL")
            data_final = c_data2.date_input("DATA FINAL")

        if st.button("Salvar cliente"):
            novo = pd.DataFrame([{
                "codigo": codigo,
                "nome_cliente": nome_cliente,
                "telefone": telefone,
                "cidade": cidade,
                "estado": estado,
                "tipo_contrato": tipo_contrato,
                "data_inicial": data_inicial,
                "data_final": data_final,
                "status": "Ativo"
            }])

            df_clientes = pd.concat([df_clientes, novo], ignore_index=True)
            df_clientes.to_excel(CLIENTES_XLSX, index=False)
            st.success("Cliente adicionado")

    elif acao_cliente == "Editar":
        if df_clientes.empty:
            st.info("Nenhum cliente cadastrado.")
        else:
            codigos_clientes = df_clientes["codigo"].astype(str).tolist()
            nomes_por_codigo = dict(zip(codigos_clientes, df_clientes["nome_cliente"].astype(str)))
            codigo_preselecionado = str(st.session_state.get("cliente_selecionado_codigo", ""))
            indice_preselecionado = codigos_clientes.index(codigo_preselecionado) if codigo_preselecionado in codigos_clientes else 0
            cliente_codigo = st.selectbox(
                "Cliente",
                codigos_clientes,
                index=indice_preselecionado,
                format_func=lambda cod: f"{cod} - {nomes_por_codigo.get(cod, '')}",
                key="cliente_editar_codigo"
            )
            dados = df_clientes[df_clientes["codigo"].astype(str) == str(cliente_codigo)].iloc[0]

            codigo = st.text_input("CÓDIGO", dados["codigo"], disabled=True)
            nome_cliente = st.text_input("NOME DO CLIENTE", dados["nome_cliente"])
            telefone = st.text_input("TELEFONE", dados["telefone"])
            cidade = st.text_input("CIDADE", dados["cidade"])
            estado = st.text_input("ESTADO", dados["estado"])
            status = st.selectbox("STATUS", ["Ativo", "Inativo"], index=0 if dados["status"] == "Ativo" else 1)
            tipo_padrao = dados["tipo_contrato"] if dados["tipo_contrato"] in ["Período definido", "Prazo indeterminado"] else "Período definido"
            tipo_contrato = st.radio("TEMPO DE CONTRATO", ["Período definido", "Prazo indeterminado"], index=0 if tipo_padrao == "Período definido" else 1, horizontal=True)

            data_inicial, data_final = "", ""
            if tipo_contrato == "Período definido":
                c_data1, c_data2 = st.columns(2)
                data_inicial_padrao = pd.to_datetime(dados["data_inicial"], errors="coerce")
                data_final_padrao = pd.to_datetime(dados["data_final"], errors="coerce")
                data_inicial = c_data1.date_input("DATA INICIAL", value=data_inicial_padrao.date() if pd.notna(data_inicial_padrao) else datetime.now().date())
                data_final = c_data2.date_input("DATA FINAL", value=data_final_padrao.date() if pd.notna(data_final_padrao) else datetime.now().date())

            if st.button("Salvar alteração do cliente"):
                linha_cliente = df_clientes["codigo"].astype(str) == str(cliente_codigo)
                df_clientes.loc[linha_cliente, "codigo"] = str(codigo)
                df_clientes.loc[linha_cliente, "nome_cliente"] = str(nome_cliente)
                df_clientes.loc[linha_cliente, "telefone"] = str(telefone)
                df_clientes.loc[linha_cliente, "cidade"] = str(cidade)
                df_clientes.loc[linha_cliente, "estado"] = str(estado)
                df_clientes.loc[linha_cliente, "tipo_contrato"] = str(tipo_contrato)
                df_clientes.loc[linha_cliente, "data_inicial"] = str(data_inicial)
                df_clientes.loc[linha_cliente, "data_final"] = str(data_final)
                df_clientes.loc[linha_cliente, "status"] = str(status)
                df_clientes.to_excel(CLIENTES_XLSX, index=False)
                st.success("Cliente atualizado")

    elif acao_cliente == "Excluir":
        if df_clientes.empty:
            st.info("Nenhum cliente cadastrado.")
        else:
            cliente = st.selectbox("Cliente", df_clientes["nome_cliente"])

            if st.button("Excluir cliente"):
                df_clientes = df_clientes[df_clientes["nome_cliente"] != cliente]
                df_clientes.to_excel(CLIENTES_XLSX, index=False)
                st.success("Cliente excluído")

    elif acao_cliente == "Inativar":
        clientes_ativos = df_clientes[df_clientes["status"] != "Inativo"].copy()
        if clientes_ativos.empty:
            st.info("Nenhum cliente ativo para inativar.")
        else:
            codigos_ativos = clientes_ativos["codigo"].astype(str).tolist()
            nomes_ativos_por_codigo = dict(zip(codigos_ativos, clientes_ativos["nome_cliente"].astype(str)))
            codigo_preselecionado = str(st.session_state.get("cliente_selecionado_codigo", ""))
            indice_preselecionado = codigos_ativos.index(codigo_preselecionado) if codigo_preselecionado in codigos_ativos else 0
            cliente_codigo = st.selectbox(
                "Cliente",
                codigos_ativos,
                index=indice_preselecionado,
                format_func=lambda cod: f"{cod} - {nomes_ativos_por_codigo.get(cod, '')}",
                key="cliente_inativar_codigo"
            )

            if st.button("Inativar cliente"):
                df_clientes.loc[df_clientes["codigo"].astype(str) == str(cliente_codigo), "status"] = "Inativo"
                df_clientes.to_excel(CLIENTES_XLSX, index=False)
                st.success("Cliente inativado")

    st.divider()
    st.subheader("Clientes cadastrados")
    st.dataframe(df_clientes, use_container_width=True)

    if not df_clientes.empty:
        st.caption("Selecione um cliente cadastrado para editar ou inativar.")
        codigos_lista = df_clientes["codigo"].astype(str).tolist()
        nomes_lista_por_codigo = dict(zip(codigos_lista, df_clientes["nome_cliente"].astype(str)))
        codigo_preselecionado = str(st.session_state.get("cliente_selecionado_codigo", ""))
        indice_preselecionado = codigos_lista.index(codigo_preselecionado) if codigo_preselecionado in codigos_lista else 0
        cliente_lista_codigo = st.selectbox(
            "Cliente selecionado",
            codigos_lista,
            index=indice_preselecionado,
            format_func=lambda cod: f"{cod} - {nomes_lista_por_codigo.get(cod, '')}",
            key="cliente_lista_codigo"
        )
        col_editar_cliente, col_inativar_cliente = st.columns(2)
        if col_editar_cliente.button("Editar cliente", key="cliente_lista_editar", use_container_width=True):
            st.session_state["cliente_selecionado_codigo"] = str(cliente_lista_codigo)
            st.session_state["acao_cliente"] = "Editar"
            st.rerun()
        if col_inativar_cliente.button("Inativar cliente", key="cliente_lista_inativar", use_container_width=True):
            st.session_state["cliente_selecionado_codigo"] = str(cliente_lista_codigo)
            st.session_state["acao_cliente"] = "Inativar"
            st.rerun()


# =========================
# FORNECEDOR
# =========================
elif menu == "FORNECEDOR":
    st.title("FORNECEDOR")

    if "acao_fornecedor" not in st.session_state:
        st.session_state["acao_fornecedor"] = "Adicionar"

    col1, col2, col3, col4 = st.columns(4)

    if col1.button("➕ Adicionar", key="fornecedor_adicionar", type="primary" if st.session_state["acao_fornecedor"] == "Adicionar" else "secondary"):
        st.session_state["acao_fornecedor"] = "Adicionar"

    if col2.button("✏️ Editar", key="fornecedor_editar", type="primary" if st.session_state["acao_fornecedor"] == "Editar" else "secondary"):
        st.session_state["acao_fornecedor"] = "Editar"

    if col3.button("🗑️ Excluir", key="fornecedor_excluir", type="primary" if st.session_state["acao_fornecedor"] == "Excluir" else "secondary"):
        st.session_state["acao_fornecedor"] = "Excluir"

    if col4.button("⛔ Inativar", key="fornecedor_inativar", type="primary" if st.session_state["acao_fornecedor"] == "Inativar" else "secondary"):
        st.session_state["acao_fornecedor"] = "Inativar"

    acao_fornecedor = st.session_state["acao_fornecedor"]

    if acao_fornecedor == "Adicionar":
        codigo = proximo_codigo_fornecedor()
        st.text_input("CÓDIGO", value=codigo, disabled=True)
        nome_fornecedor = st.text_input("NOME DO FORNECEDOR")
        telefone = st.text_input("TELEFONE")
        cidade = st.text_input("CIDADE")
        estado = st.text_input("ESTADO")

        if st.button("Salvar fornecedor"):
            novo = pd.DataFrame([{
                "codigo": codigo,
                "nome_fornecedor": nome_fornecedor,
                "telefone": telefone,
                "cidade": cidade,
                "estado": estado,
                "tipo_contrato": "",
                "data_inicial": "",
                "data_final": "",
                "status": "Ativo"
            }])

            df_fornecedores = pd.concat([df_fornecedores, novo], ignore_index=True)
            df_fornecedores.to_excel(FORNECEDORES_XLSX, index=False)
            st.success("Fornecedor adicionado")

    elif acao_fornecedor == "Editar":
        if df_fornecedores.empty:
            st.info("Nenhum fornecedor cadastrado.")
        else:
            fornecedor_sel = st.selectbox("Fornecedor", df_fornecedores["nome_fornecedor"])
            dados = df_fornecedores[df_fornecedores["nome_fornecedor"] == fornecedor_sel].iloc[0]

            codigo = st.text_input("CÓDIGO", dados["codigo"], disabled=True)
            nome_fornecedor = st.text_input("NOME DO FORNECEDOR", dados["nome_fornecedor"])
            telefone = st.text_input("TELEFONE", dados["telefone"])
            cidade = st.text_input("CIDADE", dados["cidade"])
            estado = st.text_input("ESTADO", dados["estado"])
            status = st.selectbox("STATUS", ["Ativo", "Inativo"], index=0 if dados["status"] == "Ativo" else 1)

            if st.button("Salvar alteração do fornecedor"):
                linha_fornecedor = df_fornecedores["nome_fornecedor"] == fornecedor_sel
                df_fornecedores.loc[linha_fornecedor, "codigo"] = str(codigo)
                df_fornecedores.loc[linha_fornecedor, "nome_fornecedor"] = str(nome_fornecedor)
                df_fornecedores.loc[linha_fornecedor, "telefone"] = str(telefone)
                df_fornecedores.loc[linha_fornecedor, "cidade"] = str(cidade)
                df_fornecedores.loc[linha_fornecedor, "estado"] = str(estado)
                df_fornecedores.loc[linha_fornecedor, "status"] = str(status)
                df_fornecedores.to_excel(FORNECEDORES_XLSX, index=False)
                st.success("Fornecedor atualizado")

    elif acao_fornecedor == "Excluir":
        if df_fornecedores.empty:
            st.info("Nenhum fornecedor cadastrado.")
        else:
            fornecedor_sel = st.selectbox("Fornecedor", df_fornecedores["nome_fornecedor"])

            if st.button("Excluir fornecedor"):
                df_fornecedores = df_fornecedores[df_fornecedores["nome_fornecedor"] != fornecedor_sel]
                df_fornecedores.to_excel(FORNECEDORES_XLSX, index=False)
                st.success("Fornecedor excluído")

    elif acao_fornecedor == "Inativar":
        fornecedores_ativos = df_fornecedores[df_fornecedores["status"] != "Inativo"].copy()
        if fornecedores_ativos.empty:
            st.info("Nenhum fornecedor ativo para inativar.")
        else:
            fornecedor_sel = st.selectbox("Fornecedor", fornecedores_ativos["nome_fornecedor"])

            if st.button("Inativar fornecedor"):
                df_fornecedores.loc[df_fornecedores["nome_fornecedor"] == fornecedor_sel, "status"] = "Inativo"
                df_fornecedores.to_excel(FORNECEDORES_XLSX, index=False)
                st.success("Fornecedor inativado")

    st.divider()
    st.subheader("Fornecedores cadastrados")
    st.dataframe(df_fornecedores, use_container_width=True)


# =========================
# BASES
# =========================
elif menu in ["CONTROLE DE FALTAS", "BASES"]:
    st.title("BASES")

    presenca_opcoes = ["PRESENTE", "FALTOU", "FALTA MEIO PERIODO", "ATESTADO", "FOLGA", "FÉRIAS", "FERIADO"]
    almoco_opcoes = ["Sim", "Não"]
    escala_opcoes = ["SEGUNDA A SEXTA", "12X36"]
    funcao_opcoes = [
        "JARDINEIRO FIXO",
        "JARDINEIRO TEMPORARIO",
        "SUPERVISOR OPERACIONAL",
        "PORTARIA",
        "OFICIAL DE MANUTENÇÃO",
        "ELETRICISTA BAIXA TENSÃO",
        "SERVENTE DE LIMPEZA"
    ]
    if "base_faltas_selecionada" not in st.session_state:
        st.session_state["base_faltas_selecionada"] = ""

    if supervisor_base_mode and not st.session_state["base_faltas_selecionada"]:
        st.error("Nenhuma base foi liberada para este supervisor. Ajuste em Configurações > Usuários.")
        st.stop()

    if not supervisor_base_mode and not st.session_state["base_faltas_selecionada"]:
        st.subheader("Selecione A Base")
        portal_cols = st.columns(2)
        nomes_botoes_base = {
            "TMG BASE SORRISO": "BASE TMG SORRISO",
            "TMG BASE RONDONOPOLIS": "BASE TMG RONDONOPOLIS"
        }
        for idx_base, nome_base in enumerate(BASES_FREQUENCIA):
            if portal_cols[idx_base].button(nomes_botoes_base.get(nome_base, nome_base), use_container_width=True, key=f"portal_base_{nome_base}"):
                if usuario_pode_acessar_base(usuario_logado, nome_base):
                    st.session_state["base_faltas_selecionada"] = nome_base
                    st.session_state["subtela_faltas"] = ""
                    st.rerun()
                else:
                    st.session_state["erro_permissao_base"] = nome_base
                    st.rerun()
        if st.session_state.get("erro_permissao_base"):
            st.error(f"Você não tem permissão para acessar {st.session_state['erro_permissao_base']}.")
        st.stop()

    base_faltas_atual = st.session_state["base_faltas_selecionada"]
    if not usuario_pode_acessar_base(usuario_logado, base_faltas_atual):
        st.session_state["base_faltas_selecionada"] = ""
        st.error("Você não tem permissão para acessar esta base.")
        st.stop()

    topo_base, topo_voltar = st.columns([4, 1])
    topo_base.subheader(base_faltas_atual)
    if not supervisor_base_mode:
        if topo_voltar.button("Trocar Base", use_container_width=True, key="trocar_base_portal"):
            st.session_state["base_faltas_selecionada"] = ""
            st.rerun()

    supervisor_atual = config.get("supervisores_frequencia", {}).get(base_faltas_atual, "")

    faltas_base = df_faltas[df_faltas["base_frequencia"] == base_faltas_atual].copy()
    faltas_base["data_dt"] = pd.to_datetime(faltas_base["data"], errors="coerce")
    df_colaboradores_base = colaboradores_frequencia(faltas_base)
    df_colaboradores_ativos = df_colaboradores_base[df_colaboradores_base["status_colaborador"] != "Inativo"].copy()
    colaboradores = df_colaboradores_ativos["colaborador"].tolist()
    todos_colaboradores = df_colaboradores_base["colaborador"].tolist()
    funcoes_por_colaborador = df_colaboradores_ativos.set_index("colaborador")["funcao"].to_dict() if not df_colaboradores_ativos.empty else {}
    colunas_escala_ocultas = ["tipo_escala", "data_base_escala", "trabalha_data_base", "base_frequencia"]

    if "subtela_faltas" not in st.session_state:
        st.session_state["subtela_faltas"] = ""
    subtelas_antigas_bases = {
        "PAINEL": "LISTA DE FREQUÊNCIA",
        "LANÇAR PRESENÇA": "LISTA DE FREQUÊNCIA",
        "EDITAR LANÇAMENTO": "LISTA DE FREQUÊNCIA",
        "ESTOQUE DA BASE": "ESTOQUE",
        "RELATORIOS FREQUENCIA": "RELATORIOS_FREQUENCIA",
        "RELATÓRIOS FREQUÊNCIA": "RELATORIOS_FREQUENCIA",
        "RELATÓRIOS": "RELATORIOS_FREQUENCIA",
        "DESPESAS DE FROTA": "DESPESAS FROTAS",
        "DESPESAS FROTA": "DESPESAS FROTAS"
    }
    if st.session_state["subtela_faltas"] in subtelas_antigas_bases:
        st.session_state["subtela_faltas"] = subtelas_antigas_bases[st.session_state["subtela_faltas"]]

    subtela_faltas = st.session_state["subtela_faltas"]

    if not subtela_faltas and supervisor_base_mode:
        hoje_base = datetime.now().date()
        faltas_mes_base = faltas_base[
            (faltas_base["data_dt"].notna()) &
            (faltas_base["data_dt"].dt.month == hoje_base.month) &
            (faltas_base["data_dt"].dt.year == hoje_base.year) &
            (faltas_base["presenca"].astype(str).str.upper().isin(["FALTOU", "FALTA MEIO PERIODO"]))
        ].copy()
        total_faltas_mes = int(len(faltas_mes_base))
        total_registros = int(len(faltas_base))
        total_presentes = int((faltas_base["presenca"].astype(str).str.upper() == "PRESENTE").sum()) if not faltas_base.empty else 0
        estoque_base_resumo = calcular_estoque_base(df_bases_movimentacoes, base_faltas_atual)
        produtos_base_info_alerta = df_produtos[["produto", "estoque_minimo"]].copy() if not df_produtos.empty else pd.DataFrame(columns=["produto", "estoque_minimo"])
        estoque_base_alerta = estoque_base_resumo.merge(produtos_base_info_alerta, on="produto", how="left") if not estoque_base_resumo.empty else pd.DataFrame(columns=["produto", "estoque_atual", "estoque_minimo"])
        estoque_base_alerta["estoque_minimo"] = pd.to_numeric(estoque_base_alerta.get("estoque_minimo", 0), errors="coerce").fillna(0)
        estoque_base_alerta["estoque_atual"] = pd.to_numeric(estoque_base_alerta.get("estoque_atual", 0), errors="coerce").fillna(0)
        total_produtos_criticos = int((estoque_base_alerta["estoque_atual"] <= estoque_base_alerta["estoque_minimo"]).sum()) if not estoque_base_alerta.empty else 0

        data_hoje_texto = hoje_base.isoformat()
        registros_hoje = faltas_base[faltas_base["data"].astype(str) == data_hoje_texto]["colaborador"].astype(str).str.upper().nunique()
        total_colaboradores_ativos_base = len(df_colaboradores_ativos)
        frequencia_ok = total_colaboradores_ativos_base > 0 and registros_hoje >= total_colaboradores_ativos_base
        status_frequencia = "OK" if frequencia_ok else "Pendente"
        cor_frequencia = "#22c55e" if frequencia_ok else "#f59e0b"
        detalhe_frequencia = f"{registros_hoje}/{total_colaboradores_ativos_base} lançados hoje"

        pend_abastecimentos_base = int((df_frotas_abastecimentos["status_conferencia"].astype(str) == "Pendente").sum()) if not df_frotas_abastecimentos.empty else 0
        pend_manutencoes_base = int((df_frotas_manutencoes["status_conferencia"].astype(str) == "Pendente").sum()) if not df_frotas_manutencoes.empty else 0
        total_despesas_veiculo_pendentes = pend_abastecimentos_base + pend_manutencoes_base

        r1, r2, r3, r4 = st.columns(4)
        r1.markdown(f"<div class='metric-card'><div class='metric-label'>Alerta De Faltas No Mês</div><div class='metric-value' style='color:#ef4444'>{total_faltas_mes}</div><div class='metric-label'>Faltas registradas</div></div>", unsafe_allow_html=True)
        r2.markdown(f"<div class='metric-card'><div class='metric-label'>Produtos Críticos</div><div class='metric-value' style='color:#f59e0b'>{total_produtos_criticos}</div><div class='metric-label'>Abaixo ou igual ao mínimo</div></div>", unsafe_allow_html=True)
        r3.markdown(f"<div class='metric-card'><div class='metric-label'>Lançamento De Frequência</div><div class='metric-value' style='color:{cor_frequencia}'>{status_frequencia}</div><div class='metric-label'>{detalhe_frequencia}</div></div>", unsafe_allow_html=True)
        r4.markdown(f"<div class='metric-card'><div class='metric-label'>Despesas Com Veículo Pendentes</div><div class='metric-value' style='color:#f59e0b'>{total_despesas_veiculo_pendentes}</div><div class='metric-label'>Abast. {pend_abastecimentos_base} | Manut. {pend_manutencoes_base}</div></div>", unsafe_allow_html=True)
        st.info("Use o menu lateral para acessar Lista De Frequência, Estoque ou Despesas Frota.")

    elif not subtela_faltas and not supervisor_base_mode:
        abas_faltas = ["LISTA DE FREQUÊNCIA", "ESTOQUE", "DESPESAS FROTAS"]
        nav_cols = st.columns(len(abas_faltas))
        for idx_nav, nome_aba in enumerate(abas_faltas):
            if nav_cols[idx_nav].button(
                nome_aba,
                type="primary",
                use_container_width=True,
                key=f"faltas_nav_{nome_aba}"
            ):
                st.session_state["subtela_faltas"] = nome_aba
                st.rerun()

    elif subtela_faltas == "PAINEL":
        st.subheader("Painel")
        resumo_status = pd.DataFrame(columns=["presenca", "quantidade"])
        if not faltas_base.empty:
            resumo_status = faltas_base["presenca"].replace("", "NÃO LANÇADO").value_counts().reset_index()
            resumo_status.columns = ["presenca", "quantidade"]

        col_painel1, col_painel2 = st.columns(2)
        with col_painel1:
            st.write("Resumo por presença")
            st.dataframe(formatar_colunas_tabela(resumo_status), use_container_width=True)
        with col_painel2:
            resumo_funcao = pd.DataFrame(columns=["funcao", "almocos"])
            if not faltas_base.empty:
                resumo_funcao = faltas_base[faltas_base["almocou_base"].astype(str).str.upper() == "SIM"].groupby("funcao").size().reset_index(name="almocos")
            st.write("Almoços por função")
            st.dataframe(formatar_colunas_tabela(resumo_funcao), use_container_width=True)

        st.subheader("Últimos lançamentos")
        ultimos = faltas_base.drop(columns=["data_dt"] + colunas_escala_ocultas, errors="ignore").tail(50)
        st.dataframe(formatar_colunas_tabela(ultimos), use_container_width=True)

    elif subtela_faltas == "LISTA DE FREQUÊNCIA":
        if not supervisor_base_mode and st.button("Voltar", use_container_width=True, key="voltar_base_lista"):
            st.session_state["subtela_faltas"] = ""
            st.rerun()
        acao_lista_cols = st.columns(3)
        if acao_lista_cols[0].button("Lançar Presença", type="primary", use_container_width=True, key="acao_lancar_presenca_base"):
            st.session_state["subtela_faltas"] = "LISTA DE FREQUÊNCIA"
            st.rerun()
        if acao_lista_cols[1].button("Cadastrar / Editar / Inativar Colaboradores", use_container_width=True, key="acao_colaboradores_base"):
            st.session_state["subtela_faltas"] = "COLABORADORES"
            st.rerun()
        if acao_lista_cols[2].button("Painel De Relatórios", use_container_width=True, key="acao_relatorios_frequencia_base"):
            st.session_state["subtela_faltas"] = "RELATORIOS_FREQUENCIA"
            st.rerun()
        st.divider()
        meses_pt = [
            "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
            "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
        ]
        hoje_calendario = datetime.now().date()
        cal_mes_col, cal_ano_col = st.columns([2, 1])
        mes_calendario = cal_mes_col.selectbox(
            "Mês do calendário",
            list(range(1, 13)),
            index=hoje_calendario.month - 1,
            format_func=lambda mes: meses_pt[mes - 1],
            key=f"calendario_frequencia_mes_{base_faltas_atual}"
        )
        ano_calendario = int(cal_ano_col.number_input(
            "Ano",
            min_value=2020,
            max_value=2100,
            value=hoje_calendario.year,
            step=1,
            key=f"calendario_frequencia_ano_{base_faltas_atual}"
        ))
        total_colaboradores_ativos = len(df_colaboradores_ativos)
        faltas_mes = faltas_base[
            (faltas_base["data_dt"].notna()) &
            (faltas_base["data_dt"].dt.month == mes_calendario) &
            (faltas_base["data_dt"].dt.year == ano_calendario)
        ].copy()
        lancamentos_por_dia = {}
        if not faltas_mes.empty:
            lancamentos_por_dia = faltas_mes.groupby(faltas_mes["data_dt"].dt.day)["colaborador"].nunique().to_dict()

        semanas_mes = calendar.monthcalendar(ano_calendario, mes_calendario)
        dias_semana = ["SEG", "TER", "QUA", "QUI", "SEX", "SAB", "DOM"]
        linhas_calendario = ""
        for semana in semanas_mes:
            celulas_semana = ""
            for dia in semana:
                if dia == 0:
                    celulas_semana += "<div class='cal-dia cal-vazio'></div>"
                    continue
                qtd_lancada = int(lancamentos_por_dia.get(dia, 0))
                if qtd_lancada <= 0:
                    classe_status = "cal-sem-lancamento"
                    texto_status = "Sem lançamento"
                elif total_colaboradores_ativos > 0 and qtd_lancada >= total_colaboradores_ativos:
                    classe_status = "cal-completo"
                    texto_status = "Completo"
                else:
                    classe_status = "cal-parcial"
                    texto_status = "Parcial"
                celulas_semana += (
                    f"<div class='cal-dia {classe_status}'>"
                    f"<strong>{dia}</strong>"
                    f"<span>{texto_status}</span>"
                    f"<small>{qtd_lancada}/{total_colaboradores_ativos}</small>"
                    "</div>"
                )
            linhas_calendario += f"<div class='cal-linha'>{celulas_semana}</div>"

        cabecalho_calendario = "".join([f"<div class='cal-semana'>{dia}</div>" for dia in dias_semana])
        st.markdown(
            f"""
            <style>
                .calendario-frequencia {{
                    background: linear-gradient(180deg, rgba(15,23,42,.96), rgba(17,24,39,.96));
                    border: 1px solid rgba(148,163,184,.22);
                    border-radius: 8px;
                    padding: 14px;
                    margin: 8px 0 18px 0;
                }}
                .cal-titulo {{
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 12px;
                    color: #f8fafc;
                    font-weight: 800;
                    margin-bottom: 12px;
                }}
                .cal-legenda {{
                    display: flex;
                    gap: 10px;
                    flex-wrap: wrap;
                    color: #cbd5e1;
                    font-size: 12px;
                    font-weight: 700;
                }}
                .cal-legenda span {{
                    display: inline-flex;
                    align-items: center;
                    gap: 6px;
                }}
                .cal-bolinha {{
                    width: 10px;
                    height: 10px;
                    border-radius: 999px;
                    display: inline-block;
                }}
                .cal-grid, .cal-linha {{
                    display: grid;
                    grid-template-columns: repeat(7, minmax(0, 1fr));
                    gap: 7px;
                }}
                .cal-grid {{
                    margin-bottom: 7px;
                }}
                .cal-semana {{
                    color: #94a3b8;
                    font-size: 12px;
                    font-weight: 800;
                    text-align: center;
                }}
                .cal-linha {{
                    margin-bottom: 7px;
                }}
                .cal-dia {{
                    min-height: 76px;
                    border-radius: 8px;
                    padding: 9px;
                    display: flex;
                    flex-direction: column;
                    justify-content: space-between;
                    border: 1px solid rgba(255,255,255,.12);
                    box-shadow: inset 0 1px 0 rgba(255,255,255,.16), 0 8px 14px rgba(0,0,0,.18);
                    overflow: hidden;
                }}
                .cal-dia strong {{
                    color: #fff;
                    font-size: 18px;
                    line-height: 1;
                }}
                .cal-dia span, .cal-dia small {{
                    color: rgba(255,255,255,.92);
                    font-size: 11px;
                    font-weight: 800;
                    line-height: 1.15;
                }}
                .cal-vazio {{
                    background: transparent;
                    border-color: transparent;
                    box-shadow: none;
                }}
                .cal-sem-lancamento {{
                    background: #1f2937;
                }}
                .cal-parcial {{
                    background: linear-gradient(180deg, #f59e0b, #b45309);
                }}
                .cal-completo {{
                    background: linear-gradient(180deg, #22c55e, #15803d);
                }}
                @media (max-width: 700px) {{
                    .calendario-frequencia {{
                        padding: 10px;
                    }}
                    .cal-grid, .cal-linha {{
                        gap: 4px;
                    }}
                    .cal-dia {{
                        min-height: 58px;
                        padding: 6px;
                    }}
                    .cal-dia strong {{
                        font-size: 15px;
                    }}
                    .cal-dia span {{
                        display: none;
                    }}
                    .cal-dia small {{
                        font-size: 10px;
                    }}
                }}
            </style>
            <div class="calendario-frequencia">
                <div class="cal-titulo">
                    <div>{meses_pt[mes_calendario - 1]} / {ano_calendario}</div>
                    <div class="cal-legenda">
                        <span><i class="cal-bolinha" style="background:#22c55e"></i>Completo</span>
                        <span><i class="cal-bolinha" style="background:#f59e0b"></i>Parcial</span>
                        <span><i class="cal-bolinha" style="background:#1f2937;border:1px solid rgba(255,255,255,.28)"></i>Sem lançamento</span>
                    </div>
                </div>
                <div class="cal-grid">{cabecalho_calendario}</div>
                {linhas_calendario}
            </div>
            """,
            unsafe_allow_html=True
        )
        data_registro = st.date_input("DATA", value=datetime.now().date())
        feriado_lancamento = st.checkbox("Feriado", value=False)

        if df_colaboradores_ativos.empty:
            st.info("Cadastre colaboradores ativos antes de lançar presença.")
        else:
            data_texto = data_registro.isoformat()
            estado_lancamento = f"{data_texto}_{int(feriado_lancamento)}"
            registros_existentes = faltas_base[faltas_base["data"].astype(str) == data_texto]["colaborador"].astype(str).str.upper().tolist()
            linhas_lancamento = []
            df_colaboradores_lancamento = df_colaboradores_ativos.sort_values("colaborador", kind="stable").reset_index(drop=True)
            for _, colaborador_row in df_colaboradores_lancamento.iterrows():
                colaborador_nome = str(colaborador_row["colaborador"]).strip().upper()
                presenca_prevista = status_previsto_escala(
                    data_registro,
                    colaborador_row.get("tipo_escala", "SEGUNDA A SEXTA"),
                    colaborador_row.get("data_base_escala", ""),
                    colaborador_row.get("trabalha_data_base", "Sim"),
                    feriado_lancamento
                )
                almoco_previsto = "Sim" if presenca_prevista == "PRESENTE" else "Não"
                linhas_lancamento.append({
                    "lançar": colaborador_nome not in registros_existentes,
                    "colaborador": colaborador_nome,
                    "funcao": str(colaborador_row.get("funcao", "")).strip().upper(),
                    "presenca": presenca_prevista,
                    "motivo_falta": "",
                    "almocou_base": almoco_previsto,
                    "observacoes": ""
                })

            presenca_todos = st.selectbox(
                "APLICAR PRESENÇA PARA TODOS",
                ["Manter sugestão"] + presenca_opcoes,
                key=f"presenca_todos_{estado_lancamento}"
            )
            df_lancamento_base = pd.DataFrame(linhas_lancamento).sort_values("colaborador", kind="stable").reset_index(drop=True)
            if presenca_todos != "Manter sugestão":
                df_lancamento_base["presenca"] = presenca_todos
                df_lancamento_base["almocou_base"] = "Sim" if presenca_todos == "PRESENTE" else "Não"

            st.caption("Use os campos de seleção para escolher a presença. Não é necessário digitar o status.")
            headers = st.columns([0.7, 2.2, 1.7, 1.7, 2.2, 1.3, 2.2])
            headers[0].write("Lançar")
            headers[1].write("Colaborador")
            headers[2].write("Função")
            headers[3].write("Presença")
            headers[4].write("Motivo da falta")
            headers[5].write("Almoço")
            headers[6].write("Observações")

            linhas_editadas = []
            for idx_lanc, row in df_lancamento_base.iterrows():
                row_cols = st.columns([0.7, 2.2, 1.7, 1.7, 2.2, 1.3, 2.2])
                colaborador_nome = str(row["colaborador"])
                presenca_atual = str(row["presenca"]).upper()
                almoco_atual = str(row["almocou_base"]).capitalize()
                lancar = row_cols[0].checkbox(
                    "Lançar",
                    value=bool(row["lançar"]),
                    key=f"faltas_lancar_{estado_lancamento}_{idx_lanc}",
                    label_visibility="collapsed"
                )
                row_cols[1].write(colaborador_nome)
                row_cols[2].write(str(row["funcao"]))
                presenca_linha = row_cols[3].selectbox(
                    "Presença",
                    presenca_opcoes,
                    index=presenca_opcoes.index(presenca_atual) if presenca_atual in presenca_opcoes else 0,
                    key=f"faltas_presenca_{estado_lancamento}_{idx_lanc}",
                    label_visibility="collapsed"
                )
                motivo_linha = row_cols[4].text_input(
                    "Motivo da falta",
                    value=str(row.get("motivo_falta", "")),
                    key=f"faltas_motivo_{estado_lancamento}_{idx_lanc}",
                    label_visibility="collapsed"
                )
                almoco_default = "Sim" if presenca_linha == "PRESENTE" else "Não"
                almoco_linha = row_cols[5].selectbox(
                    "Almoço",
                    almoco_opcoes,
                    index=almoco_opcoes.index(almoco_default),
                    key=f"faltas_almoco_{estado_lancamento}_{idx_lanc}",
                    label_visibility="collapsed"
                )
                observacoes_linha = row_cols[6].text_input(
                    "Observações",
                    value=str(row.get("observacoes", "")),
                    key=f"faltas_obs_{estado_lancamento}_{idx_lanc}",
                    label_visibility="collapsed"
                )
                linhas_editadas.append({
                    "lançar": lancar,
                    "colaborador": colaborador_nome,
                    "funcao": str(row["funcao"]),
                    "presenca": presenca_linha,
                    "motivo_falta": motivo_linha,
                    "almocou_base": almoco_linha,
                    "observacoes": observacoes_linha
                })

            df_lancamento = pd.DataFrame(linhas_editadas)

            if registros_existentes:
                st.warning("Alguns colaboradores já possuem lançamento nesta data. Eles vieram desmarcados para evitar duplicidade.")

            if st.button("Salvar todos os lançamentos", type="primary", use_container_width=True):
                selecionados = df_lancamento[df_lancamento["lançar"] == True].copy()
                erros = []
                for posicao, row in selecionados.iterrows():
                    if not str(row.get("colaborador", "")).strip():
                        erros.append(f"linha {posicao + 1}: colaborador")
                    if not str(row.get("funcao", "")).strip():
                        erros.append(f"linha {posicao + 1}: função")
                    if str(row.get("presenca", "")).upper() in ["FALTOU", "FALTA MEIO PERIODO"] and not str(row.get("motivo_falta", "")).strip():
                        erros.append(f"linha {posicao + 1}: motivo da falta")

                if selecionados.empty:
                    st.error("Selecione pelo menos um colaborador para salvar.")
                elif erros:
                    st.error("Preencha os campos obrigatórios: " + "; ".join(erros) + ".")
                else:
                    novos = selecionados.drop(columns=["lançar"], errors="ignore").copy()
                    novos.insert(0, "data", data_texto)
                    novos["tipo_escala"] = novos["colaborador"].map(df_colaboradores_lancamento.set_index("colaborador")["tipo_escala"]).fillna("SEGUNDA A SEXTA")
                    novos["data_base_escala"] = novos["colaborador"].map(df_colaboradores_lancamento.set_index("colaborador")["data_base_escala"]).fillna("")
                    novos["trabalha_data_base"] = novos["colaborador"].map(df_colaboradores_lancamento.set_index("colaborador")["trabalha_data_base"]).fillna("Sim")
                    novos["status_colaborador"] = "Ativo"
                    novos["base_frequencia"] = base_faltas_atual
                    df_faltas = pd.concat([df_faltas.drop(columns=["data_dt"], errors="ignore"), novos], ignore_index=True)
                    df_faltas.to_excel(CONTROLE_FALTAS_XLSX, index=False)
                    st.success("Lançamentos salvos com sucesso.")
                    st.rerun()

    elif subtela_faltas == "EDITAR LANÇAMENTO":
        if faltas_base.empty:
            st.info("Nenhum registro de frequência cadastrado.")
        else:
            opcoes_registros = {
                f"{idx} - {row['data']} - {row['colaborador']} - {row['presenca']}": idx
                for idx, row in faltas_base.iterrows()
            }
            registro_label = st.selectbox("Registro", list(opcoes_registros.keys()))
            idx = opcoes_registros[registro_label]
            dados = df_faltas.loc[idx]
            data_atual = pd.to_datetime(dados.get("data", ""), errors="coerce")
            data_edit = st.date_input("DATA", value=data_atual.date() if pd.notna(data_atual) else datetime.now().date(), key="faltas_data_edit")
            colaborador_edit = st.text_input("COLABORADOR", str(dados.get("colaborador", ""))).strip().upper()
            funcao_edit = st.text_input("FUNÇÃO", str(dados.get("funcao", ""))).strip().upper()
            presenca_atual = str(dados.get("presenca", "PRESENTE")).upper()
            presenca_edit = st.selectbox("PRESENÇA", presenca_opcoes, index=presenca_opcoes.index(presenca_atual) if presenca_atual in presenca_opcoes else 0, key="faltas_presenca_edit")
            motivo_edit = st.text_input("MOTIVO DA FALTA", str(dados.get("motivo_falta", "")))
            almoco_atual = str(dados.get("almocou_base", "Sim")).capitalize()
            almoco_edit = st.selectbox("ALMOÇOU NA BASE?", almoco_opcoes, index=almoco_opcoes.index(almoco_atual) if almoco_atual in almoco_opcoes else 0, key="faltas_almoco_edit")
            obs_edit = st.text_area("OBSERVAÇÕES", str(dados.get("observacoes", "")))

            if st.button("Salvar alteração de frequência"):
                if not colaborador_edit or not funcao_edit:
                    st.error("Preencha colaborador e função antes de salvar.")
                elif presenca_edit in ["FALTOU", "FALTA MEIO PERIODO"] and not motivo_edit.strip():
                    st.error("Informe o motivo da falta antes de salvar.")
                else:
                    df_faltas.loc[idx, "data"] = data_edit.isoformat()
                    df_faltas.loc[idx, "colaborador"] = colaborador_edit
                    df_faltas.loc[idx, "funcao"] = funcao_edit
                    df_faltas.loc[idx, "presenca"] = presenca_edit
                    df_faltas.loc[idx, "motivo_falta"] = motivo_edit.strip()
                    df_faltas.loc[idx, "almocou_base"] = almoco_edit
                    df_faltas.loc[idx, "observacoes"] = obs_edit.strip()
                    df_faltas.drop(columns=["data_dt"], errors="ignore").to_excel(CONTROLE_FALTAS_XLSX, index=False)
                    st.success("Registro atualizado.")
                    st.rerun()

            st.divider()
            st.subheader("Excluir lançamento")
            registro_excluir = st.selectbox("Registro para excluir", list(opcoes_registros.keys()), key="faltas_excluir")
            idx_excluir = opcoes_registros[registro_excluir]
            if st.button("Excluir registro de frequência"):
                df_faltas = df_faltas.drop(index=idx_excluir).reset_index(drop=True)
                df_faltas.drop(columns=["data_dt"], errors="ignore").to_excel(CONTROLE_FALTAS_XLSX, index=False)
                st.success("Registro excluído.")
                st.rerun()

    elif subtela_faltas == "COLABORADORES":
        if st.button("Voltar Para Lançamentos", use_container_width=True, key="voltar_colaboradores_lancamentos"):
            st.session_state["subtela_faltas"] = "LISTA DE FREQUÊNCIA"
            st.rerun()
        st.subheader("Colaboradores")
        df_colaboradores = df_colaboradores_base.copy()
        if not df_colaboradores.empty:
            contagem_registros = faltas_base[faltas_base["data"].astype(str).str.strip() != ""].groupby("colaborador").size()
            df_colaboradores["registros"] = df_colaboradores["colaborador"].map(contagem_registros).fillna(0).astype(int)
        else:
            df_colaboradores = pd.DataFrame(columns=["colaborador", "funcao", "tipo_escala", "data_base_escala", "trabalha_data_base", "status_colaborador", "registros"])

        colab1, colab2 = st.columns(2)
        with colab1:
            st.write("Adicionar colaborador")
            novo_colaborador = st.text_input("NOME DO COLABORADOR", key="novo_colaborador_faltas").strip().upper()
            nova_funcao = st.selectbox("FUNÇÃO", funcao_opcoes, key="nova_funcao_faltas")
            novo_tipo_escala = st.selectbox("TIPO DE ESCALA", escala_opcoes, key="novo_tipo_escala_faltas")
            nova_data_base = ""
            novo_trabalha_base = "Sim"
            if novo_tipo_escala == "12X36":
                nova_data_base = st.date_input("DATA BASE DA ESCALA", value=datetime.now().date(), key="nova_data_base_escala").isoformat()
                novo_trabalha_base = st.selectbox("TRABALHA NA DATA BASE?", ["Sim", "Não"], key="novo_trabalha_data_base")
            if st.button("ADICIONAR COLABORADOR"):
                if not novo_colaborador or not nova_funcao:
                    st.error("Preencha colaborador e função.")
                elif novo_colaborador in todos_colaboradores:
                    st.error("Este colaborador já existe nos registros.")
                else:
                    novo = pd.DataFrame([{
                        "data": "",
                        "colaborador": novo_colaborador,
                        "funcao": nova_funcao,
                        "presenca": "",
                        "motivo_falta": "",
                        "almocou_base": "",
                        "observacoes": "Cadastro de colaborador",
                        "tipo_escala": novo_tipo_escala,
                        "data_base_escala": nova_data_base,
                        "trabalha_data_base": novo_trabalha_base,
                        "status_colaborador": "Ativo",
                        "base_frequencia": base_faltas_atual
                    }])
                    df_faltas = pd.concat([df_faltas.drop(columns=["data_dt"], errors="ignore"), novo], ignore_index=True)
                    df_faltas.to_excel(CONTROLE_FALTAS_XLSX, index=False)
                    st.success("Colaborador adicionado.")
                    st.rerun()

        with colab2:
            st.write("Editar colaborador")
            if todos_colaboradores:
                colaborador_alterar = st.selectbox("COLABORADOR", todos_colaboradores, key="colaborador_funcao_edit")
                dados_colaborador = df_colaboradores_base[df_colaboradores_base["colaborador"] == colaborador_alterar].iloc[0]
                nome_alterar = st.text_input("NOME DO COLABORADOR", value=colaborador_alterar, key="nome_colaborador_edit").strip().upper()
                funcao_atual = str(dados_colaborador.get("funcao", ""))
                funcao_alterar = st.selectbox(
                    "FUNÇÃO",
                    funcao_opcoes,
                    index=funcao_opcoes.index(funcao_atual) if funcao_atual in funcao_opcoes else 0,
                    key="funcao_colaborador_edit"
                )
                escala_atual = str(dados_colaborador.get("tipo_escala", "SEGUNDA A SEXTA")).upper()
                tipo_escala_alterar = st.selectbox(
                    "TIPO DE ESCALA",
                    escala_opcoes,
                    index=escala_opcoes.index(escala_atual) if escala_atual in escala_opcoes else 0,
                    key="tipo_escala_colaborador_edit"
                )
                data_base_atual = pd.to_datetime(dados_colaborador.get("data_base_escala", ""), errors="coerce")
                data_base_alterar = ""
                trabalha_base_alterar = "Sim"
                if tipo_escala_alterar == "12X36":
                    data_base_alterar = st.date_input(
                        "DATA BASE DA ESCALA",
                        value=data_base_atual.date() if pd.notna(data_base_atual) else datetime.now().date(),
                        key="data_base_colaborador_edit"
                    ).isoformat()
                    trabalha_atual = str(dados_colaborador.get("trabalha_data_base", "Sim")).capitalize()
                    trabalha_base_alterar = st.selectbox(
                        "TRABALHA NA DATA BASE?",
                        ["Sim", "Não"],
                        index=0 if trabalha_atual != "Não" else 1,
                        key="trabalha_base_colaborador_edit"
                    )

                status_atual = str(dados_colaborador.get("status_colaborador", "Ativo")).capitalize()
                status_alterar = st.selectbox(
                    "STATUS",
                    ["Ativo", "Inativo"],
                    index=0 if status_atual != "Inativo" else 1,
                    key="status_colaborador_edit"
                )

                if st.button("SALVAR COLABORADOR"):
                    if not nome_alterar or not funcao_alterar:
                        st.error("Informe nome e função.")
                    elif nome_alterar != colaborador_alterar and nome_alterar in todos_colaboradores:
                        st.error("Já existe outro colaborador com este nome.")
                    else:
                        mask_colaborador = (
                            (df_faltas["base_frequencia"] == base_faltas_atual)
                            & (df_faltas["colaborador"] == colaborador_alterar)
                        )
                        df_faltas.loc[mask_colaborador, "colaborador"] = nome_alterar
                        df_faltas.loc[mask_colaborador, "funcao"] = funcao_alterar
                        df_faltas.loc[mask_colaborador, "tipo_escala"] = tipo_escala_alterar
                        df_faltas.loc[mask_colaborador, "data_base_escala"] = data_base_alterar
                        df_faltas.loc[mask_colaborador, "trabalha_data_base"] = trabalha_base_alterar
                        df_faltas.loc[mask_colaborador, "status_colaborador"] = status_alterar
                        df_faltas.drop(columns=["data_dt"], errors="ignore").to_excel(CONTROLE_FALTAS_XLSX, index=False)
                        st.success("Colaborador atualizado.")
                        st.rerun()
            else:
                st.info("Nenhum colaborador cadastrado.")

        st.divider()
        st.subheader("Inativar colaborador")
        colaboradores_ativos_para_inativar = df_colaboradores_base[df_colaboradores_base["status_colaborador"] != "Inativo"]["colaborador"].tolist()
        if colaboradores_ativos_para_inativar:
            colaborador_inativar = st.selectbox("COLABORADOR PARA INATIVAR", colaboradores_ativos_para_inativar, key="colaborador_inativar")
            if st.button("INATIVAR COLABORADOR"):
                df_faltas.loc[
                    (df_faltas["base_frequencia"] == base_faltas_atual)
                    & (df_faltas["colaborador"] == colaborador_inativar),
                    "status_colaborador"
                ] = "Inativo"
                df_faltas.drop(columns=["data_dt"], errors="ignore").to_excel(CONTROLE_FALTAS_XLSX, index=False)
                st.success("Colaborador inativado.")
                st.rerun()
        else:
            st.info("Nenhum colaborador ativo para inativar.")

        st.divider()
        st.subheader("Excluir colaborador")
        if todos_colaboradores:
            colaborador_excluir = st.selectbox("COLABORADOR PARA EXCLUIR", todos_colaboradores, key="colaborador_excluir")
            confirmar_exclusao = st.checkbox(
                "Confirmo que desejo excluir este colaborador e todos os registros vinculados a ele.",
                key="confirmar_excluir_colaborador"
            )
            if st.button("EXCLUIR COLABORADOR"):
                if not confirmar_exclusao:
                    st.error("Marque a confirmação antes de excluir o colaborador.")
                else:
                    df_faltas = df_faltas[
                        ~(
                            (df_faltas["base_frequencia"] == base_faltas_atual)
                            & (df_faltas["colaborador"] == colaborador_excluir)
                        )
                    ].reset_index(drop=True)
                    df_faltas.drop(columns=["data_dt"], errors="ignore").to_excel(CONTROLE_FALTAS_XLSX, index=False)
                    st.success("Colaborador excluído.")
                    st.rerun()
        else:
            st.info("Nenhum colaborador cadastrado para excluir.")

        st.dataframe(formatar_colunas_tabela(df_colaboradores.drop(columns=colunas_escala_ocultas, errors="ignore")), use_container_width=True, hide_index=True)

    elif subtela_faltas == "RELATORIOS_FREQUENCIA":
        if st.button("Voltar Para Lançamentos", use_container_width=True, key="voltar_relatorios_lancamentos"):
            st.session_state["subtela_faltas"] = "LISTA DE FREQUÊNCIA"
            st.rerun()
        st.subheader("Painel De Relatórios")
        registros_relatorio = faltas_base[
            (faltas_base["data"].astype(str).str.strip() != "")
            & (faltas_base["data_dt"].notna())
        ].copy()

        if registros_relatorio.empty:
            st.info("Nenhum lançamento de frequência encontrado para gerar relatório.")
        else:
            data_minima = registros_relatorio["data_dt"].min().date()
            data_maxima = registros_relatorio["data_dt"].max().date()
            filtro_ini_col, filtro_fim_col = st.columns(2)
            data_inicio_rel = filtro_ini_col.date_input("Data Inicial", value=data_minima, key="rel_frequencia_inicio")
            data_fim_rel = filtro_fim_col.date_input("Data Final", value=data_maxima, key="rel_frequencia_fim")

            if data_inicio_rel > data_fim_rel:
                st.error("A data inicial não pode ser maior que a data final.")
            else:
                rel_filtrado = registros_relatorio[
                    (registros_relatorio["data_dt"].dt.date >= data_inicio_rel)
                    & (registros_relatorio["data_dt"].dt.date <= data_fim_rel)
                ].copy()

                total_lancamentos = len(rel_filtrado)
                total_presentes = int((rel_filtrado["presenca"].astype(str).str.upper() == "PRESENTE").sum()) if not rel_filtrado.empty else 0
                total_faltas = int(rel_filtrado["presenca"].astype(str).str.upper().isin(["FALTOU", "FALTA MEIO PERIODO"]).sum()) if not rel_filtrado.empty else 0
                total_atestados = int((rel_filtrado["presenca"].astype(str).str.upper() == "ATESTADO").sum()) if not rel_filtrado.empty else 0
                total_almocos = int((rel_filtrado["almocou_base"].astype(str).str.upper() == "SIM").sum()) if not rel_filtrado.empty else 0
                metricas_frequencia = {
                    "lancamentos": total_lancamentos,
                    "presentes": total_presentes,
                    "faltas": total_faltas,
                    "atestados": total_atestados,
                    "almocos": total_almocos
                }

                met1, met2, met3, met4, met5 = st.columns(5)
                met1.markdown(f"<div class='metric-card'><div class='metric-label'>Lançamentos</div><div class='metric-value'>{total_lancamentos}</div></div>", unsafe_allow_html=True)
                met2.markdown(f"<div class='metric-card'><div class='metric-label'>Presentes</div><div class='metric-value' style='color:#22c55e'>{total_presentes}</div></div>", unsafe_allow_html=True)
                met3.markdown(f"<div class='metric-card'><div class='metric-label'>Faltas</div><div class='metric-value' style='color:#ef4444'>{total_faltas}</div></div>", unsafe_allow_html=True)
                met4.markdown(f"<div class='metric-card'><div class='metric-label'>Atestados</div><div class='metric-value' style='color:#f59e0b'>{total_atestados}</div></div>", unsafe_allow_html=True)
                met5.markdown(f"<div class='metric-card'><div class='metric-label'>Almoços</div><div class='metric-value'>{total_almocos}</div></div>", unsafe_allow_html=True)

                st.divider()
                rel_col1, rel_col2 = st.columns(2)
                with rel_col1:
                    st.write("Resumo Por Presença")
                    resumo_presenca = rel_filtrado["presenca"].fillna("").replace("", "NÃO INFORMADO").value_counts().reset_index()
                    resumo_presenca.columns = ["presenca", "quantidade"]
                    st.dataframe(formatar_colunas_tabela(resumo_presenca), use_container_width=True, hide_index=True)

                    st.write("Resumo Por Função")
                    resumo_funcao = rel_filtrado.groupby("funcao").size().reset_index(name="quantidade") if not rel_filtrado.empty else pd.DataFrame(columns=["funcao", "quantidade"])
                    st.dataframe(formatar_colunas_tabela(resumo_funcao), use_container_width=True, hide_index=True)

                with rel_col2:
                    st.write("Resumo Por Dia")
                    resumo_dia = rel_filtrado.groupby(rel_filtrado["data_dt"].dt.date).agg(
                        lancamentos=("colaborador", "count"),
                        presentes=("presenca", lambda serie: int((serie.astype(str).str.upper() == "PRESENTE").sum())),
                        faltas=("presenca", lambda serie: int(serie.astype(str).str.upper().isin(["FALTOU", "FALTA MEIO PERIODO"]).sum())),
                        almocos=("almocou_base", lambda serie: int((serie.astype(str).str.upper() == "SIM").sum()))
                    ).reset_index()
                    resumo_dia = resumo_dia.rename(columns={"data_dt": "data"})
                    st.dataframe(formatar_colunas_tabela(resumo_dia), use_container_width=True, hide_index=True)

                    st.write("Quantidade De Almoço Por Função")
                    rel_almocos = rel_filtrado[rel_filtrado["almocou_base"].astype(str).str.upper() == "SIM"].copy()
                    resumo_almoco_funcao = rel_almocos.groupby("funcao").size().reset_index(name="quantidade_almocos") if not rel_almocos.empty else pd.DataFrame(columns=["funcao", "quantidade_almocos"])
                    st.dataframe(formatar_colunas_tabela(resumo_almoco_funcao), use_container_width=True, hide_index=True)

                st.write("Resumo Por Colaborador")
                resumo_colaborador = rel_filtrado.groupby("colaborador").agg(
                    lancamentos=("colaborador", "count"),
                    presentes=("presenca", lambda serie: int((serie.astype(str).str.upper() == "PRESENTE").sum())),
                    faltas=("presenca", lambda serie: int(serie.astype(str).str.upper().isin(["FALTOU", "FALTA MEIO PERIODO"]).sum())),
                    atestados=("presenca", lambda serie: int((serie.astype(str).str.upper() == "ATESTADO").sum())),
                    almocos=("almocou_base", lambda serie: int((serie.astype(str).str.upper() == "SIM").sum()))
                ).reset_index() if not rel_filtrado.empty else pd.DataFrame(columns=["colaborador", "lancamentos", "presentes", "faltas", "atestados", "almocos"])
                st.dataframe(formatar_colunas_tabela(resumo_colaborador), use_container_width=True, hide_index=True)

                st.write("Lançamentos Do Período")
                colunas_relatorio = ["data", "colaborador", "funcao", "presenca", "motivo_falta", "almocou_base", "observacoes"]
                st.dataframe(formatar_colunas_tabela(rel_filtrado[colunas_relatorio]), use_container_width=True, hide_index=True)
                st.divider()
                tipo_pdf_frequencia = st.selectbox(
                    "Selecionar Relatorio Para PDF",
                    ["Completo", "Por Presenca", "Por Funcao", "Por Dia", "Por Colaborador", "Almoco Por Funcao", "Lancamentos Detalhados"],
                    key="tipo_pdf_frequencia"
                )
                nome_base_pdf = base_faltas_atual.lower().replace(" ", "_")
                nome_tipo_pdf = tipo_pdf_frequencia.lower().replace(" ", "_")
                pdf_frequencia = gerar_pdf_relatorio_frequencia(
                    base_faltas_atual,
                    data_inicio_rel,
                    data_fim_rel,
                    rel_filtrado,
                    resumo_presenca,
                    resumo_funcao,
                    resumo_dia,
                    resumo_colaborador,
                    resumo_almoco_funcao,
                    metricas_frequencia,
                    tipo_pdf_frequencia
                )
                st.download_button(
                    "Baixar RelatÃ³rio Em PDF",
                    data=pdf_frequencia,
                    file_name=f"relatorio_frequencia_{nome_tipo_pdf}_{nome_base_pdf}_{data_inicio_rel.isoformat()}_{data_fim_rel.isoformat()}.pdf",
                    mime="application/pdf",
                    use_container_width=True
                )

    elif subtela_faltas in ["ESTOQUE DA BASE", "ESTOQUE"]:
        if not supervisor_base_mode and st.button("Voltar", use_container_width=True, key="voltar_base_estoque"):
            st.session_state["subtela_faltas"] = ""
            st.session_state["acao_estoque_base"] = ""
            st.rerun()
        st.subheader(f"Estoque Local - {base_faltas_atual}")
        estoque_base = calcular_estoque_base(df_bases_movimentacoes, base_faltas_atual)
        st.session_state["acao_estoque_base"] = ""

        colunas_produto_base = ["codigo", "produto", "categoria", "estoque_minimo", "localizacao", "imagem"]
        produtos_base_info = df_produtos[colunas_produto_base].copy() if not df_produtos.empty else pd.DataFrame(columns=colunas_produto_base)
        estoque_base_view = estoque_base.merge(produtos_base_info, on="produto", how="left") if not estoque_base.empty else pd.DataFrame(columns=["produto", "entradas", "saidas", "estoque_atual"] + [c for c in colunas_produto_base if c != "produto"])
        for col_info in ["codigo", "categoria", "estoque_minimo", "localizacao", "imagem"]:
            if col_info not in estoque_base_view.columns:
                estoque_base_view[col_info] = ""
            estoque_base_view[col_info] = estoque_base_view[col_info].astype("object").fillna("")
        estoque_base_view["estoque_minimo"] = pd.to_numeric(estoque_base_view["estoque_minimo"], errors="coerce").fillna(0)
        estoque_base_view["situacao"] = estoque_base_view.apply(
            lambda row: "🔴 ESTOQUE BAIXO" if float(row.get("estoque_atual", 0)) <= float(row.get("estoque_minimo", 0)) else "🟢 OK",
            axis=1
        ) if not estoque_base_view.empty else ""

        total_itens_base = len(estoque_base_view)
        total_ok_base = int((estoque_base_view["situacao"] == "🟢 OK").sum()) if not estoque_base_view.empty else 0
        total_baixo_base = int((estoque_base_view["situacao"] == "🔴 ESTOQUE BAIXO").sum()) if not estoque_base_view.empty else 0

        b1, b2, b3 = st.columns(3)
        b1.markdown(f"<div class='metric-card'><div class='metric-label'>Total de Produtos Na Base</div><div class='metric-value'>{total_itens_base}</div><div class='metric-label'>{base_faltas_atual}</div></div>", unsafe_allow_html=True)
        b2.markdown(f"<div class='metric-card'><div class='metric-label'>Estoque OK</div><div class='metric-value' style='color:#22c55e'>{total_ok_base}</div><div class='metric-label'>Acima do mínimo</div></div>", unsafe_allow_html=True)
        b3.markdown(f"<div class='metric-card'><div class='metric-label'>Estoque Baixo</div><div class='metric-value' style='color:#ef4444'>{total_baixo_base}</div><div class='metric-label'>Abaixo ou igual ao mínimo</div></div>", unsafe_allow_html=True)

        reposicao_base = estoque_base_view.copy()
        reposicao_base["necessita"] = (reposicao_base["estoque_minimo"] + 5) - reposicao_base["estoque_atual"]
        reposicao_base = reposicao_base[reposicao_base["necessita"] > 0]
        if st.button("Solicitar Reposição Para Matriz", type="primary", use_container_width=True, key=f"solicitar_reposicao_{base_faltas_atual}"):
            if reposicao_base.empty:
                st.info("Nenhum item precisa de reposição nesta base.")
            else:
                pasta_downloads = os.path.join(os.path.expanduser("~"), "Downloads")
                nome_base_arquivo = base_faltas_atual.lower().replace(" ", "_")
                caminho_pdf = os.path.join(pasta_downloads, f"solicitacao_reposicao_{nome_base_arquivo}.pdf")
                data_pdf = [["Código", "Produto", "Atual", "Mínimo", "Necessita", "Imagem"]]
                for _, r in reposicao_base.iterrows():
                    img_path = os.path.join(PASTA_IMAGENS, str(r.get("imagem", "")))
                    img_rl = ""
                    if os.path.exists(img_path):
                        try:
                            img_rl = RLImage(img_path, width=1 * inch, height=1 * inch)
                        except Exception:
                            pass
                    data_pdf.append([
                        r.get("codigo", ""),
                        r.get("produto", ""),
                        str(int(float(r.get("estoque_atual", 0)))),
                        str(int(float(r.get("estoque_minimo", 0)))),
                        str(int(float(r.get("necessita", 0)))),
                        img_rl
                    ])
                pdf = SimpleDocTemplate(caminho_pdf, pagesize=letter)
                tabela = Table(data_pdf, colWidths=[60, 150, 50, 60, 60, 100])
                tabela.setStyle(TableStyle([
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
                ]))
                pdf.build([tabela])
                st.success(f"Solicitação de reposição salva em: {caminho_pdf}")

        st.markdown("<br>", unsafe_allow_html=True)
        busca_base = st.text_input("Busca", placeholder="Buscar por código, produto ou categoria", label_visibility="collapsed", key=f"busca_estoque_base_{base_faltas_atual}")

        with st.expander("Filtros Avançados"):
            f_base_col1, f_base_col2 = st.columns(2)
            categorias_base = ["Todas"] + list(dict.fromkeys(estoque_base_view["categoria"].dropna().astype(str).tolist()))
            f_cat_base = f_base_col1.selectbox("Categoria", categorias_base, key=f"f_cat_base_{base_faltas_atual}")
            f_sit_base = f_base_col2.selectbox("Situação", ["Todas", "Estoque OK", "Estoque Baixo"], key=f"f_sit_base_{base_faltas_atual}")

        df_base_filtrado = estoque_base_view.copy()
        if busca_base:
            termo_base = str(busca_base).lower()
            df_base_filtrado = df_base_filtrado[
                df_base_filtrado["codigo"].astype(str).str.lower().str.contains(termo_base)
                | df_base_filtrado["produto"].astype(str).str.lower().str.contains(termo_base)
                | df_base_filtrado["categoria"].astype(str).str.lower().str.contains(termo_base)
            ]
        if f_cat_base != "Todas":
            df_base_filtrado = df_base_filtrado[df_base_filtrado["categoria"] == f_cat_base]
        if f_sit_base == "Estoque OK":
            df_base_filtrado = df_base_filtrado[df_base_filtrado["situacao"] == "🟢 OK"]
        elif f_sit_base == "Estoque Baixo":
            df_base_filtrado = df_base_filtrado[df_base_filtrado["situacao"] == "🔴 ESTOQUE BAIXO"]

        st.markdown("<br>", unsafe_allow_html=True)
        headers_base = st.columns([1, 2, 2, 1, 1, 2, 2, 3])
        headers_base[0].write("Código")
        headers_base[1].write("Produto")
        headers_base[2].write("Categoria")
        headers_base[3].write("Estoque Atual")
        headers_base[4].write("Estoque Mínimo")
        headers_base[5].write("Localização")
        headers_base[6].write("Situação")
        headers_base[7].write("Imagem")

        if df_base_filtrado.empty:
            st.info("Nenhum produto com estoque nesta base.")
        for i_base, row_base in df_base_filtrado.iterrows():
            col_base = st.columns([1, 2, 2, 1, 1, 2, 2, 3])
            col_base[0].write(row_base.get("codigo", ""))
            if col_base[1].button(row_base.get("produto", ""), key=f"prod_base_{base_faltas_atual}_{i_base}"):
                st.session_state["produto_base_historico"] = str(row_base.get("produto", ""))
            col_base[2].markdown(f"<span style='color:{cor_categoria(row_base.get('categoria', ''))}'><b>{row_base.get('categoria', '')}</b></span>", unsafe_allow_html=True)
            col_base[3].write(int(float(row_base.get("estoque_atual", 0))))
            col_base[4].markdown(f"<span style='color:#facc15'><b>{int(float(row_base.get('estoque_minimo', 0)))}</b></span>", unsafe_allow_html=True)
            col_base[5].write(row_base.get("localizacao", ""))
            col_base[6].write(row_base.get("situacao", ""))
            img_base = os.path.join(PASTA_IMAGENS, str(row_base.get("imagem", "")))
            if os.path.exists(img_base):
                col_base[7].image(img_base, use_container_width=True)

        produto_base_historico = st.session_state.get("produto_base_historico", "")
        if produto_base_historico:
            st.divider()
            st.subheader(f"Histórico - {produto_base_historico}")
            hist_base_produto = df_bases_movimentacoes[
                (df_bases_movimentacoes["base"].astype(str) == base_faltas_atual)
                & (df_bases_movimentacoes["produto"].astype(str) == produto_base_historico)
            ].copy()
            if not hist_base_produto.empty:
                st.dataframe(formatar_colunas_tabela(hist_base_produto), use_container_width=True, hide_index=True)
            else:
                st.info("Sem movimentações para este produto nesta base.")
            if st.button("Fechar Histórico", key="fechar_historico_produto_base"):
                st.session_state.pop("produto_base_historico", None)
                st.rerun()
    elif subtela_faltas == "TRANSFERÊNCIAS":
        st.subheader("Transferência Da Matriz Para A Base")
        produtos_matriz = df_produtos["produto"].dropna().astype(str).tolist() if not df_produtos.empty else []
        if not produtos_matriz:
            st.info("Cadastre produtos no almoxarifado matriz antes de transferir.")
        else:
            c1, c2, c3 = st.columns(3)
            data_transf = c1.date_input("Data", value=datetime.now().date(), key="base_transf_data")
            produto_transf = c2.selectbox("Produto", produtos_matriz, key="base_transf_produto")
            quantidade_transf = c3.number_input("Quantidade", min_value=0.0, step=1.0, format="%.2f", key="base_transf_qtd")
            estoque_disponivel_matriz = estoque_matriz_produto(produto_transf)
            st.info(f"Estoque disponível na Matriz: {estoque_disponivel_matriz:,.2f}")
            c4, c5 = st.columns(2)
            responsavel_envio = c4.text_input("Responsável Pelo Envio", key="base_transf_envio").strip().upper()
            responsavel_recebimento = c5.text_input("Responsável Pelo Recebimento", key="base_transf_recebimento").strip().upper()
            obs_transf = st.text_area("Observações", key="base_transf_obs").strip()

            if st.button("ENVIAR PARA BASE", type="primary", use_container_width=True):
                if quantidade_transf <= 0:
                    st.error("Informe uma quantidade maior que zero.")
                elif quantidade_transf > estoque_disponivel_matriz:
                    st.error("Quantidade maior que o estoque disponível na Matriz.")
                elif not responsavel_envio or not responsavel_recebimento:
                    st.error("Informe os responsáveis pelo envio e recebimento.")
                else:
                    nova_saida_matriz = pd.DataFrame([{
                        "produto": produto_transf,
                        "tipo": "Saída",
                        "quantidade": float(quantidade_transf),
                        "data": datetime.now(),
                        "cliente": base_faltas_atual,
                        "observacao": f"Transferência Matriz -> {base_faltas_atual}. {obs_transf}".strip()
                    }])
                    df_mov = pd.concat([df_mov, nova_saida_matriz], ignore_index=True)
                    df_mov.to_excel(MOVIMENTACOES_XLSX, index=False)

                    nova_entrada_base = pd.DataFrame([{
                        "data": data_transf.isoformat(),
                        "base": base_faltas_atual,
                        "produto": produto_transf,
                        "tipo": "Entrada",
                        "quantidade": float(quantidade_transf),
                        "responsavel": responsavel_recebimento,
                        "origem_destino": "MATRIZ",
                        "observacoes": obs_transf
                    }])
                    df_bases_movimentacoes = pd.concat([df_bases_movimentacoes, nova_entrada_base], ignore_index=True)
                    df_bases_movimentacoes.to_excel(BASES_MOVIMENTACOES_XLSX, index=False)

                    nova_transferencia = pd.DataFrame([{
                        "data": data_transf.isoformat(),
                        "produto": produto_transf,
                        "quantidade": float(quantidade_transf),
                        "origem": "MATRIZ",
                        "destino": base_faltas_atual,
                        "responsavel_envio": responsavel_envio,
                        "responsavel_recebimento": responsavel_recebimento,
                        "status": "Recebido",
                        "observacoes": obs_transf
                    }])
                    df_bases_transferencias = pd.concat([df_bases_transferencias, nova_transferencia], ignore_index=True)
                    df_bases_transferencias.to_excel(BASES_TRANSFERENCIAS_XLSX, index=False)
                    st.success("Transferência registrada. A Matriz foi baixada e a Base recebeu a entrada.")
                    st.rerun()

        st.subheader("Histórico De Transferências")
        hist_transferencias = df_bases_transferencias[
            (df_bases_transferencias["destino"].astype(str) == base_faltas_atual)
            | (df_bases_transferencias["origem"].astype(str) == base_faltas_atual)
        ].copy()
        st.dataframe(formatar_colunas_tabela(hist_transferencias), use_container_width=True, hide_index=True)

    elif subtela_faltas == "RELATÓRIOS":
        with st.expander("Filtros", expanded=True):
            f1, f2, f3, f4 = st.columns(4)
            data_ini = f1.date_input("Data inicial", value=datetime.now().date().replace(day=1), key="faltas_data_ini")
            data_fim = f2.date_input("Data final", value=datetime.now().date(), key="faltas_data_fim")
            colaborador_filtro = f3.selectbox("Colaborador", ["Todos"] + colaboradores)
            status_filtro = f4.selectbox("Presença", ["Todos"] + presenca_opcoes)

        df_rel_faltas = faltas_base.copy()
        df_rel_faltas = df_rel_faltas[
            (df_rel_faltas["data_dt"] >= pd.to_datetime(data_ini))
            & (df_rel_faltas["data_dt"] <= pd.to_datetime(data_fim))
        ]
        if colaborador_filtro != "Todos":
            df_rel_faltas = df_rel_faltas[df_rel_faltas["colaborador"] == colaborador_filtro]
        if status_filtro != "Todos":
            df_rel_faltas = df_rel_faltas[df_rel_faltas["presenca"].astype(str).str.upper() == status_filtro]

        resumo_colaborador = pd.DataFrame(columns=[
            "colaborador", "funcao", "presencas", "faltas", "falta_meio_periodo",
            "atestados", "folgas", "ferias", "feriados", "dias_considerados",
            "almoco_na_base", "nao_almocou_na_base"
        ])
        if not df_rel_faltas.empty:
            df_rel_faltas["presenca_normalizada"] = df_rel_faltas["presenca"].astype(str).str.upper()
            df_rel_faltas["almoco_normalizado"] = df_rel_faltas["almocou_base"].astype(str).str.upper()
            resumo_colaborador = df_rel_faltas.groupby(["colaborador", "funcao"], as_index=False).agg(
                presencas=("presenca_normalizada", lambda s: (s == "PRESENTE").sum()),
                faltas=("presenca", lambda s: (s.astype(str).str.upper() == "FALTOU").sum()),
                falta_meio_periodo=("presenca", lambda s: (s.astype(str).str.upper() == "FALTA MEIO PERIODO").sum()),
                atestados=("presenca", lambda s: (s.astype(str).str.upper() == "ATESTADO").sum()),
                folgas=("presenca", lambda s: (s.astype(str).str.upper() == "FOLGA").sum()),
                ferias=("presenca", lambda s: (s.astype(str).str.upper() == "FÉRIAS").sum()),
                feriados=("presenca", lambda s: (s.astype(str).str.upper() == "FERIADO").sum()),
                dias_considerados=("presenca", "count"),
                almoco_na_base=("almocou_base", lambda s: (s.astype(str).str.upper() == "SIM").sum())
            )
            resumo_colaborador["nao_almocou_na_base"] = (
                resumo_colaborador["dias_considerados"] - resumo_colaborador["almoco_na_base"]
            ).clip(lower=0)

        total_medicao = {
            "presencas": int(resumo_colaborador["presencas"].sum()) if not resumo_colaborador.empty else 0,
            "faltas": int(resumo_colaborador["faltas"].sum()) if not resumo_colaborador.empty else 0,
            "falta_meio_periodo": int(resumo_colaborador["falta_meio_periodo"].sum()) if not resumo_colaborador.empty else 0,
            "atestados": int(resumo_colaborador["atestados"].sum()) if not resumo_colaborador.empty else 0,
            "folgas": int(resumo_colaborador["folgas"].sum()) if not resumo_colaborador.empty else 0,
            "ferias": int(resumo_colaborador["ferias"].sum()) if not resumo_colaborador.empty else 0,
            "feriados": int(resumo_colaborador["feriados"].sum()) if not resumo_colaborador.empty else 0,
            "dias_considerados": int(resumo_colaborador["dias_considerados"].sum()) if not resumo_colaborador.empty else 0,
            "almoco_na_base": int(resumo_colaborador["almoco_na_base"].sum()) if not resumo_colaborador.empty else 0
        }
        total_medicao["nao_almocou_na_base"] = max(total_medicao["dias_considerados"] - total_medicao["almoco_na_base"], 0)

        resumo_geral_medicao = pd.DataFrame([{
            "periodo": f"{data_ini.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}",
            "colaboradores": int(resumo_colaborador["colaborador"].nunique()) if not resumo_colaborador.empty else 0,
            **total_medicao
        }])

        resumo_funcao_medicao = pd.DataFrame(columns=[
            "funcao", "presencas", "faltas", "falta_meio_periodo",
            "atestados", "folgas", "ferias", "feriados", "dias_considerados",
            "almoco_na_base", "nao_almocou_na_base"
        ])
        if not resumo_colaborador.empty:
            resumo_funcao_medicao = resumo_colaborador.groupby("funcao", as_index=False)[[
                "presencas", "faltas", "falta_meio_periodo", "atestados",
                "folgas", "ferias", "feriados", "dias_considerados",
                "almoco_na_base", "nao_almocou_na_base"
            ]].sum()

        st.subheader("Medição Mensal")
        med1, med2, med3, med4, med5, med6 = st.columns(6)
        med1.markdown(f"<div class='metric-card'><div class='metric-label'>Presenças</div><div class='metric-value' style='color:#22c55e'>{total_medicao['presencas']}</div></div>", unsafe_allow_html=True)
        med2.markdown(f"<div class='metric-card'><div class='metric-label'>Faltas</div><div class='metric-value' style='color:#ef4444'>{total_medicao['faltas']}</div></div>", unsafe_allow_html=True)
        med3.markdown(f"<div class='metric-card'><div class='metric-label'>Atestados</div><div class='metric-value' style='color:#facc15'>{total_medicao['atestados']}</div></div>", unsafe_allow_html=True)
        med4.markdown(f"<div class='metric-card'><div class='metric-label'>Almoços Na Base</div><div class='metric-value' style='color:#38bdf8'>{total_medicao['almoco_na_base']}</div></div>", unsafe_allow_html=True)
        med5.markdown(f"<div class='metric-card'><div class='metric-label'>Não Almoçou</div><div class='metric-value' style='color:#f97316'>{total_medicao['nao_almocou_na_base']}</div></div>", unsafe_allow_html=True)
        med6.markdown(f"<div class='metric-card'><div class='metric-label'>Dias Considerados</div><div class='metric-value'>{total_medicao['dias_considerados']}</div></div>", unsafe_allow_html=True)

        st.subheader("Resumo Geral Para Cliente")
        st.dataframe(formatar_colunas_tabela(resumo_geral_medicao), use_container_width=True)

        st.subheader("Resumo Por Função")
        st.dataframe(formatar_colunas_tabela(resumo_funcao_medicao), use_container_width=True)

        st.subheader("Quantitativos por colaborador")
        st.dataframe(formatar_colunas_tabela(resumo_colaborador), use_container_width=True)

        st.subheader("Histórico de frequência")
        historico_faltas_exibir = df_rel_faltas.drop(
            columns=["data_dt", "presenca_normalizada", "almoco_normalizado"] + colunas_escala_ocultas,
            errors="ignore"
        )
        st.dataframe(formatar_colunas_tabela(historico_faltas_exibir), use_container_width=True)

        buffer_faltas = io.BytesIO()
        with pd.ExcelWriter(buffer_faltas, engine="openpyxl") as writer:
            resumo_geral_medicao.to_excel(writer, sheet_name="Resumo Geral", index=False)
            resumo_funcao_medicao.to_excel(writer, sheet_name="Resumo Por Funcao", index=False)
            resumo_colaborador.to_excel(writer, sheet_name="Por Colaborador", index=False)
            historico_faltas_exibir.to_excel(writer, sheet_name="Historico", index=False)
        buffer_faltas.seek(0)
        st.download_button(
            "Exportar Excel",
            data=buffer_faltas,
            file_name="controle_de_faltas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    elif subtela_faltas in ["DESPESAS DE FROTA", "DESPESAS FROTA", "DESPESAS FROTAS"]:
        if not supervisor_base_mode and st.button("Voltar", use_container_width=True, key="voltar_base_despesas"):
            st.session_state["subtela_faltas"] = ""
            st.rerun()
        if usuario_pode_lancar_despesa_frota(usuario_logado):
            tela_responsavel_frota()
        else:
            st.error("Você não tem permissão para lançar despesas de frota.")


# =========================
# RELATORIOS
# =========================
elif menu == "RELATÓRIOS":
    st.title("RELATÓRIOS")

    top1, top2, top3, top4 = st.columns([1, 1, 1, 6])

    with st.expander("Filtros", expanded=True):
        f1, f2, f3, f4, f5 = st.columns(5)
        periodo = f1.selectbox("Período", ["7 dias", "30 dias", "Personalizado"], index=1)
        tipo_relatorio = f2.selectbox("Tipo", ["Por cliente", "Por produto"])
        categoria_rel = f3.selectbox("Categoria", ["Todas"] + list(df_produtos["categoria"].dropna().unique()))
        produto_rel = f4.selectbox("Produto", ["Todos"] + list(df_produtos["produto"].dropna().unique()))
        clientes_rel_lista = list(dict.fromkeys(
            df_clientes["nome_cliente"].dropna().astype(str).tolist()
            + df_mov["cliente"].dropna().astype(str).tolist()
        ))
        clientes_rel_lista = [c for c in clientes_rel_lista if c.strip()]
        cliente_rel = f5.selectbox("Cliente", ["Todos"] + clientes_rel_lista)

        data_ini_rel, data_fim_rel = None, None
        if periodo == "Personalizado":
            d1, d2 = st.columns(2)
            data_ini_rel = d1.date_input("Data inicial")
            data_fim_rel = d2.date_input("Data final")

        filtrar = st.button("Filtrar")

    df_rel = filtrar_movimentacoes(df_mov, periodo, "Todos", categoria_rel, produto_rel, data_ini_rel, data_fim_rel)
    if cliente_rel != "Todos":
        df_rel = df_rel[df_rel["cliente"].fillna("").astype(str) == cliente_rel]
    df_criticos = df_produtos[df_produtos["estoque_atual"] <= df_produtos["estoque_minimo"]].copy()
    produtos_rel = df_produtos.copy()
    if categoria_rel != "Todas":
        produtos_rel = produtos_rel[produtos_rel["categoria"] == categoria_rel]
    if produto_rel != "Todos":
        produtos_rel = produtos_rel[produtos_rel["produto"] == produto_rel]
    df_menos_mov = calcular_menos_movimentados(df_rel, produtos_rel)
    df_gastos_clientes, df_gastos_detalhe = calcular_gastos_clientes(df_rel)
    df_top_produtos_saidas = produtos_mais_saidas(df_rel)
    metricas = {
        "total_produtos": int(len(df_produtos)),
        "entradas": int(df_rel[df_rel["tipo"] == "Entrada"]["quantidade"].sum()) if not df_rel.empty else 0,
        "saidas": int(df_rel[df_rel["tipo"] == "Saída"]["quantidade"].sum()) if not df_rel.empty else 0,
        "criticos": int(len(df_criticos)),
        "gasto_clientes": float(df_gastos_clientes["total"].sum()) if not df_gastos_clientes.empty else 0
    }

    with top1:
        st.download_button("PDF", data=gerar_pdf_relatorios(df_rel, df_criticos, df_menos_mov, df_gastos_clientes, df_gastos_detalhe, metricas), file_name="relatorios_estoque.pdf", mime="application/pdf", use_container_width=True)
    with top2:
        st.download_button("Excel", data=gerar_excel_relatorios(df_rel, df_criticos, df_menos_mov, df_gastos_clientes, df_gastos_detalhe, metricas), file_name="relatorios_estoque.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    with top3:
        if filtrar:
            st.success("Filtros aplicados")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.markdown(f"<div class='metric-card'><div class='metric-label'>Total de produtos</div><div class='metric-value'>{metricas['total_produtos']}</div></div>", unsafe_allow_html=True)
    c2.markdown(f"<div class='metric-card'><div class='metric-label'>Entradas</div><div class='metric-value' style='color:#22c55e'>{metricas['entradas']}</div></div>", unsafe_allow_html=True)
    c3.markdown(f"<div class='metric-card'><div class='metric-label'>Saídas</div><div class='metric-value' style='color:#ef4444'>{metricas['saidas']}</div></div>", unsafe_allow_html=True)
    c4.markdown(f"<div class='metric-card'><div class='metric-label'>Itens críticos</div><div class='metric-value' style='color:#facc15'>{metricas['criticos']}</div></div>", unsafe_allow_html=True)
    c5.markdown(f"<div class='metric-card'><div class='metric-label'>Gasto clientes</div><div class='metric-value' style='color:#22c55e'>R$ {metricas['gasto_clientes']:,.2f}</div></div>", unsafe_allow_html=True)

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

    st.subheader("Total Gasto Por Cliente")
    if not df_gastos_clientes.empty:
        df_total_cliente = df_gastos_clientes.copy()
        df_total_cliente["total"] = df_total_cliente["total"].map(lambda v: f"R$ {v:,.2f}")
        st.dataframe(formatar_colunas_tabela(df_total_cliente), use_container_width=True)
    else:
        st.info("Sem gastos por cliente no perÃ­odo selecionado.")

    r1, r2 = st.columns(2)
    with r1:
        st.subheader("Top 5 Clientes Que Mais Gastam")
        if not df_gastos_clientes.empty:
            top_clientes_mais_grafico = df_gastos_clientes.sort_values("total", ascending=False).head(5).copy()
            top_clientes_mais = top_clientes_mais_grafico.copy()
            top_clientes_mais["total"] = top_clientes_mais["total"].map(lambda v: f"R$ {v:,.2f}")
            st.dataframe(formatar_colunas_tabela(top_clientes_mais), use_container_width=True, hide_index=True)
            st.bar_chart(top_clientes_mais_grafico.set_index("cliente")[["total"]])
        else:
            st.info("Sem dados de clientes.")

    with r2:
        st.subheader("Top 5 Clientes Que Menos Gastam")
        if not df_gastos_clientes.empty:
            top_clientes_menos_grafico = df_gastos_clientes.sort_values("total", ascending=True).head(5).copy()
            top_clientes_menos = top_clientes_menos_grafico.copy()
            top_clientes_menos["total"] = top_clientes_menos["total"].map(lambda v: f"R$ {v:,.2f}")
            st.dataframe(formatar_colunas_tabela(top_clientes_menos), use_container_width=True, hide_index=True)
            st.bar_chart(top_clientes_menos_grafico.set_index("cliente")[["total"]])
        else:
            st.info("Sem dados de clientes.")

    st.subheader("Top 5 Produtos Com Mais Saídas")
    if not df_top_produtos_saidas.empty:
        st.dataframe(formatar_colunas_tabela(df_top_produtos_saidas.head(5)), use_container_width=True, hide_index=True)
    else:
        st.info("Sem saídas de produtos no período selecionado.")

    if tipo_relatorio == "Por cliente":
        st.subheader("Produtos por cliente")
        if not df_gastos_detalhe.empty:
            df_gastos_detalhe_exibir = df_gastos_detalhe.copy()
            df_gastos_detalhe_exibir["valor_unitario"] = df_gastos_detalhe_exibir["valor_unitario"].map(lambda v: f"R$ {v:,.2f}")
            df_gastos_detalhe_exibir["total"] = df_gastos_detalhe_exibir["total"].map(lambda v: f"R$ {v:,.2f}")
            st.dataframe(formatar_colunas_tabela(df_gastos_detalhe_exibir), use_container_width=True)
        else:
            st.info("Sem produtos consumidos por cliente no período selecionado.")

    elif tipo_relatorio == "Por produto":
        st.subheader("Produtos mais movimentados")
        if not df_rel.empty:
            mais_mov = df_rel.groupby("produto")["quantidade"].sum().reset_index().sort_values("quantidade", ascending=False)
            st.dataframe(formatar_colunas_tabela(mais_mov), use_container_width=True)
        else:
            st.info("Sem movimentações no período selecionado.")

        st.subheader("Produtos menos movimentados")
        st.dataframe(formatar_colunas_tabela(df_menos_mov), use_container_width=True)

    st.subheader("Histórico")
    hist_rel = df_rel.copy()
    if not hist_rel.empty:
        hist_rel["data"] = pd.to_datetime(hist_rel["data"], errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
    st.dataframe(formatar_colunas_tabela(hist_rel), use_container_width=True)


# =========================
# FROTAS
# =========================
elif menu == "FROTAS":
    st.title("FROTAS")

    if "subtela_frotas" not in st.session_state:
        st.session_state["subtela_frotas"] = "PAINEL"

    abas_frotas = ["PAINEL", "VEÍCULOS", "ABASTECIMENTOS", "MANUTENÇÕES", "CONFERÊNCIA", "DOCUMENTOS", "RELATÓRIOS"]
    nav_frotas = st.columns(len(abas_frotas))
    for idx, aba in enumerate(abas_frotas):
        if nav_frotas[idx].button(
            aba,
            type="primary" if st.session_state["subtela_frotas"] == aba else "secondary",
            use_container_width=True,
            key=f"frotas_nav_{aba}"
        ):
            st.session_state["subtela_frotas"] = aba
            st.rerun()

    subtela_frotas = st.session_state["subtela_frotas"]
    placas_ativas = df_frotas_veiculos[df_frotas_veiculos["status"] != "Inativo"]["placa"].dropna().astype(str).tolist()
    placas_ativas = [p for p in placas_ativas if p.strip()]
    alertas_preventiva = alertas_manutencao_preventiva(df_frotas_manutencoes)
    assinatura_alerta_frotas = assinatura_alertas_preventiva(alertas_preventiva)

    if not alertas_preventiva.empty and st.session_state.get("alerta_preventiva_ok") != assinatura_alerta_frotas:
        preventivas_vencidas = alertas_preventiva[alertas_preventiva["status"] == "Vencida"]
        preventivas_vencendo = alertas_preventiva[alertas_preventiva["status"] != "Vencida"]
        if not preventivas_vencidas.empty:
            placas_vencidas = ", ".join(preventivas_vencidas["placa"].astype(str).tolist())
            st.error(f"Manutenção preventiva vencida: {placas_vencidas}. Registrar execução da preventiva para encerrar o alerta.")
            st.dataframe(formatar_colunas_tabela(preventivas_vencidas), use_container_width=True, hide_index=True)
        if not preventivas_vencendo.empty:
            placas_vencendo = ", ".join(preventivas_vencendo["placa"].astype(str).tolist())
            st.warning(f"Manutenção preventiva vencendo em até 10 dias: {placas_vencendo}.")
            st.dataframe(formatar_colunas_tabela(preventivas_vencendo), use_container_width=True, hide_index=True)

    if subtela_frotas == "PAINEL":
        hoje = datetime.now().date()
        documentos_temp = df_frotas_documentos.copy()
        documentos_temp["vencimento_dt"] = pd.to_datetime(documentos_temp["vencimento"], errors="coerce").dt.date
        documentos_vencendo = int(((pd.to_datetime(documentos_temp["vencimento_dt"], errors="coerce") >= pd.to_datetime(hoje)) & (pd.to_datetime(documentos_temp["vencimento_dt"], errors="coerce") <= pd.to_datetime(hoje + timedelta(days=30)))).sum()) if not documentos_temp.empty else 0
        documentos_vencidos = int((pd.to_datetime(documentos_temp["vencimento_dt"], errors="coerce") < pd.to_datetime(hoje)).sum()) if not documentos_temp.empty else 0
        gasto_abastecimento = float(df_frotas_abastecimentos["valor_total"].sum()) if not df_frotas_abastecimentos.empty else 0
        gasto_manutencao = float(df_frotas_manutencoes["valor"].sum()) if not df_frotas_manutencoes.empty else 0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.markdown(f"<div class='metric-card'><div class='metric-label'>Veículos</div><div class='metric-value'>{len(df_frotas_veiculos)}</div></div>", unsafe_allow_html=True)
        c2.markdown(f"<div class='metric-card'><div class='metric-label'>Ativos</div><div class='metric-value' style='color:#22c55e'>{len(placas_ativas)}</div></div>", unsafe_allow_html=True)
        c3.markdown(f"<div class='metric-card'><div class='metric-label'>Documentos Vencidos</div><div class='metric-value' style='color:#ef4444'>{documentos_vencidos}</div></div>", unsafe_allow_html=True)
        c4.markdown(f"<div class='metric-card'><div class='metric-label'>Vencendo 30 Dias</div><div class='metric-value' style='color:#facc15'>{documentos_vencendo}</div></div>", unsafe_allow_html=True)
        c5.markdown(f"<div class='metric-card'><div class='metric-label'>Gasto Total</div><div class='metric-value' style='color:#38bdf8'>R$ {gasto_abastecimento + gasto_manutencao:,.2f}</div></div>", unsafe_allow_html=True)

        st.subheader("Veículos Cadastrados")
        st.dataframe(formatar_colunas_tabela(df_frotas_veiculos), use_container_width=True, hide_index=True)

    elif subtela_frotas == "VEÍCULOS":
        st.subheader("Veículos")
        acao_veiculo = st.radio("Ação", ["Adicionar", "Editar", "Inativar"], horizontal=True)
        if acao_veiculo == "Adicionar":
            placa = st.text_input("Placa").strip().upper()
            modelo = st.text_input("Modelo").strip().title()
            marca = st.text_input("Marca").strip().title()
            ano = st.number_input("Ano", min_value=1900, max_value=2100, value=datetime.now().year)
            tipo = st.selectbox("Tipo", ["Carro", "Moto", "Caminhão", "Van", "Utilitário", "Outro"])
            responsavel = st.text_input("Responsável").strip().title()
            cidade_local = st.text_input("Cidade / Local").strip().title()
            km_atual = st.text_input("Km Atual").strip()
            if st.button("ADICIONAR VEÍCULO"):
                if not placa:
                    st.error("Informe a placa.")
                elif placa in df_frotas_veiculos["placa"].astype(str).tolist():
                    st.error("Já existe veículo com esta placa.")
                else:
                    novo = pd.DataFrame([{"placa": placa, "modelo": modelo, "marca": marca, "ano": int(ano), "tipo": tipo, "responsavel": responsavel, "cidade_local": cidade_local, "status": "Ativo", "km_atual": km_atual}])
                    df_frotas_veiculos = pd.concat([df_frotas_veiculos, novo], ignore_index=True)
                    df_frotas_veiculos.to_excel(FROTAS_VEICULOS_XLSX, index=False)
                    st.success("Veículo adicionado.")
                    st.rerun()
        elif acao_veiculo == "Editar":
            if df_frotas_veiculos.empty:
                st.info("Nenhum veículo cadastrado.")
            else:
                placa_sel = st.selectbox("Veículo", df_frotas_veiculos["placa"].astype(str).tolist())
                dados = df_frotas_veiculos[df_frotas_veiculos["placa"].astype(str) == placa_sel].iloc[0]
                modelo = st.text_input("Modelo", str(dados.get("modelo", ""))).strip().title()
                marca = st.text_input("Marca", str(dados.get("marca", ""))).strip().title()
                ano = st.number_input("Ano", min_value=1900, max_value=2100, value=int(dados.get("ano", datetime.now().year)) if str(dados.get("ano", "")).isdigit() else datetime.now().year)
                tipo = st.text_input("Tipo", str(dados.get("tipo", ""))).strip().title()
                responsavel = st.text_input("Responsável", str(dados.get("responsavel", ""))).strip().title()
                cidade_local = st.text_input("Cidade / Local", str(dados.get("cidade_local", ""))).strip().title()
                status = st.selectbox("Status", ["Ativo", "Inativo"], index=0 if str(dados.get("status", "Ativo")) != "Inativo" else 1)
                km_atual = st.text_input("Km Atual", str(dados.get("km_atual", ""))).strip()
                if st.button("SALVAR VEÍCULO"):
                    linha = df_frotas_veiculos["placa"].astype(str) == placa_sel
                    df_frotas_veiculos.loc[linha, ["modelo", "marca", "ano", "tipo", "responsavel", "cidade_local", "status", "km_atual"]] = [modelo, marca, int(ano), tipo, responsavel, cidade_local, status, km_atual]
                    df_frotas_veiculos.to_excel(FROTAS_VEICULOS_XLSX, index=False)
                    st.success("Veículo atualizado.")
                    st.rerun()
        else:
            if placas_ativas:
                placa_inativar = st.selectbox("Veículo Para Inativar", placas_ativas)
                if st.button("INATIVAR VEÍCULO"):
                    df_frotas_veiculos.loc[df_frotas_veiculos["placa"].astype(str) == placa_inativar, "status"] = "Inativo"
                    df_frotas_veiculos.to_excel(FROTAS_VEICULOS_XLSX, index=False)
                    st.success("Veículo inativado.")
                    st.rerun()
            else:
                st.info("Nenhum veículo ativo.")
        st.dataframe(formatar_colunas_tabela(df_frotas_veiculos), use_container_width=True, hide_index=True)

    elif subtela_frotas == "ABASTECIMENTOS":
        st.subheader("Abastecimentos")
        if not placas_ativas:
            st.info("Cadastre um veículo ativo antes de registrar abastecimento.")
        else:
            data = st.date_input("Data", value=datetime.now().date(), key="abastecimento_data")
            placa = st.selectbox("Veículo", placas_ativas, key="abastecimento_placa")
            veiculo_lancamento = df_frotas_veiculos[df_frotas_veiculos["placa"].astype(str) == str(placa)]
            responsavel_padrao = str(veiculo_lancamento.iloc[0].get("responsavel", "")).strip().title() if not veiculo_lancamento.empty else ""
            responsavel_lancamento = st.text_input(
                "Responsável Pelo Lançamento",
                value=responsavel_padrao,
                key=f"abastecimento_responsavel_{placa}"
            ).strip().title()
            km = st.number_input("Km", min_value=0, value=0, key="abastecimento_km")
            combustivel = st.selectbox("Combustível", ["Gasolina", "Etanol", "Diesel", "GNV", "Outro"])
            litros = st.number_input("Litros", min_value=0.0, step=0.01, format="%.2f")
            valor_litro = st.number_input("Valor Por Litro", min_value=0.0, step=0.01, format="%.2f")
            valor_total = litros * valor_litro
            posto = st.text_input("Posto").strip().title()
            observacoes = st.text_area("Observações")
            st.write(f"Valor Total: R$ {valor_total:,.2f}")
            if st.button("SALVAR ABASTECIMENTO"):
                if not responsavel_lancamento:
                    st.error("Informe o responsável pelo lançamento.")
                else:
                    novo = pd.DataFrame([{"data": data.isoformat(), "placa": placa, "km": int(km), "combustivel": combustivel, "litros": float(litros), "valor_litro": float(valor_litro), "valor_total": float(valor_total), "posto": posto, "responsavel_lancamento": responsavel_lancamento, "registrado_em": datetime.now().strftime("%d/%m/%Y %H:%M"), "observacoes": observacoes.strip()}])
                    df_frotas_abastecimentos = pd.concat([df_frotas_abastecimentos, novo], ignore_index=True)
                    df_frotas_abastecimentos.to_excel(FROTAS_ABASTECIMENTOS_XLSX, index=False)
                    df_frotas_veiculos.loc[df_frotas_veiculos["placa"].astype(str) == placa, "km_atual"] = int(km)
                    df_frotas_veiculos.to_excel(FROTAS_VEICULOS_XLSX, index=False)
                    st.success("Abastecimento salvo.")
                    st.rerun()
        exibir_consulta_abastecimentos(df_frotas_abastecimentos)

    elif subtela_frotas == "MANUTENÇÕES":
        st.subheader("Manutenções")
        if not placas_ativas:
            st.info("Cadastre um veículo ativo antes de registrar manutenção.")
        else:
            if "tipo_lancamento_manutencao" not in st.session_state:
                st.session_state["tipo_lancamento_manutencao"] = "Atual"

            botao_atual, botao_programada = st.columns(2)
            if botao_atual.button(
                "MANUTENÇÃO ATUAL",
                type="primary" if st.session_state["tipo_lancamento_manutencao"] == "Atual" else "secondary",
                use_container_width=True
            ):
                st.session_state["tipo_lancamento_manutencao"] = "Atual"
                st.rerun()
            if botao_programada.button(
                "MANUTENÇÃO PROGRAMADA",
                type="primary" if st.session_state["tipo_lancamento_manutencao"] == "Programada" else "secondary",
                use_container_width=True
            ):
                st.session_state["tipo_lancamento_manutencao"] = "Programada"
                st.rerun()

            modo_manutencao = st.session_state["tipo_lancamento_manutencao"]
            placa = st.selectbox("Veículo", placas_ativas, key=f"manutencao_placa_{modo_manutencao}")
            veiculo_lancamento = df_frotas_veiculos[df_frotas_veiculos["placa"].astype(str) == str(placa)]
            responsavel_padrao = str(veiculo_lancamento.iloc[0].get("responsavel", "")).strip().title() if not veiculo_lancamento.empty else ""
            responsavel_lancamento = st.text_input(
                "Responsável Pelo Lançamento",
                value=responsavel_padrao,
                key=f"manutencao_responsavel_{modo_manutencao}_{placa}"
            ).strip().title()
            tipo_manutencao = st.selectbox("Tipo De Manutenção", ["Preventiva", "Corretiva"], key=f"tipo_manutencao_{modo_manutencao}")

            if modo_manutencao == "Atual":
                data = st.date_input("Data", value=datetime.now().date(), key="manutencao_data_atual")
                km = st.number_input("Km", min_value=0, value=0, key="manutencao_km_atual")
                servico_executado = st.text_input("Serviço Executado", key="servico_manutencao_atual").strip().title()
                fornecedor = st.text_input("Fornecedor/Oficina", key="fornecedor_manutencao_atual").strip().title()
                valor = st.number_input("Valor", min_value=0.0, step=0.01, format="%.2f", key="valor_manutencao_atual")
                observacoes = st.text_area("Observações", key="manutencao_obs_atual")
                if st.button("SALVAR MANUTENÇÃO ATUAL"):
                    if not responsavel_lancamento:
                        st.error("Informe o responsável pelo lançamento.")
                    else:
                        if tipo_manutencao == "Preventiva":
                            df_frotas_manutencoes = baixar_manutencoes_programadas(df_frotas_manutencoes, placa, data)
                        novo = pd.DataFrame([{
                            "data": data.isoformat(),
                            "placa": placa,
                            "tipo_manutencao": tipo_manutencao,
                            "km": int(km),
                            "servico_executado": servico_executado,
                            "fornecedor": fornecedor,
                            "valor": float(valor),
                            "manutencao_agendada": "",
                            "proxima_revisao": "",
                            "status_manutencao": "Executada",
                            "responsavel_lancamento": responsavel_lancamento,
                            "registrado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
                            "observacoes": observacoes.strip()
                        }])
                        df_frotas_manutencoes = pd.concat([df_frotas_manutencoes, novo], ignore_index=True)
                        df_frotas_manutencoes.to_excel(FROTAS_MANUTENCOES_XLSX, index=False)
                        st.success("Manutenção atual salva.")
                        st.rerun()
            else:
                manutencao_agendada = st.date_input("Manutenção Agendada", value=datetime.now().date() + timedelta(days=10), key="manutencao_agendada_programada")
                servico_executado = st.text_input("Serviço Programado", key="servico_manutencao_programada").strip().title()
                observacoes = st.text_area("Observações", key="manutencao_obs_programada")
                if st.button("SALVAR MANUTENÇÃO PROGRAMADA"):
                    if not responsavel_lancamento:
                        st.error("Informe o responsável pelo lançamento.")
                    else:
                        novo = pd.DataFrame([{
                            "data": datetime.now().date().isoformat(),
                            "placa": placa,
                            "tipo_manutencao": tipo_manutencao,
                            "km": 0,
                            "servico_executado": servico_executado,
                            "fornecedor": "",
                            "valor": 0.0,
                            "manutencao_agendada": manutencao_agendada.isoformat(),
                            "proxima_revisao": "",
                            "status_manutencao": "Programada",
                            "responsavel_lancamento": responsavel_lancamento,
                            "registrado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
                            "observacoes": observacoes.strip()
                        }])
                        df_frotas_manutencoes = pd.concat([df_frotas_manutencoes, novo], ignore_index=True)
                        df_frotas_manutencoes.to_excel(FROTAS_MANUTENCOES_XLSX, index=False)
                        st.success("Manutenção programada salva.")
                        st.rerun()
        st.dataframe(formatar_colunas_tabela(df_frotas_manutencoes), use_container_width=True, hide_index=True)

    elif subtela_frotas == "CONFERÊNCIA":
        st.subheader("Conferência De Despesas Recebidas")
        pend_abast = int((df_frotas_abastecimentos["status_conferencia"].astype(str) == "Pendente").sum()) if not df_frotas_abastecimentos.empty else 0
        pend_manut = int((df_frotas_manutencoes["status_conferencia"].astype(str) == "Pendente").sum()) if not df_frotas_manutencoes.empty else 0
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"<div class='metric-card'><div class='metric-label'>Abastecimentos Pendentes</div><div class='metric-value'>{pend_abast}</div></div>", unsafe_allow_html=True)
        c2.markdown(f"<div class='metric-card'><div class='metric-label'>Manutenções Pendentes</div><div class='metric-value'>{pend_manut}</div></div>", unsafe_allow_html=True)
        c3.markdown(f"<div class='metric-card'><div class='metric-label'>Total Pendente</div><div class='metric-value'>{pend_abast + pend_manut}</div></div>", unsafe_allow_html=True)

        tab_abastecimentos, tab_manutencoes = st.tabs(["ABASTECIMENTOS", "MANUTENÇÕES"])
        with tab_abastecimentos:
            exibir_conferencia_lancamentos("Abastecimento", df_frotas_abastecimentos, FROTAS_ABASTECIMENTOS_XLSX)
        with tab_manutencoes:
            exibir_conferencia_lancamentos("Manutenção", df_frotas_manutencoes, FROTAS_MANUTENCOES_XLSX)

    elif subtela_frotas == "DOCUMENTOS":
        st.subheader("Documentos")
        if not placas_ativas:
            st.info("Cadastre um veículo ativo antes de registrar documento.")
        else:
            placa = st.selectbox("Veículo", placas_ativas, key="documento_placa")
            documento = st.selectbox("Documento", ["Licenciamento", "Seguro", "IPVA", "Multa", "Outro"])
            vencimento = st.date_input("Vencimento", value=datetime.now().date() + timedelta(days=30))
            valor = st.number_input("Valor", min_value=0.0, step=0.01, format="%.2f", key="documento_valor")
            status = st.selectbox("Status", ["Ativo", "Pago", "Pendente", "Vencido"])
            observacoes = st.text_area("Observações", key="documento_obs")
            if st.button("SALVAR DOCUMENTO"):
                novo = pd.DataFrame([{"placa": placa, "documento": documento, "vencimento": vencimento.isoformat(), "valor": float(valor), "status": status, "observacoes": observacoes.strip()}])
                df_frotas_documentos = pd.concat([df_frotas_documentos, novo], ignore_index=True)
                df_frotas_documentos.to_excel(FROTAS_DOCUMENTOS_XLSX, index=False)
                st.success("Documento salvo.")
                st.rerun()
        st.dataframe(formatar_colunas_tabela(df_frotas_documentos), use_container_width=True, hide_index=True)

    elif subtela_frotas == "RELATÓRIOS":
        st.subheader("Relatórios De Frotas")
        placas_rel = ["Todos"] + sorted(df_frotas_veiculos["placa"].dropna().astype(str).unique().tolist())
        placa_rel = st.selectbox("Veículo", placas_rel)
        abast_rel = df_frotas_abastecimentos.copy()
        manut_rel = df_frotas_manutencoes.copy()
        if placa_rel != "Todos":
            abast_rel = abast_rel[abast_rel["placa"].astype(str) == placa_rel]
            manut_rel = manut_rel[manut_rel["placa"].astype(str) == placa_rel]
        gasto_abast = float(abast_rel["valor_total"].sum()) if not abast_rel.empty else 0
        gasto_manut = float(manut_rel["valor"].sum()) if not manut_rel.empty else 0
        r1, r2, r3 = st.columns(3)
        r1.markdown(f"<div class='metric-card'><div class='metric-label'>Abastecimentos</div><div class='metric-value'>R$ {gasto_abast:,.2f}</div></div>", unsafe_allow_html=True)
        r2.markdown(f"<div class='metric-card'><div class='metric-label'>Manutenções</div><div class='metric-value'>R$ {gasto_manut:,.2f}</div></div>", unsafe_allow_html=True)
        r3.markdown(f"<div class='metric-card'><div class='metric-label'>Total</div><div class='metric-value'>R$ {gasto_abast + gasto_manut:,.2f}</div></div>", unsafe_allow_html=True)
        exibir_consulta_abastecimentos(abast_rel, "Abastecimentos")
        st.subheader("Manutenções")
        st.dataframe(formatar_colunas_tabela(manut_rel), use_container_width=True, hide_index=True)

# =========================
# PATRIMONIO
# =========================
elif menu == "PATRIMÔNIO":
    st.title("PATRIMÔNIO")

    if "subtela_patrimonio" not in st.session_state:
        st.session_state["subtela_patrimonio"] = "CADASTRO"

    abas_patrimonio = ["CADASTRO", "INSUMOS DA BASE", "CUSTOS", "TRANSFERÊNCIAS", "RELATÓRIOS"]
    nav_patrimonio = st.columns(len(abas_patrimonio))
    for idx, aba in enumerate(abas_patrimonio):
        if nav_patrimonio[idx].button(
            aba,
            type="primary" if st.session_state["subtela_patrimonio"] == aba else "secondary",
            use_container_width=True,
            key=f"patrimonio_nav_{aba}"
        ):
            st.session_state["subtela_patrimonio"] = aba
            st.rerun()

    subtela_patrimonio = st.session_state["subtela_patrimonio"]
    bases_patrimonio = BASES_FREQUENCIA
    equipamentos_ativos = df_patrimonio[df_patrimonio["status"].astype(str) != "Baixado"].copy()
    codigos_patrimonio = equipamentos_ativos["codigo"].dropna().astype(str).tolist()

    if subtela_patrimonio == "CADASTRO":
        st.subheader("Cadastrar Patrimônio")
        codigo = proximo_codigo_patrimonio()
        c1, c2, c3 = st.columns(3)
        c1.text_input("Código Patrimonial", value=codigo, disabled=True)
        nome = c2.text_input("Nome Do Item", key="pat_nome").strip().upper()
        tipo = c3.selectbox("Tipo", ["Roçadeira", "Soprador", "Motosserra", "Cortador De Grama", "Ferramenta", "Máquina", "Outro"])
        c4, c5, c6 = st.columns(3)
        marca = c4.text_input("Marca", key="pat_marca").strip().upper()
        modelo = c5.text_input("Modelo", key="pat_modelo").strip().upper()
        serie = c6.text_input("Número De Série", key="pat_serie").strip().upper()
        c7, c8, c9 = st.columns(3)
        base_item = c7.selectbox("Base / Local", bases_patrimonio, key="pat_base")
        local_item = c8.text_input("Local Detalhado", key="pat_local").strip().upper()
        responsavel = c9.text_input("Responsável", key="pat_responsavel").strip().upper()
        c10, c11, c12 = st.columns(3)
        data_aquisicao = c10.date_input("Data De Aquisição", value=datetime.now().date(), key="pat_data")
        valor_compra = c11.number_input("Valor De Compra", min_value=0.0, step=0.01, format="%.2f", key="pat_valor")
        status = c12.selectbox("Status", ["Ativo", "Manutenção", "Baixado", "Extraviado"], key="pat_status")
        observacoes = st.text_area("Observações", key="pat_obs").strip()

        if st.button("SALVAR PATRIMÔNIO", type="primary"):
            if not nome:
                st.error("Informe o nome do item.")
            else:
                novo = pd.DataFrame([{
                    "codigo": codigo,
                    "nome": nome,
                    "tipo": tipo,
                    "marca": marca,
                    "modelo": modelo,
                    "serie": serie,
                    "base": base_item,
                    "local": local_item,
                    "responsavel": responsavel,
                    "data_aquisicao": data_aquisicao.isoformat(),
                    "valor_compra": float(valor_compra),
                    "status": status,
                    "observacoes": observacoes
                }])
                df_patrimonio = pd.concat([df_patrimonio, novo], ignore_index=True)
                df_patrimonio.to_excel(PATRIMONIO_XLSX, index=False)
                st.success("Patrimônio cadastrado.")
                st.rerun()

        st.subheader("Patrimônios Cadastrados")
        st.dataframe(formatar_colunas_tabela(df_patrimonio), use_container_width=True, hide_index=True)

    elif subtela_patrimonio == "INSUMOS DA BASE":
        st.subheader("Insumos Da Base")
        saldo_atual_insumos = saldo_insumos_base(df_patrimonio_insumos)

        c1, c2, c3 = st.columns(3)
        data_insumo = c1.date_input("Data", value=datetime.now().date(), key="insumo_data")
        base_insumo = c2.selectbox("Base", bases_patrimonio, key="insumo_base")
        tipo_mov_insumo = c3.selectbox("Tipo De Movimento", ["Entrada", "Saída"], key="insumo_tipo_mov")
        c4, c5, c6 = st.columns(3)
        insumo = c4.selectbox("Insumo", ["Gasolina", "Óleo 2 Tempos", "Óleo Lubrificante", "Graxa", "Outro"], key="insumo_nome")
        quantidade_insumo = c5.number_input("Quantidade", min_value=0.0, step=0.1, format="%.2f", key="insumo_quantidade")
        unidade_insumo = c6.selectbox("Unidade", ["L", "ML", "UN"], key="insumo_unidade")
        c7, c8, c9 = st.columns(3)
        valor_unitario_insumo = c7.number_input("Valor Unitário", min_value=0.0, step=0.01, format="%.2f", key="insumo_valor_unitario")
        valor_total_insumo = float(quantidade_insumo) * float(valor_unitario_insumo)
        c8.metric("Valor Total", f"R$ {valor_total_insumo:,.2f}")
        patrimonio_opcoes = ["Uso Geral Da Base"] + codigos_patrimonio
        codigo_insumo = c9.selectbox("Equipamento Opcional", patrimonio_opcoes, key="insumo_codigo")
        patrimonio_nome = ""
        if codigo_insumo != "Uso Geral Da Base":
            item_insumo = equipamentos_ativos[equipamentos_ativos["codigo"].astype(str) == codigo_insumo]
            patrimonio_nome = str(item_insumo.iloc[0].get("nome", "")) if not item_insumo.empty else ""
            st.caption(f"Equipamento: {patrimonio_nome}")
        operador_insumo = st.text_input("Operador / Responsável", key="insumo_operador").strip().upper()
        obs_insumo = st.text_area("Observações", key="insumo_obs").strip()

        saldo_linha = saldo_atual_insumos[
            (saldo_atual_insumos["base"] == base_insumo)
            & (saldo_atual_insumos["insumo"] == insumo)
            & (saldo_atual_insumos["unidade"] == unidade_insumo)
        ]
        saldo_disponivel = float(saldo_linha["saldo"].iloc[0]) if not saldo_linha.empty else 0
        st.info(f"Saldo Atual Em {base_insumo}: {saldo_disponivel:,.2f} {unidade_insumo}")

        if st.button("SALVAR MOVIMENTO DE INSUMO", type="primary"):
            if quantidade_insumo <= 0:
                st.error("Informe uma quantidade maior que zero.")
            elif tipo_mov_insumo == "Entrada" and valor_unitario_insumo <= 0:
                st.error("Informe o valor unitário da entrada.")
            elif tipo_mov_insumo == "Saída" and quantidade_insumo > saldo_disponivel:
                st.error("Saldo insuficiente para essa saída.")
            else:
                novo_insumo = pd.DataFrame([{
                    "data": data_insumo.isoformat(),
                    "base": base_insumo,
                    "insumo": insumo,
                    "tipo_movimentacao": tipo_mov_insumo,
                    "quantidade": float(quantidade_insumo),
                    "unidade": unidade_insumo,
                    "valor_unitario": float(valor_unitario_insumo),
                    "valor_total": valor_total_insumo,
                    "codigo": "" if codigo_insumo == "Uso Geral Da Base" else codigo_insumo,
                    "patrimonio": patrimonio_nome,
                    "operador": operador_insumo,
                    "observacoes": obs_insumo
                }])
                df_patrimonio_insumos = pd.concat([df_patrimonio_insumos, novo_insumo], ignore_index=True)
                df_patrimonio_insumos.to_excel(PATRIMONIO_INSUMOS_XLSX, index=False)
                st.success("Movimento de insumo salvo.")
                st.rerun()

        st.subheader("Saldo De Insumos Por Base")
        st.dataframe(formatar_colunas_tabela(saldo_atual_insumos), use_container_width=True, hide_index=True)
        st.subheader("Histórico De Insumos")
        st.dataframe(formatar_colunas_tabela(df_patrimonio_insumos), use_container_width=True, hide_index=True)

    elif subtela_patrimonio == "CUSTOS":
        st.subheader("Lançar Custo Do Patrimônio")
        if equipamentos_ativos.empty:
            st.info("Cadastre um patrimônio ativo antes de lançar custos.")
        else:
            c1, c2, c3 = st.columns(3)
            data_custo = c1.date_input("Data", value=datetime.now().date(), key="pat_custo_data")
            codigo_sel = c2.selectbox("Patrimônio", codigos_patrimonio, key="pat_custo_codigo")
            item_sel = equipamentos_ativos[equipamentos_ativos["codigo"].astype(str) == codigo_sel].iloc[0]
            c3.text_input("Base", value=str(item_sel.get("base", "")), disabled=True)
            st.caption(f"Item: {item_sel.get('nome', '')}")
            c4, c5, c6 = st.columns(3)
            tipo_custo = c4.selectbox("Tipo De Custo", ["Combustível", "Óleo 2 Tempos", "Óleo Lubrificante", "Peças", "Manutenção", "Acessórios", "Outro"], key="pat_tipo_custo")
            quantidade = c5.number_input("Quantidade", min_value=0.0, step=0.1, format="%.2f", key="pat_qtd")
            unidade = c6.selectbox("Unidade", ["L", "ML", "UN", "R$"], key="pat_unidade")
            c7, c8, c9 = st.columns(3)
            valor_unitario = c7.number_input("Valor Unitário", min_value=0.0, step=0.01, format="%.2f", key="pat_vunit")
            valor_total = float(quantidade) * float(valor_unitario)
            c8.metric("Valor Total", f"R$ {valor_total:,.2f}")
            fornecedor_custo = c9.text_input("Fornecedor", key="pat_fornecedor").strip().upper()
            operador = st.text_input("Operador / Responsável Pelo Uso", key="pat_operador").strip().upper()
            obs_custo = st.text_area("Observações", key="pat_custo_obs").strip()

            if st.button("SALVAR CUSTO", type="primary"):
                if quantidade <= 0 or valor_total <= 0:
                    st.error("Informe quantidade e valor unitário maiores que zero.")
                else:
                    novo_custo = pd.DataFrame([{
                        "data": data_custo.isoformat(),
                        "codigo": codigo_sel,
                        "patrimonio": item_sel.get("nome", ""),
                        "base": item_sel.get("base", ""),
                        "tipo_custo": tipo_custo,
                        "quantidade": float(quantidade),
                        "unidade": unidade,
                        "valor_unitario": float(valor_unitario),
                        "valor_total": valor_total,
                        "fornecedor": fornecedor_custo,
                        "operador": operador,
                        "observacoes": obs_custo
                    }])
                    df_patrimonio_custos = pd.concat([df_patrimonio_custos, novo_custo], ignore_index=True)
                    df_patrimonio_custos.to_excel(PATRIMONIO_CUSTOS_XLSX, index=False)
                    st.success("Custo lançado.")
                    st.rerun()

        st.subheader("Histórico De Custos")
        st.dataframe(formatar_colunas_tabela(df_patrimonio_custos), use_container_width=True, hide_index=True)

    elif subtela_patrimonio == "TRANSFERÊNCIAS":
        st.subheader("Transferir Patrimônio")
        if equipamentos_ativos.empty:
            st.info("Cadastre um patrimônio ativo antes de transferir.")
        else:
            codigo_mov = st.selectbox("Patrimônio", codigos_patrimonio, key="pat_mov_codigo")
            linha = df_patrimonio["codigo"].astype(str) == codigo_mov
            item_mov = df_patrimonio[linha].iloc[0]
            c1, c2, c3 = st.columns(3)
            data_mov = c1.date_input("Data", value=datetime.now().date(), key="pat_mov_data")
            c2.text_input("Base Origem", value=str(item_mov.get("base", "")), disabled=True)
            base_destino = c3.selectbox("Base Destino", bases_patrimonio, key="pat_base_destino")
            c4, c5 = st.columns(2)
            c4.text_input("Responsável Origem", value=str(item_mov.get("responsavel", "")), disabled=True)
            responsavel_destino = c5.text_input("Responsável Destino", key="pat_resp_destino").strip().upper()
            tipo_mov = st.selectbox("Tipo De Movimentação", ["Transferência De Base", "Troca De Responsável", "Envio Para Manutenção", "Retorno De Manutenção", "Baixa"], key="pat_tipo_mov")
            obs_mov = st.text_area("Observações", key="pat_mov_obs").strip()

            if st.button("SALVAR MOVIMENTAÇÃO", type="primary"):
                novo_mov = pd.DataFrame([{
                    "data": data_mov.isoformat(),
                    "codigo": codigo_mov,
                    "patrimonio": item_mov.get("nome", ""),
                    "base_origem": item_mov.get("base", ""),
                    "base_destino": base_destino,
                    "responsavel_origem": item_mov.get("responsavel", ""),
                    "responsavel_destino": responsavel_destino,
                    "tipo_movimentacao": tipo_mov,
                    "observacoes": obs_mov
                }])
                df_patrimonio_movimentacoes = pd.concat([df_patrimonio_movimentacoes, novo_mov], ignore_index=True)
                if tipo_mov == "Baixa":
                    df_patrimonio.loc[linha, "status"] = "Baixado"
                elif tipo_mov == "Envio Para Manutenção":
                    df_patrimonio.loc[linha, "status"] = "Manutenção"
                elif tipo_mov == "Retorno De Manutenção":
                    df_patrimonio.loc[linha, "status"] = "Ativo"
                df_patrimonio.loc[linha, "base"] = base_destino
                if responsavel_destino:
                    df_patrimonio.loc[linha, "responsavel"] = responsavel_destino
                df_patrimonio.to_excel(PATRIMONIO_XLSX, index=False)
                df_patrimonio_movimentacoes.to_excel(PATRIMONIO_MOVIMENTACOES_XLSX, index=False)
                st.success("Movimentação salva.")
                st.rerun()

        st.subheader("Histórico De Movimentações")
        st.dataframe(formatar_colunas_tabela(df_patrimonio_movimentacoes), use_container_width=True, hide_index=True)

    else:
        st.subheader("Relatórios De Patrimônio")
        f1, f2, f3 = st.columns(3)
        base_rel = f1.selectbox("Base", ["Todas"] + bases_patrimonio, key="pat_rel_base")
        tipo_rel = f2.selectbox("Tipo De Custo", ["Todos"] + sorted(df_patrimonio_custos["tipo_custo"].dropna().astype(str).unique().tolist()), key="pat_rel_tipo")
        status_rel = f3.selectbox("Status", ["Todos"] + sorted(df_patrimonio["status"].dropna().astype(str).unique().tolist()), key="pat_rel_status")

        patrimonio_rel = df_patrimonio.copy()
        custos_rel = df_patrimonio_custos.copy()
        insumos_rel = df_patrimonio_insumos.copy()
        if base_rel != "Todas":
            patrimonio_rel = patrimonio_rel[patrimonio_rel["base"] == base_rel]
            custos_rel = custos_rel[custos_rel["base"] == base_rel]
            insumos_rel = insumos_rel[insumos_rel["base"] == base_rel]
        if tipo_rel != "Todos":
            custos_rel = custos_rel[custos_rel["tipo_custo"] == tipo_rel]
        if status_rel != "Todos":
            patrimonio_rel = patrimonio_rel[patrimonio_rel["status"] == status_rel]

        total_itens = len(patrimonio_rel)
        total_custos = float(custos_rel["valor_total"].sum()) if not custos_rel.empty else 0
        total_insumos = float(insumos_rel[insumos_rel["tipo_movimentacao"] == "Entrada"]["valor_total"].sum()) if not insumos_rel.empty else 0
        valor_patrimonio = float(patrimonio_rel["valor_compra"].sum()) if not patrimonio_rel.empty else 0
        manutencao = len(patrimonio_rel[patrimonio_rel["status"] == "Manutenção"]) if not patrimonio_rel.empty else 0
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.markdown(f"<div class='metric-card'><div class='metric-label'>Itens</div><div class='metric-value'>{total_itens}</div></div>", unsafe_allow_html=True)
        m2.markdown(f"<div class='metric-card'><div class='metric-label'>Valor Patrimonial</div><div class='metric-value'>R$ {valor_patrimonio:,.2f}</div></div>", unsafe_allow_html=True)
        m3.markdown(f"<div class='metric-card'><div class='metric-label'>Custos</div><div class='metric-value' style='color:#facc15'>R$ {total_custos:,.2f}</div></div>", unsafe_allow_html=True)
        m4.markdown(f"<div class='metric-card'><div class='metric-label'>Em Manutenção</div><div class='metric-value' style='color:#ef4444'>{manutencao}</div></div>", unsafe_allow_html=True)
        m5.markdown(f"<div class='metric-card'><div class='metric-label'>Insumos</div><div class='metric-value' style='color:#38bdf8'>R$ {total_insumos:,.2f}</div></div>", unsafe_allow_html=True)

        custos_por_item = custos_rel.groupby(["codigo", "patrimonio"], as_index=False)["valor_total"].sum().sort_values("valor_total", ascending=False) if not custos_rel.empty else pd.DataFrame(columns=["codigo", "patrimonio", "valor_total"])
        custos_por_base = custos_rel.groupby("base", as_index=False)["valor_total"].sum() if not custos_rel.empty else pd.DataFrame(columns=["base", "valor_total"])
        custos_por_tipo = custos_rel.groupby("tipo_custo", as_index=False)["valor_total"].sum() if not custos_rel.empty else pd.DataFrame(columns=["tipo_custo", "valor_total"])
        saldo_rel = saldo_insumos_base(insumos_rel)

        if not custos_por_item.empty:
            st.subheader("Top Equipamentos Que Mais Gastam")
            st.bar_chart(custos_por_item.set_index("patrimonio")["valor_total"])
        if not custos_por_base.empty:
            st.subheader("Gasto Por Base")
            st.bar_chart(custos_por_base.set_index("base")["valor_total"])

        st.subheader("Gasto Por Tipo")
        st.dataframe(formatar_colunas_tabela(custos_por_tipo), use_container_width=True, hide_index=True)
        st.subheader("Saldo De Insumos")
        st.dataframe(formatar_colunas_tabela(saldo_rel), use_container_width=True, hide_index=True)
        st.subheader("Histórico De Insumos")
        st.dataframe(formatar_colunas_tabela(insumos_rel), use_container_width=True, hide_index=True)
        st.subheader("Gasto Por Patrimônio")
        st.dataframe(formatar_colunas_tabela(custos_por_item), use_container_width=True, hide_index=True)
        st.subheader("Patrimônios")
        st.dataframe(formatar_colunas_tabela(patrimonio_rel), use_container_width=True, hide_index=True)


# =========================
# ORCAMENTOS
# =========================
elif menu == "ORÇAMENTOS":
    st.title("ORÇAMENTOS")

    if "subtela_orcamentos" not in st.session_state:
        st.session_state["subtela_orcamentos"] = "NOVO ORÇAMENTO"

    abas_orcamentos = ["NOVO ORÇAMENTO", "EM ABERTO", "APROVADOS", "REPROVADOS", "RELATÓRIOS"]
    nav_orcamentos = st.columns(len(abas_orcamentos))
    for idx, aba in enumerate(abas_orcamentos):
        if nav_orcamentos[idx].button(aba, type="primary" if st.session_state["subtela_orcamentos"] == aba else "secondary", use_container_width=True, key=f"orcamentos_nav_{aba}"):
            st.session_state["subtela_orcamentos"] = aba
            st.rerun()

    subtela_orcamentos = st.session_state["subtela_orcamentos"]
    clientes_orcamento = ["Não Informado"] + sorted(df_clientes[df_clientes["status"] != "Inativo"]["nome_cliente"].dropna().astype(str).tolist()) if not df_clientes.empty else ["Não Informado"]
    fornecedores_orcamento = ["Não Informado"] + sorted(df_fornecedores[df_fornecedores["status"] != "Inativo"]["nome_fornecedor"].dropna().astype(str).tolist()) if not df_fornecedores.empty else ["Não Informado"]
    veiculos_orcamento = ["Não Informado"] + sorted(df_frotas_veiculos[df_frotas_veiculos["status"] != "Inativo"]["placa"].dropna().astype(str).tolist()) if not df_frotas_veiculos.empty else ["Não Informado"]
    status_opcoes_orcamento = ["Em Aberto", "Aprovado", "Reprovado", "Cancelado"]

    if subtela_orcamentos == "NOVO ORÇAMENTO":
        numero = proximo_numero_orcamento()
        st.text_input("Número Do Orçamento", value=numero, disabled=True)
        data_orcamento = st.date_input("Data Do Orçamento", value=datetime.now().date(), key="orcamento_data")
        validade = st.date_input("Validade", value=datetime.now().date() + timedelta(days=15), key="orcamento_validade")
        c1, c2, c3 = st.columns(3)
        cliente = c1.selectbox("Cliente", clientes_orcamento)
        fornecedor = c2.selectbox("Fornecedor", fornecedores_orcamento)
        veiculo = c3.selectbox("Veículo", veiculos_orcamento)
        tipo = st.selectbox("Tipo", ["Serviço", "Produto", "Manutenção", "Outro"])
        descricao = st.text_area("Descrição").strip()
        q1, q2, q3 = st.columns(3)
        quantidade = q1.number_input("Quantidade", min_value=1.0, value=1.0, step=1.0)
        valor_unitario = q2.number_input("Valor Unitário", min_value=0.0, step=0.01, format="%.2f")
        valor_total = float(quantidade) * float(valor_unitario)
        q3.metric("Valor Total", f"R$ {valor_total:,.2f}")
        anexo_orcamento = st.file_uploader(
            "Anexar Orçamento",
            type=["pdf", "png", "jpg", "jpeg", "xlsx", "xls", "docx", "doc"]
        )
        observacoes = st.text_area("Observações", key="orcamento_observacoes").strip()
        if st.button("SALVAR ORÇAMENTO", type="primary"):
            if not descricao:
                st.error("Informe a descrição do orçamento.")
            elif valor_total <= 0:
                st.error("Informe um valor unitário maior que zero.")
            else:
                caminho_anexo = ""
                if anexo_orcamento is not None:
                    os.makedirs(PASTA_ANEXOS_ORCAMENTOS, exist_ok=True)
                    nome_seguro = "".join(c if c.isalnum() or c in "._- " else "_" for c in anexo_orcamento.name)
                    caminho_anexo = os.path.join(PASTA_ANEXOS_ORCAMENTOS, f"{numero}_{nome_seguro}")
                    with open(caminho_anexo, "wb") as arquivo:
                        arquivo.write(anexo_orcamento.getbuffer())
                    upload_arquivo_remoto(caminho_anexo)
                novo = pd.DataFrame([{"numero": numero, "data": data_orcamento.isoformat(), "validade": validade.isoformat(), "cliente": "" if cliente == "Não Informado" else cliente, "fornecedor": "" if fornecedor == "Não Informado" else fornecedor, "veiculo": "" if veiculo == "Não Informado" else veiculo, "tipo": tipo, "descricao": descricao, "quantidade": float(quantidade), "valor_unitario": float(valor_unitario), "valor_total": valor_total, "status": "Em Aberto", "anexo": caminho_anexo, "observacoes": observacoes}])
                df_orcamentos = pd.concat([df_orcamentos, novo], ignore_index=True)
                df_orcamentos.to_excel(ORCAMENTOS_XLSX, index=False)
                st.success("Orçamento salvo.")
                st.rerun()

    elif subtela_orcamentos in ["EM ABERTO", "APROVADOS", "REPROVADOS"]:
        status_filtro = {"EM ABERTO": "Em Aberto", "APROVADOS": "Aprovado", "REPROVADOS": "Reprovado"}[subtela_orcamentos]
        dados_status = df_orcamentos[df_orcamentos["status"] == status_filtro].copy()
        st.subheader(subtela_orcamentos.title())
        if dados_status.empty:
            st.info("Nenhum orçamento encontrado.")
        else:
            numero_sel = st.selectbox("Orçamento", dados_status["numero"].astype(str).tolist())
            linha = df_orcamentos["numero"].astype(str) == numero_sel
            status_atual = str(df_orcamentos.loc[linha, "status"].iloc[0])
            novo_status = st.selectbox("Status", status_opcoes_orcamento, index=status_opcoes_orcamento.index(status_atual) if status_atual in status_opcoes_orcamento else 0)
            if st.button("SALVAR STATUS"):
                df_orcamentos.loc[linha, "status"] = novo_status
                df_orcamentos.to_excel(ORCAMENTOS_XLSX, index=False)
                st.success("Status atualizado.")
                st.rerun()
            st.dataframe(formatar_colunas_tabela(dados_status), use_container_width=True, hide_index=True)

    else:
        st.subheader("Relatórios De Orçamentos")
        total_orcamentos = len(df_orcamentos)
        valor_aberto = float(df_orcamentos[df_orcamentos["status"] == "Em Aberto"]["valor_total"].sum()) if not df_orcamentos.empty else 0
        valor_aprovado = float(df_orcamentos[df_orcamentos["status"] == "Aprovado"]["valor_total"].sum()) if not df_orcamentos.empty else 0
        valor_reprovado = float(df_orcamentos[df_orcamentos["status"] == "Reprovado"]["valor_total"].sum()) if not df_orcamentos.empty else 0
        r1, r2, r3, r4 = st.columns(4)
        r1.markdown(f"<div class='metric-card'><div class='metric-label'>Orçamentos</div><div class='metric-value'>{total_orcamentos}</div></div>", unsafe_allow_html=True)
        r2.markdown(f"<div class='metric-card'><div class='metric-label'>Em Aberto</div><div class='metric-value' style='color:#facc15'>R$ {valor_aberto:,.2f}</div></div>", unsafe_allow_html=True)
        r3.markdown(f"<div class='metric-card'><div class='metric-label'>Aprovados</div><div class='metric-value' style='color:#22c55e'>R$ {valor_aprovado:,.2f}</div></div>", unsafe_allow_html=True)
        r4.markdown(f"<div class='metric-card'><div class='metric-label'>Reprovados</div><div class='metric-value' style='color:#ef4444'>R$ {valor_reprovado:,.2f}</div></div>", unsafe_allow_html=True)
        resumo_status = df_orcamentos.groupby("status", as_index=False)["valor_total"].sum() if not df_orcamentos.empty else pd.DataFrame(columns=["status", "valor_total"])
        if not resumo_status.empty:
            st.bar_chart(resumo_status.set_index("status"))
        else:
            st.info("Sem orçamentos cadastrados.")
        st.dataframe(formatar_colunas_tabela(df_orcamentos), use_container_width=True, hide_index=True)


# =========================
# CONFIGURACOES
# =========================
elif menu == "CONFIGURAÇÕES":
    st.title("CONFIGURAÇÕES")

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

    usuario_atual = st.session_state.get("usuario_logado", {})
    admin_logado = usuario_atual.get("nivel") == "Administrador"

    if admin_logado:
        tab_geral, tab_usuarios, tab_estoque, tab_categorias, tab_unidades, tab_aparencia, tab_backup = st.tabs([
            "GERAL", "USUÁRIOS", "ESTOQUE", "CATEGORIAS", "UNIDADE", "APARÊNCIA", "BACKUP"
        ])
    else:
        tab_usuarios, tab_aparencia, tab_backup = st.tabs([
            "USUÁRIOS", "APARÊNCIA", "BACKUP"
        ])

    if admin_logado:
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
                        upload_arquivo_remoto(logo_path) if os.path.abspath(logo_path).startswith(os.path.abspath(DATA_DIR)) else None
                        config["logo"] = logo_path
                    salvar_json(CONFIG_JSON, config)
                    st.success("Configurações gerais salvas.")

    with tab_usuarios:
        usuarios = carregar_json(USUARIOS_JSON, [])
        st.write(f"Usuário: {usuario_atual.get('nome', '')}")
        st.write(f"Nível: {usuario_atual.get('nivel', '')}")

        with st.form("alterar_senha_usuario"):
            senha_atual = st.text_input("Senha atual", type="password")
            nova_senha = st.text_input("Nova senha", type="password")
            confirmar_senha = st.text_input("Confirmar nova senha", type="password")
            if st.form_submit_button("Alterar senha"):
                usuario_encontrado = next(
                    (
                        u for u in usuarios
                        if u.get("nome") == usuario_atual.get("nome")
                        or u.get("email") == usuario_atual.get("email")
                    ),
                    None
                )
                if not usuario_encontrado:
                    st.error("Usuário logado não encontrado.")
                elif usuario_encontrado.get("senha") != hash_senha(senha_atual):
                    st.error("Senha atual incorreta.")
                elif not nova_senha:
                    st.error("Informe a nova senha.")
                elif nova_senha != confirmar_senha:
                    st.error("A confirmação da senha não confere.")
                else:
                    usuario_encontrado["senha"] = hash_senha(nova_senha)
                    salvar_json(USUARIOS_JSON, usuarios)
                    st.success("Senha alterada com sucesso.")

        if admin_logado:
            st.divider()
            st.subheader("Gerenciar usuários")
            st.dataframe(pd.DataFrame([{k: v for k, v in u.items() if k != "senha"} for u in usuarios]), use_container_width=True)

            niveis_usuario = ["Administrador", "Usuário", "Supervisor Base", "Responsável Frota"]
            veiculos_ativos_usuarios = sorted(df_frotas_veiculos[df_frotas_veiculos["status"] != "Inativo"]["placa"].dropna().astype(str).tolist())
            responsaveis_frota = sorted(set(
                nome
                for responsavel in df_frotas_veiculos["responsavel"].dropna().astype(str).tolist()
                for nome in nomes_responsaveis_frota(responsavel)
            ))
            acao_usuario = st.radio("Ação", ["Criar", "Editar", "Excluir"], horizontal=True)
            if acao_usuario == "Criar":
                nivel = st.selectbox("Nível", niveis_usuario, key="nivel_criar_usuario")
                bases_permitidas = st.multiselect(
                    "Bases Permitidas",
                    BASES_FREQUENCIA,
                    default=BASES_FREQUENCIA if nivel == "Administrador" else ["TMG BASE SORRISO"] if nivel == "Supervisor Base" else [],
                    key="bases_criar_usuario"
                )
                pode_lancar_despesa_frota = st.checkbox(
                    "Permitir Lançamento De Despesas De Frota",
                    value=nivel in ["Administrador", "Supervisor Base", "Responsável Frota"],
                    key="pode_lancar_frota_criar"
                )
                with st.form("criar_usuario"):
                    if nivel == "Responsável Frota":
                        if responsaveis_frota:
                            nome = st.selectbox("Nome", responsaveis_frota)
                        else:
                            nome = ""
                            st.info("Cadastre o responsável no veículo em Frotas antes de criar este usuário.")
                    else:
                        nome = st.text_input("Nome")
                    email_user = st.text_input("Email")
                    veiculos_frota = []
                    if nivel == "Responsável Frota" or pode_lancar_despesa_frota:
                        veiculos_sugeridos = df_frotas_veiculos[
                            df_frotas_veiculos["responsavel"].astype(str).apply(
                                lambda responsavel: str(nome).strip().title() in nomes_responsaveis_frota(responsavel)
                            )
                        ]["placa"].dropna().astype(str).tolist()
                        veiculos_frota = st.multiselect(
                            "Veículos Liberados",
                            veiculos_ativos_usuarios,
                            default=[p for p in veiculos_sugeridos if p in veiculos_ativos_usuarios]
                        )
                    senha = st.text_input("Senha", type="password")
                    if st.form_submit_button("Criar usuário"):
                        nome_existe = any(u.get("nome", "").lower() == nome.lower() for u in usuarios)
                        email_existe = bool(email_user) and any(u.get("email", "").lower() == email_user.lower() for u in usuarios)
                        if not nome or not senha:
                            st.error("Informe nome e senha.")
                        elif nivel == "Responsável Frota" and not responsaveis_frota:
                            st.error("Cadastre o responsável no veículo em Frotas antes de criar este usuário.")
                        elif (nivel == "Responsável Frota" or pode_lancar_despesa_frota) and not veiculos_frota:
                            st.error("Selecione pelo menos um veículo liberado para lançamento de despesas.")
                        elif nome_existe:
                            st.error("Já existe um usuário com esse nome.")
                        elif email_existe:
                            st.error("Já existe um usuário com esse email.")
                        else:
                            usuarios.append({
                                "nome": nome,
                                "email": email_user,
                                "nivel": nivel,
                                "veiculo_frota": veiculos_frota[0] if len(veiculos_frota) == 1 else "",
                                "veiculos_frota": veiculos_frota,
                                "bases_permitidas": BASES_FREQUENCIA if nivel == "Administrador" else bases_permitidas,
                                "pode_lancar_despesa_frota": bool(pode_lancar_despesa_frota),
                                "senha": hash_senha(senha),
                                "criado_em": datetime.now().strftime("%d/%m/%Y %H:%M")
                            })
                            salvar_json(USUARIOS_JSON, usuarios)
                            st.success("Usuário criado.")
                            st.rerun()

            elif acao_usuario == "Editar":
                if usuarios:
                    nomes = [u["nome"] for u in usuarios]
                    selecionado = st.selectbox("Usuário", nomes)
                    idx = nomes.index(selecionado)
                    with st.form("editar_usuario"):
                        nome = st.text_input("Nome", usuarios[idx].get("nome", ""))
                        email_user = st.text_input("Email", usuarios[idx].get("email", ""))
                        nivel_atual = usuarios[idx].get("nivel", "Usuário")
                        if nivel_atual not in niveis_usuario:
                            nivel_atual = "Usuário"
                        nivel = st.selectbox("Nível", niveis_usuario, index=niveis_usuario.index(nivel_atual))
                        bases_atuais = usuarios[idx].get("bases_permitidas", [])
                        if isinstance(bases_atuais, str):
                            bases_atuais = [bases_atuais] if bases_atuais.strip() else []
                        bases_permitidas = st.multiselect(
                            "Bases Permitidas",
                            BASES_FREQUENCIA,
                            default=BASES_FREQUENCIA if nivel == "Administrador" else [b for b in bases_atuais if b in BASES_FREQUENCIA]
                        )
                        pode_lancar_despesa_frota = st.checkbox(
                            "Permitir Lançamento De Despesas De Frota",
                            value=bool(usuarios[idx].get("pode_lancar_despesa_frota", False)) or nivel in ["Administrador", "Supervisor Base", "Responsável Frota"]
                        )
                        veiculos_atuais = usuarios[idx].get("veiculos_frota", [])
                        if isinstance(veiculos_atuais, str):
                            veiculos_atuais = [veiculos_atuais] if veiculos_atuais.strip() else []
                        veiculo_antigo = usuarios[idx].get("veiculo_frota", "")
                        if veiculo_antigo and veiculo_antigo not in veiculos_atuais:
                            veiculos_atuais.append(veiculo_antigo)
                        veiculos_edicao = sorted(set(veiculos_ativos_usuarios + veiculos_atuais))
                        veiculos_frota = []
                        if nivel == "Responsável Frota" or pode_lancar_despesa_frota:
                            veiculos_frota = st.multiselect(
                                "Veículos Liberados",
                                veiculos_edicao,
                                default=[p for p in veiculos_atuais if p in veiculos_edicao]
                            )
                        nova_senha_admin = st.text_input("Nova senha", type="password")
                        if st.form_submit_button("Salvar usuário"):
                            usuarios[idx]["nome"] = nome
                            usuarios[idx]["email"] = email_user
                            usuarios[idx]["nivel"] = nivel
                            usuarios[idx]["veiculo_frota"] = veiculos_frota[0] if len(veiculos_frota) == 1 else ""
                            usuarios[idx]["veiculos_frota"] = veiculos_frota
                            usuarios[idx]["bases_permitidas"] = BASES_FREQUENCIA if nivel == "Administrador" else bases_permitidas
                            usuarios[idx]["pode_lancar_despesa_frota"] = bool(pode_lancar_despesa_frota)
                            if nova_senha_admin:
                                usuarios[idx]["senha"] = hash_senha(nova_senha_admin)
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

    if admin_logado:
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

        with tab_unidades:
            st.dataframe(pd.DataFrame(unidades_config), use_container_width=True)
            acao_unidade = st.radio("Ação de unidade", ["Adicionar", "Editar", "Excluir"], horizontal=True)

            if acao_unidade == "Adicionar":
                nome_unidade = st.text_input("Nome da unidade")
                cor_unidade = st.color_picker("Cor", "#38bdf8", key="cor_unidade_add")
                if st.button("Adicionar unidade"):
                    if nome_unidade:
                        unidades_config.append({"nome": nome_unidade.upper(), "cor": cor_unidade})
                        salvar_json(UNIDADES_JSON, unidades_config)
                        st.success("Unidade adicionada.")
                        st.rerun()

            elif acao_unidade == "Editar" and unidades_config:
                nomes_unidade = [u["nome"] for u in unidades_config]
                selecionada = st.selectbox("Unidade", nomes_unidade, key="editar_unidade")
                idx = nomes_unidade.index(selecionada)
                nome_unidade = st.text_input("Nome", unidades_config[idx]["nome"], key="nome_unidade_edit")
                cor_unidade = st.color_picker("Cor", unidades_config[idx].get("cor", "#38bdf8"), key="cor_unidade_edit")
                if st.button("Salvar unidade"):
                    unidades_config[idx] = {"nome": nome_unidade.upper(), "cor": cor_unidade}
                    salvar_json(UNIDADES_JSON, unidades_config)
                    st.success("Unidade atualizada.")
                    st.rerun()

            elif acao_unidade == "Excluir" and unidades_config:
                nomes_unidade = [u["nome"] for u in unidades_config]
                selecionada = st.selectbox("Unidade", nomes_unidade, key="excluir_unidade")
                if st.button("Excluir unidade"):
                    unidades_config = [u for u in unidades_config if u["nome"] != selecionada]
                    salvar_json(UNIDADES_JSON, unidades_config)
                    st.success("Unidade excluída.")
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
                    if os.path.basename(nome) in [
                        "produtos.xlsx", "movimentacoes.xlsx", "clientes.xlsx", "fornecedores.xlsx",
                        "controle_faltas.xlsx", "frotas_veiculos.xlsx", "frotas_abastecimentos.xlsx",
                        "frotas_manutencoes.xlsx", "frotas_documentos.xlsx", "orcamentos.xlsx",
                        "patrimonio.xlsx", "patrimonio_custos.xlsx", "patrimonio_movimentacoes.xlsx",
                        "patrimonio_insumos_base.xlsx", "bases_movimentacoes.xlsx", "bases_transferencias.xlsx", "usuarios.json",
                        "configuracoes.json", "categorias.json", "unidades.json"
                    ]:
                        zip_ref.extract(nome, BASE_DIR)
                        extraido = os.path.join(BASE_DIR, nome)
                        destino = os.path.join(BASE_DIR, os.path.basename(nome))
                        if extraido != destino:
                            shutil.move(extraido, destino)
            st.success("Backup restaurado.")
            st.rerun()
