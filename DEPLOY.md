# Deploy Do Sistema Alpes

Este projeto esta preparado para rodar em um servidor Python com Streamlit.

## Arquivos principais

- `app.py`: sistema principal.
- `requirements.txt`: dependencias Python.
- `.streamlit/config.toml`: configuracao de servidor e tema.
- `run_producao.bat`: execucao local/Windows.
- `Dockerfile` e `docker-compose.yml`: execucao em VPS com Docker.
- `ALPES_DATA_DIR`: variavel opcional para definir onde ficam planilhas, JSON, imagens e anexos.

## Rodar em servidor Windows

1. Instalar Python 3.12.
2. Abrir o PowerShell na pasta do sistema.
3. Executar:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install -r requirements.txt
streamlit run app.py --server.address 0.0.0.0 --server.port 8502 --server.fileWatcherType poll
```

Depois acessar:

```text
http://IP_DO_SERVIDOR:8502
```

## Rodar em VPS Linux com Docker

1. Instalar Docker e Docker Compose.
2. Enviar a pasta do sistema para o servidor.
3. Dentro da pasta, executar:

```bash
docker compose up -d --build
```

Depois acessar:

```text
http://IP_DO_SERVIDOR:8502
```

No Docker, os dados ficam na pasta `dados` do projeto. Isso permite atualizar o codigo sem apagar planilhas, usuarios, imagens, anexos e configuracoes.

## Publicacao com dominio

Para usar dominio com HTTPS, coloque um proxy reverso na frente do Streamlit, como Nginx, Apache ou o painel do provedor. O proxy deve apontar para:

```text
http://127.0.0.1:8502
```

## Observacao importante sobre acesso simultaneo

Hoje os dados ainda ficam em arquivos `.xlsx` e `.json`. Isso funciona para uso simples, mas nao e o ideal para muitos usuarios ao mesmo tempo.

Para producao com varios usuarios, o proximo passo recomendado e migrar os dados para PostgreSQL. Assim o sistema ganha mais seguranca, historico consistente, backups melhores e menor risco de conflito entre usuarios.

## Backup

O sistema ja possui rotina de backup na aba `CONFIGURACOES`. Antes de publicar, gere um backup completo.
