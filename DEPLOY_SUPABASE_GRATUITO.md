# Deploy gratuito com Streamlit Cloud + Supabase

Este sistema está preparado para usar o Supabase Free como armazenamento online persistente.

## 1. Criar projeto no Supabase

1. Acesse https://supabase.com.
2. Crie uma conta ou entre com GitHub/Google.
3. Crie um novo projeto.
4. Aguarde o projeto terminar de provisionar.

## 2. Criar bucket de arquivos

1. No Supabase, vá em **Storage**.
2. Clique em **New bucket**.
3. Nome do bucket:

```text
alpes-system
```

4. Pode deixar como bucket privado.
5. Clique em **Create bucket**.

## 3. Pegar as chaves

No Supabase, vá em **Project Settings** > **API**.

Copie:

```text
Project URL
service_role key
```

Use a `service_role key` apenas no Streamlit Secrets. Nao coloque essa chave no GitHub.

## 4. Configurar Streamlit Secrets

No Streamlit Cloud:

1. Abra o app.
2. Clique em **Settings**.
3. Abra **Secrets**.
4. Adicione:

```toml
SUPABASE_URL = "COLE_AQUI_PROJECT_URL"
SUPABASE_SERVICE_ROLE_KEY = "COLE_AQUI_SERVICE_ROLE_KEY"
SUPABASE_BUCKET = "alpes-system"
```

5. Clique em **Save changes**.
6. Clique em **Reboot app**.

## 5. Como o sistema salva os dados

Quando Supabase estiver configurado:

- arquivos `.xlsx` sao enviados automaticamente para o bucket;
- arquivos `.json` sao enviados automaticamente para o bucket;
- imagens de produtos vao para `Imagens Produtos`;
- anexos de frotas vao para `Anexos Frotas`;
- anexos de orcamentos vao para `Anexos Orcamentos`;
- ao iniciar, o sistema baixa os arquivos do bucket para a memoria local do app.

Se Supabase nao estiver configurado, o sistema ainda tenta usar Google Drive, caso esteja configurado.

## 6. Proxima etapa recomendada

Este ajuste resolve armazenamento persistente gratuito.

Para multiplos usuarios com mais seguranca, a evolucao ideal e migrar aos poucos os arquivos `.xlsx` para tabelas PostgreSQL no Supabase:

1. produtos;
2. movimentacoes;
3. usuarios;
4. frotas;
5. frequencia;
6. relatorios.

Essa migracao deve ser feita por partes para nao perder dados ja armazenados.
