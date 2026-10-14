---
description: Gera ou atualiza o site de documentação (GitHub Pages) a partir do repositório
agent: build
---

Mantenha o site de documentação deste projeto em sincronia com o repositório.

Contexto do site:

- Gerador: MkDocs Material + plugin `mkdocs-static-i18n` (estrutura `suffix`).
- Config: `mkdocs.yml`. Conteúdo: `docs/*.md`. Dependências: `requirements-docs.txt`.
- Idioma padrão **pt-BR**, servido na raiz. Outros idiomas entram como
  `docs/<pagina>.<locale>.md` mais um bloco em `plugins.i18n.languages`.
- Deploy: `.github/workflows/docs.yml` publica no GitHub Pages a cada push em `main`.

Tarefa: $ARGUMENTS

Regras:

1. Escreva em pt-BR, no tom e no estilo já usados em `docs/`. Não traduza
   identificadores de código, nomes de campos JSON, comandos ou saída de
   ferramentas.
2. Fonte da verdade do conteúdo: `README.md`, `src/DESIGN.md`, `DESIGN-0.3.md`,
   `docker/README.md`, `ci/README.md`, `monitoring/README.md` e `CHANGELOG.md`.
   Ao sincronizar, preserve os comandos, valores padrão, tabelas e limites exatos —
   não invente números.
3. Não edite este site para descrever comportamento que ainda não existe no
   código.
4. Mantenha links entre páginas como caminhos relativos sem a extensão de idioma
   (por exemplo `nats.md`, nunca `nats.pt-BR.md`).
5. Se renomear algo estrutural (repositório, título, seções de navegação),
   atualize também `site_url`, `repo_url`, `repo_name` e `edit_uri` em `mkdocs.yml`.
6. Valide com `pip install -r requirements-docs.txt` e `mkdocs build --strict`;
   corrija qualquer aviso antes de concluir.
7. Ao terminar, relate em uma linha o que mudou e onde.
