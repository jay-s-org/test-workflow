# Dynamic Repository Template

This repository is a **dynamic, feature‑toggled** starter template for Python projects. It includes:

* **CI**: linting & testing via Ruff, Pre‑commit, pytest.
* **CD / Release Notes**: automated changelog & GitHub Release descriptions (Changelog‑Weaver).
* **Static Documentation**: MkDocs with auto‑regulation of your Markdown files.
* **AI‑driven PR Agent**: Codium‑based pull‑request reviews with issue linking.
* **Dev Versioning**: automatic `-dev.N` tag bumps on `dev` branch pushes.
* **Initial Template Setup**: project renaming & configuration via `configure_template.yml`.

> All features can be turned on/off per‑repo via simple GitHub Variables.

---

## 1. Quickstart

1. **Create a new repo** from this template (GitHub → Use this template).
2. Go to **Settings > Variables > Repository variables** and set:

   ```yaml
   FEATURE_CI=true
   FEATURE_CD=true
   FEATURE_DOCS=true
   FEATURE_PRAGENT=true
   ```
3. Push your first commit to `main`.  The **configure‑template** workflow will:

   * Rename placeholders to your repo name.
   * Remove template files.
   * Commit back with `✅ Ready to clone and code.`
4. Locally run either:

   * **Unix**: `bash setup.bat`  (alias to the shell setup)
   * **Windows**: `.\setup.bat`

This installs UV CLI, sets up your virtual env, installs dependencies, and hooks pre‑commit.

---

## 2. Feature Toggles

| Variable          | Workflow                                   | Description                          |
| ----------------- | ------------------------------------------ | ------------------------------------ |
| `FEATURE_CI`      | `assets_pr_labeler.yml`,<br>`pr_agent.yml` | Enable CI labeler & PR Agent         |
| `FEATURE_CD`      | `release.yml`                              | Enable auto‑release notes generation |
| `FEATURE_DOCS`    | `docs_regulator.yml`                       | Enable docs auto‑structuring         |
| `FEATURE_PRAGENT` | `pr_agent.yml`                             | Enable AI PR review comments         |

Set any to `false` to skip that piece of automation.

---

## 3. CI & PR Agent

* **Lint & test**: runs on `push`/`pull_request` via `pr_agent.yml` and standard Pre‑commit hooks.
* **PR labeling**: `assets_pr_labeler.yml` uses `.github/labeler.yml` to tag PRs touching `assets/**`.
* **AI PR reviews**: `qodo-ai/pr-agent` auto‑comments on PRs if enabled.

---

## 4. Dev‑Branch Versioning

Workflow `dev_versioning.yml` triggers on pushes to `dev`:

1. Finds your last non‑dev tag (e.g. `v1.2.3`).
2. Bumps or creates `v1.2.3-dev.N`.
3. Pushes that tag back to GitHub.

---

## 5. Release Notes Automation

Workflow `release.yml` triggers on new **GitHub Release**:

1. Full checkout (`fetch-depth: 0`, `fetch-tags: true`).
2. Installs Changelog‑Weaver and deps.
3. Detects `FROM_TAG` (last release) & `TO_TAG` (new release tag).
4. Runs `python -m changelog_weaver` → writes `Releases/*.md`.
5. Patches the Release body with the generated Markdown.

> **First release?** no prior tag → it will compare from the very first commit.

---

## 6. Static Docs Regulation

`docs_regulator.yml` moves any stray `*.md` into `/docs` (preserving structure) on non‑`main`/`dev` pushes.

---

## 7. UV Migration

We swapped **Poetry** for **UV** to simplify lockfile management:

* Local setup via `uv sync` & `uv pip install -e .`
* CI uses `uv sync` instead of `poetry install`.

## 8. Contributing

* Add or remove folder‑based labels in `.github/labeler.yml`.
* Flip features on/off via GitHub > Settings > Variables.
* Extend or disable workflows by editing their `if: vars.…` guards.

---

## License

This template is MIT‑licensed. See [LICENSE](LICENSE) for details.
