# testprefectcloud

Sample repository for trying out **Prefect Cloud** with a **Managed** work pool.

Flows live in [`flows/`](flows) and are deployed via [`prefect.yaml`](prefect.yaml).
Every flow run clones this (public) GitHub repo at runtime — **no Docker image is
baked**. The managed worker pulls fresh code on each execution and installs
dependencies from [`requirements.txt`](requirements.txt).

## Repo layout

```
.
├── prefect.yaml            # Deployment definitions (managed pool + git_clone pull)
├── requirements.txt        # Installed on every run by the pull step
├── flows/
│   ├── hello_flow.py       # Trivial smoke-test flow
│   ├── etl_flow.py         # Example extract / transform / load flow
│   └── hs_notes_flows.py   # USITC chapter/section notes (bronze + silver)
└── README.md
```

## Flows

| Deployment       | Entry point                                                                 | Schedule          |
| ---------------- | --------------------------------------------------------------------------- | ----------------- |
| `hello`          | `flows/hello_flow.py:hello_flow`                                            | none (manual)     |
| `etl-daily`      | `flows/etl_flow.py:etl_flow`                                               | `0 7 * * *` UTC   |
| `hs-notes-bronze`| `flows/hs_notes_flows.py:hs_data_chapter_section_notes_2b_from_usitc`        | none (manual)     |
| `hs-notes-silver`| `flows/hs_notes_flows.py:hs_data_chapter_section_notes_b2s_from_html_to_json` | none (manual)  |

## Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run flows directly (no deployment / no Cloud needed):
python -m flows.hello_flow
python -m flows.etl_flow
```

## One-time Prefect Cloud setup

All commands below are run from the repo root on your workstation.

### 1. Log in to Prefect Cloud

```bash
prefect cloud login
```

Select the workspace you want to use. This writes your API key and URL into the
local Prefect profile; the same key is used by `prefect deploy`.

### 2. Create the managed work pool

```bash
prefect work-pool create kyg-managed-pool --type prefect:managed
```

If you want a different name, change it here **and** in `prefect.yaml` (two
places — one per deployment).

### 3. Push this repo to GitHub (public)

Create a new **public** repository on GitHub, then:

```bash
git init
git add .
git commit -m "Initial Prefect Cloud sample"
git branch -M main
git remote add origin https://github.com/<org-or-user>/<repo>.git
git push -u origin main
```

### 4. Point `prefect.yaml` at your GitHub repo

Edit the `repository:` URL in [`prefect.yaml`](prefect.yaml):

```yaml
pull_steps: &pull_steps
  - prefect.deployments.steps.git_clone:
      id: clone-step
      repository: https://github.com/<org-or-user>/<repo>.git
      branch: main
```

Replace `<org-or-user>/<repo>` with your GitHub org/user and repo name, and
adjust `branch:` if you don't use `main`. Because the repo is public, no
credentials block is needed.

### 5. Deploy to Prefect Cloud

From the repo root:

```bash
prefect deploy --all
```

This registers all deployments in `prefect.yaml` (e.g. `hello-flow/hello`,
`etl-flow/etl-daily`, `hs-notes-bronze`, `hs-notes-silver`)
against the `kyg-managed-pool`. `prefect deploy` does **not** upload any code —
the pull steps in `prefect.yaml` tell the managed worker how to fetch it at run
time.

### 6. Run a deployment

```bash
prefect deployment run 'hello-flow/hello'
```

Watch the run in the Cloud UI. In the logs you should see:

1. `git clone` of your GitHub repo
2. `pip install -r requirements.txt`
3. The flow's own log output (`Hello, world!`, etc.)

The ETL deployment (`etl-flow/etl-daily`) will fire automatically at 07:00 UTC
daily; trigger it manually the same way to test.

## Updating flows

Because the managed worker re-clones on every run, you just push changes to
GitHub — the next flow run picks them up. You only need to re-run
`prefect deploy` when you change **deployment-level** things in `prefect.yaml`
(entrypoint, schedule, work pool, parameters, tags, pull steps).

## Switching to a private repo later

If you move to a private GitHub repo, add a `credentials:` line to the
`git_clone` step referencing a `GitHubCredentials` block (from the
`prefect-github` integration) or a `Secret` block holding a PAT:

```yaml
  - prefect.deployments.steps.git_clone:
      repository: https://github.com/<org>/<repo>.git
      branch: main
      access_token: "{{ prefect.blocks.secret.github-pat }}"
```

## Troubleshooting

- **`git_clone` fails**: confirm the repo URL is correct and the repo is
  actually public (browse the `https://github.com/...` URL in an incognito
  window). For private repos, see the section above.
- **Flow imports fail at runtime but work locally**: add the missing package
  to `requirements.txt` and push — the next run will pip-install it.
- **`ModuleNotFoundError` (e.g. `requests`) on one deployment but `etl_flow` works**:
  Managed runs only install `requirements.txt` if the deployment includes the
  **`pip_install_requirements` pull step** (see `pull_steps` in
  [`prefect.yaml`](prefect.yaml)). Deployments created only in the UI often
  clone code but **skip** that step, so third-party imports fail. Use
  `prefect deploy --all` from this repo (or copy the same `pull: *pull_steps`
  into the deployment), then trigger **`testprefectcloud/hs-notes-bronze`** (or
  silver), not an ad-hoc deployment missing pip install.
- **Changing the work pool name**: update `work_pool.name` under **each**
  deployment in `prefect.yaml`.
