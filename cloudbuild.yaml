# mercado-libre

Extractor de inversión en Mercado Ads (Product Ads + Display Ads) hacia GCS,
mismo patrón que `magento/`. Escribe a
`gs://GCS_BUCKET/raw/mercado-libre/inversion/{fecha_ejecucion}.json` (JSON
Lines) vía `escribir_a_gcs()` de `_common/gcs_utils.py`.

A diferencia de Magento (que trae una ventana rodante de órdenes por
`updated_at`), acá la ventana rodante es de **días de gasto** (`scope=daily`
día por día, no un agregado mensual) — ver `MERCADO_LIBRE_SYNC_DAYS_BACK` y
el docstring de `fetch_mercado_libre_metrics_rolling_window` en
`src/extraccion_meli.py`.

## Setup — una sola vez, antes del primer deploy

Esta fuente necesita un paso manual que Magento no necesita: autorizar la
cuenta de Mercado Ads de World Sport (OAuth) para conseguir el primer
`refresh_token`. Se hace UNA vez, desde tu máquina (no desde Cloud Run — el
flujo abre un navegador):

```bash
cd worldsport-extractors        # raíz del repo, no adentro de mercado-libre/
pip install -r mercado-libre/requirements.txt --break-system-packages

cat > .env <<'EOF'
MERCADO_LIBRE_CLIENT_ID=<tu client_id>
MERCADO_LIBRE_CLIENT_SECRET=<tu client_secret>
MERCADO_LIBRE_REDIRECT_URI=<una URL que controles, ej. https://tudominio.com/callback>
EOF

python3 mercado-libre/src/extraccion_meli.py --mode authorize
```

Esto guarda `mercadolibre_oauth_state.json` en la carpeta actual (backend
`file`, el default). **Ese archivo NO se sube a git** (ver `.gitignore` en
la raíz del repo) y tampoco sirve para el Cloud Run Job — hay que subirlo a
Secret Manager una sola vez (ver guía de deploy en el proyecto Claude
"Mauro", `world-sport-meli-extractor-deploy-v0.1.md`):

```bash
gcloud secrets create meli-oauth-state --project=$PROJECT_ID --replication-policy=automatic
gcloud secrets versions add meli-oauth-state --project=$PROJECT_ID --data-file=mercadolibre_oauth_state.json
```

De ahí en más, el Cloud Run Job renueva el `refresh_token` solo (lee/escribe
versiones nuevas del mismo secret) — no hace falta repetir `--mode authorize`
salvo que se revoque el permiso desde Mercado Libre.

## Variables de entorno (Cloud Run Job)

Ver el encabezado de `src/extraccion_meli.py` para la lista completa y las
notas de cada una. Las específicas de esta fuente que no comparte con
`magento/`:

| Variable | Para qué es |
|---|---|
| `MERCADO_LIBRE_CLIENT_ID` / `_CLIENT_SECRET` | App de Mercado Libre (Developers → Mis aplicaciones) |
| `MERCADO_LIBRE_OAUTH_BACKEND=secretmanager` | Obligatorio en el Job — un archivo local no sobrevive entre ejecuciones |
| `MERCADO_LIBRE_OAUTH_SECRET_NAME` | `projects/$PROJECT_ID/secrets/meli-oauth-state` |
| `MERCADO_LIBRE_ADVERTISER_IDS` | Opcional — vacío = se descubren vía API |
| `MERCADO_LIBRE_PRODUCTS` | `PADS` (default) o `PADS,DSP` |
| `MERCADO_LIBRE_SYNC_DAYS_BACK` | Ventana rodante en días (default 5) |
| `GCS_PROJECT` / `GCS_BUCKET` | Mismas que usa `magento/` |

## Probar localmente antes de deployar

```bash
export MERCADO_LIBRE_OAUTH_BACKEND=file   # usa el .json local generado por --mode authorize
export GCS_PROJECT=<project>
export GCS_BUCKET=<bucket>
python3 mercado-libre/src/extraccion_meli.py --mode daily-sync
```
