# meta-ads

Extractor de inversión en Meta Ads (Facebook/Instagram) hacia GCS, mismo
patrón que `magento/` y `mercado-libre/`. Escribe a
`gs://GCS_BUCKET/raw/meta-ads/inversion/{fecha}.json` (JSON Lines) vía
`escribir_a_gcs()` de `_common/gcs_utils.py`.

Una fila por **campaña y día** (Insights API, `level=campaign`,
`time_increment=1`): spend, impresiones, alcance, clicks, compras y valor de
compras, más los arrays `actions` / `action_values` completos como JSON. Ver
el docstring de `src/extraccion_meta.py` para el detalle.

## Modos

| Modo | Para qué | Escribe |
|---|---|---|
| `--mode list-accounts` | Ver qué cuentas va a consultar | Nada (solo imprime) |
| `--mode extract --since X --until Y` | Probar / revisar a mano | CSV en `output/` |
| `--mode backfill --since 2025-01-01` | Carga histórica, una sola vez | GCS, **un archivo por día de dato** |
| `--mode daily-sync` | Cloud Run Job diario | GCS, un archivo por día de ejecución con la ventana rodante |

## Autenticación

Token de un **system user** del Business Manager (app "Loopi") con
`ads_read`. No vence, así que no hay flujo OAuth como en Mercado Libre.

- Local: `meta-ads/.env` con `META_ACCESS_TOKEN=...` (no va al repo, ver `.gitignore`).
- Cloud Run Job: secret `ACCESS_TOKEN_META` en Secret Manager (mismo estilo
  de nombre que `ACCESS_TOKEN_MAGENTO`), inyectado con `--set-secrets`.

El token también tiene `ads_management`: el script solo lee, pero el secret
es una credencial con permiso de escritura sobre las campañas.

## Variables de entorno

| Variable | Para qué es |
|---|---|
| `META_ACCESS_TOKEN` | Token del system user (secret) |
| `META_AD_ACCOUNT_IDS` | Opcional — `act_1,act_2`. Vacío = cuentas del grupo de activos |
| `META_ASSET_GROUP_ID` | Default `100101348432744` (grupo "Worldsport" del Business Manager) |
| `META_SYNC_DAYS_BACK` | Ventana rodante del daily-sync (default 5, excluye hoy) |
| `META_API_VERSION` | Default `v21.0` |
| `GCS_PROJECT` / `GCS_BUCKET` | Mismas que usan `magento/` y `mercado-libre/` |

## 1. Probar local (parado en la raíz del repo)

```bash
pip install -r meta-ads/requirements.txt
python meta-ads/src/extraccion_meta.py --mode list-accounts
python meta-ads/src/extraccion_meta.py --mode extract --since 2026-10-01 --until 2026-10-29
```

## 2. Carga histórica (una vez)

Necesita credenciales de GCP locales con permiso de escritura en el bucket
(`gcloud auth application-default login`).

```bash
GCS_PROJECT=worldsport-503720 GCS_BUCKET=worldsport-data \
  python meta-ads/src/extraccion_meta.py --mode backfill --since 2025-01-01
```

Trae mes por mes hasta ayer y deja `raw/meta-ads/inversion/2025-01-01.json`,
`2025-01-02.json`, ... igual que el histórico de Magento.

## 3. Deploy del job diario

Todo con la service account `ws-account@worldsport-503720.iam.gserviceaccount.com`,
la misma del job y el scheduler de Magento (ya tiene escritura en
`worldsport-data` e `run.invoker`).

```bash
# Secret (una vez) -- ver output/crear_secret_meta.ps1 para hacerlo sin
# dejar un salto de linea al final del token
gcloud secrets create ACCESS_TOKEN_META --project=worldsport-503720 --replication-policy=automatic
gcloud secrets versions add ACCESS_TOKEN_META --project=worldsport-503720 --data-file=<archivo_con_el_token>
gcloud secrets add-iam-policy-binding ACCESS_TOKEN_META --project=worldsport-503720 \
  --member=serviceAccount:ws-account@worldsport-503720.iam.gserviceaccount.com \
  --role=roles/secretmanager.secretAccessor

# Imagen
gcloud builds submit --config=meta-ads/cloudbuild.yaml .

# Job
gcloud run jobs create meta-ads-extractor-ws \
  --region=southamerica-east1 \
  --image=southamerica-east1-docker.pkg.dev/worldsport-503720/worldsport-jobs/meta-ads-extractor \
  --set-env-vars=GCS_PROJECT=worldsport-503720,GCS_BUCKET=worldsport-data \
  --set-secrets=META_ACCESS_TOKEN=ACCESS_TOKEN_META:latest \
  --service-account=ws-account@worldsport-503720.iam.gserviceaccount.com \
  --max-retries=1 --task-timeout=10m --memory=512Mi

# Disparo diario (11:00 hora Argentina)
gcloud scheduler jobs create http meta-ads-extractor-ws-diario \
  --location=southamerica-east1 \
  --schedule="0 11 * * *" --time-zone="America/Argentina/Buenos_Aires" \
  --uri="https://southamerica-east1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/worldsport-503720/jobs/meta-ads-extractor-ws:run" \
  --http-method=POST \
  --oauth-service-account-email=ws-account@worldsport-503720.iam.gserviceaccount.com
```
