# google-ads

Pendiente de construir -- extractor de inversion en Google Ads hacia GCS,
mismo patron que `magento/`:

- `src/extraccion_google_ads.py`
- `Dockerfile` (mismo formato que `magento/Dockerfile`, copiando `_common/`)
- `cloudbuild.yaml` (mismo formato que `magento/cloudbuild.yaml`)
- `requirements.txt`

Al escribir a GCS, usar `escribir_a_gcs(..., fuente="google-ads", entidad="inversion")`
de `_common/gcs_utils.py` -- no reimplementar la subida a GCS aca.
