# meta-ads

Pendiente de construir -- extractor de inversion en Meta (Facebook/Instagram
Ads) hacia GCS, mismo patron que `magento/`:

- `src/extraccion_meta.py`
- `Dockerfile` (mismo formato que `magento/Dockerfile`, copiando `_common/`)
- `cloudbuild.yaml` (mismo formato que `magento/cloudbuild.yaml`)
- `requirements.txt`

Al escribir a GCS, usar `escribir_a_gcs(..., fuente="meta-ads", entidad="inversion")`
de `_common/gcs_utils.py` -- no reimplementar la subida a GCS aca.
