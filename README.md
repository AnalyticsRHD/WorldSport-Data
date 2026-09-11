# worldsport-extractors

Repositorio único con los extractores de datos de World Sport hacia GCS
(capa raw). Un job de Cloud Run por fuente, todos compartiendo el mismo
proyecto de GCP, el mismo bucket, y las funciones comunes en `_common/`.

## Estructura

```
worldsport-extractors/
├── magento/
│   ├── src/extraccion_magento.py   -- ordenes a nivel linea de item
│   ├── Dockerfile
│   ├── cloudbuild.yaml
│   └── requirements.txt
├── meta-ads/                       -- pendiente de construir
├── google-ads/                     -- pendiente de construir
├── _common/
│   └── gcs_utils.py                -- escribir_a_gcs(), reutilizada por todas las fuentes
├── .gitignore
└── .gcloudignore
```

Cada subcarpeta de fuente (`magento/`, `meta-ads/`, `google-ads/`) es su
propio Cloud Run Job independiente: imagen propia, deploy propio, trigger
de Cloud Build propio. Lo único que comparten es este repo como lugar de
almacenamiento del codigo, y el modulo `_common/` para no duplicar logica
(hoy solo `escribir_a_gcs`, pero ahi va cualquier otra funcion que se repita
entre fuentes -- ej. reintentos ante rate limits).

## Por que el build usa `cloudbuild.yaml` en vez de solo `--tag`

`gcloud builds submit --tag ...` por defecto espera un `Dockerfile` en la
raiz de lo que se sube. Como cada fuente tiene su propio `Dockerfile` en su
propia subcarpeta, pero necesita poder copiar `_common/` (que esta un nivel
arriba, fuera de esa subcarpeta), el build tiene que correr con el
**contexto en la raiz del repo** y decirle explicitamente donde esta el
Dockerfile de esa fuente. Por eso cada subcarpeta trae su propio
`cloudbuild.yaml`: es la forma de decirle a Cloud Build "usa este
Dockerfile puntual, pero con toda la raiz del repo como contexto".

## Deploy de un extractor (ejemplo: Magento)

Parado en la RAIZ del repo (no dentro de `magento/`):

```powershell
gcloud builds submit --config=magento/cloudbuild.yaml .
```

Esto arma y sube la imagen a Artifact Registry (el nombre de la imagen esta
definido dentro de `magento/cloudbuild.yaml`). Despues, crear/actualizar el
Cloud Run Job apuntando a esa imagen como ya veniamos haciendo.

## Variables de entorno / secretos

No van en el repo. Se configuran en cada Cloud Run Job:
- `GCS_PROJECT`, `GCS_BUCKET`, y las que sean especificas de cada fuente
  (ej. `MAGENTO_BASE_URL`) como variables de entorno normales.
- Credenciales (Magento, y en el futuro Meta/Google Ads) como referencias a
  Secret Manager, nunca como variable de entorno en texto plano ni
  hardcodeadas en el codigo.

## `.env` local

Para correr cualquier extractor en tu maquina, cada subcarpeta de fuente
espera su propio `.env` (mismo nivel que su `Dockerfile`) con las
credenciales de esa fuente. Ninguno de esos `.env` va al repo -- ver
`.gitignore`.
