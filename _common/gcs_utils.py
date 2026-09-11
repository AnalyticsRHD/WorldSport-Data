"""
Funciones compartidas para escribir datos crudos a GCS.

Usado por todos los extractores del repo (magento/, meta-ads/, google-ads/,
los que se agreguen despues). La idea es escribir esta logica una sola vez
y que cada extractor solo importe y llame, en vez de reimplementarla.
"""
import json
import os

import pandas as pd
from google.cloud import storage


def escribir_a_gcs(
    df: pd.DataFrame,
    fecha_ejecucion: str,
    fuente: str,
    entidad: str,
    gcs_project: str | None = None,
    gcs_bucket: str | None = None,
) -> str:
    """
    Sube un DataFrame a GCS como JSON Lines (una linea por registro) en:
        raw/{fuente}/{entidad}/{fecha_ejecucion}.json

    JSON Lines a proposito: es el formato que espera directo
    `bq load --source_format=NEWLINE_DELIMITED_JSON`, sin transformar nada
    despues.

    Parametros
    ----------
    df : DataFrame ya armado, listo para subir (una fila = un registro).
    fecha_ejecucion : dia de la corrida (ej. "2026-09-10"), NO el rango
        desde/hasta de una eventual ventana rodante -- asi una re-ejecucion
        el mismo dia pisa el archivo de ese dia en vez de acumular uno nuevo.
        La deduplicacion de registros que aparecen en varias corridas (si el
        extractor usa ventana rodante) se resuelve rio abajo en BigQuery via
        alguna columna tipo Fecha_Carga, no en el nombre del archivo.
    fuente : origen de datos -- "magento", "meta-ads", "google-ads", etc.
        Separa el primer nivel de carpetas dentro de raw/.
    entidad : tipo de dato dentro de esa fuente -- "ordenes", "inversion",
        "campanas", etc. Separa el segundo nivel.
    gcs_project / gcs_bucket : si no se pasan, se leen de las variables de
        entorno GCS_PROJECT / GCS_BUCKET.

    Devuelve el path relativo (sin el nombre del bucket) donde quedo escrito.
    """
    gcs_project = gcs_project or os.getenv("GCS_PROJECT")
    gcs_bucket = gcs_bucket or os.getenv("GCS_BUCKET")

    if not gcs_project or not gcs_bucket:
        raise RuntimeError(
            "Faltan GCS_PROJECT / GCS_BUCKET (variables de entorno o "
            "argumentos). Completar antes de correr esto en serio."
        )

    client = storage.Client(project=gcs_project)
    bucket = client.bucket(gcs_bucket)
    path = f"raw/{fuente}/{entidad}/{fecha_ejecucion}.json"
    blob = bucket.blob(path)

    # NaN/NaT -> None antes de serializar, para que salga `null` en el JSON
    # (json.dumps con un NaN de pandas/numpy escribe el literal `NaN`, que
    # no es JSON valido y puede romper el `bq load` de despues).
    registros = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
    contenido = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in registros)
    blob.upload_from_string(contenido, content_type="application/json")

    return path
