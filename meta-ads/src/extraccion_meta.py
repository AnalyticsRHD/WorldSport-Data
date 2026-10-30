"""
extraccion_meta.py
==================
Extractor de inversion en Meta Ads (Facebook/Instagram) de World Sport hacia
GCS, capa raw -- mismo patron que mercado-libre/ y magento/ en este repo.

Que trae
--------
Una fila por CAMPANA y por DIA (Insights API, `level=campaign`,
`time_increment=1`) de cada cuenta publicitaria de World Sport: spend,
impresiones, alcance, clicks y las conversiones de compra (cantidad y valor)
que reporta Meta. Mismo nivel de detalle que se eligio para Mercado Ads en la
revision 1/10/2026 de mercado-libre/src/extraccion_meli.py ("toda la
informacion a nivel campana").

Los arrays `actions` / `action_values` de la API se guardan ademas completos
como JSON (`actions_json` / `action_values_json`) para no perder ningun tipo
de conversion que hoy no se este mapeando a columna propia.

Autenticacion
-------------
Token de un SYSTEM USER del Business Manager de Red Hook (app "Loopi"), con
`ads_read`. Ese tipo de token NO vence, asi que -- a diferencia de Mercado
Libre -- no hace falta flujo OAuth ni renovar nada: en el Cloud Run Job se
inyecta como variable de entorno desde Secret Manager (`--set-secrets`), y en
local se lee de `meta-ads/.env`.

OJO: el mismo token tiene tambien `ads_management` y `business_management`
(puede modificar campanas). Este script solo hace GETs, pero el secret hay que
tratarlo como credencial de escritura.

Que cuentas consulta
--------------------
- `META_AD_ACCOUNT_IDS` (lista separada por coma, con o sin prefijo `act_`)
  si esta seteada.
- Si no, las cuentas del grupo de activos de World Sport en el Business
  Manager (`META_ASSET_GROUP_ID`, default 100101348432744 = "Worldsport"),
  via `/{asset_group_id}/contained_adaccounts`. [A CONFIRMAR] el nombre del
  edge contra la primera corrida real -- si da error, setear
  META_AD_ACCOUNT_IDS a mano (ver `--mode list-accounts`).

Ventana rodante (--mode daily-sync)
-----------------------------------
Ultimos `META_SYNC_DAYS_BACK` dias (default 5) EXCLUYENDO hoy, con "hoy" en
hora de Buenos Aires -- mismo criterio que Mercado Libre / Magento. Meta sigue
ajustando conversiones atribuidas varios dias despues (ventana de atribucion
de 7 dias por click), por eso se re-trae una ventana y no solo "ayer"; la
deduplicacion se resuelve rio abajo en BigQuery con `fecha_carga` (ver
docstring de `escribir_a_gcs`).

-----------------------------------------------------------------------------
CONFIG (variables de entorno / meta-ads/.env)
-----------------------------------------------------------------------------
META_ACCESS_TOKEN          (obligatoria -- token del system user)
META_AD_ACCOUNT_IDS        (opcional -- ej "act_123,456"; vacio = se descubren via META_ASSET_GROUP_ID)
META_ASSET_GROUP_ID        (default: 100101348432744 -- grupo de activos "Worldsport")
META_API_VERSION           (default: v21.0)
META_SYNC_DAYS_BACK        (default: 5 -- ventana rodante de --mode daily-sync)
META_TIMEOUT_SECONDS       (default: 60)
GCS_PROJECT / GCS_BUCKET   (obligatorias para --mode daily-sync -- mismas que usan magento/ y mercado-libre/)

-----------------------------------------------------------------------------
USO (parado en la RAIZ del repo)
-----------------------------------------------------------------------------
python meta-ads/src/extraccion_meta.py --mode list-accounts
python meta-ads/src/extraccion_meta.py --mode extract --since 2026-10-01 --until 2026-10-29   # CSV local
python meta-ads/src/extraccion_meta.py --mode backfill --since 2025-01-01                     # historico a GCS, un archivo por dia
python meta-ads/src/extraccion_meta.py --mode daily-sync                                      # Cloud Run Job
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

try:
    from dotenv import load_dotenv
    # Primero el .env de esta fuente (meta-ads/.env, convencion del README de
    # la raiz), despues uno en el directorio actual si existiera.
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    load_dotenv()
except ImportError:
    pass

# Raiz del repo = dos niveles arriba de este archivo (meta-ads/src/... ->
# repo root), para poder hacer `from _common.gcs_utils import escribir_a_gcs`
# -- mismo mecanismo que magento/ y mercado-libre/.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

logger = logging.getLogger("meta_ads")
logging.basicConfig(level=logging.INFO)
# httpx loguea cada request en INFO con la URL completa, y la Graph API lleva
# el access_token como query param -- sin esto el token terminaria en Cloud
# Logging en cada corrida.
logging.getLogger("httpx").setLevel(logging.WARNING)

BSAS_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

INSIGHTS_FIELDS = (
    "account_id,account_name,account_currency,campaign_id,campaign_name,objective,"
    "spend,impressions,reach,frequency,clicks,inline_link_clicks,cpm,cpc,ctr,"
    "actions,action_values"
)

# action_type que Meta usa para "compra" segun el origen (pixel web, app,
# onsite). Se toma el primero que aparezca en este orden -- NO se suman entre
# si porque `purchase` ya es el agregado omnicanal de los otros.
PURCHASE_ACTION_TYPES = (
    "purchase",
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
)

# Codigos de error de la Graph API que indican rate limit / throttling
# (https://developers.facebook.com/docs/graph-api/overview/rate-limiting):
# se reintentan con backoff en vez de fallar la cuenta entera.
RATE_LIMIT_CODES = {4, 17, 32, 613, 80000, 80004}

# Destino en GCS: raw/inversiones/meta/{fecha}.json, al lado de la inversion
# de Mercado Libre (raw/inversiones/meli/). escribir_a_gcs arma la ruta como
# raw/{fuente}/{entidad}/, por eso "inversiones" va como fuente y "meta" como
# entidad.
GCS_FUENTE = "inversiones"
GCS_ENTIDAD = "meta"


class Config:
    def __init__(self):
        # .strip(): un secret cargado con `--data-file` desde PowerShell/echo
        # suele traer un salto de linea al final, y la Graph API lo rechaza.
        self.access_token = os.environ.get("META_ACCESS_TOKEN", "").strip()
        self.ad_account_ids_raw = os.environ.get("META_AD_ACCOUNT_IDS", "")
        self.asset_group_id = os.environ.get("META_ASSET_GROUP_ID", "100101348432744")
        self.api_version = os.environ.get("META_API_VERSION", "v21.0")
        self.sync_days_back = int(os.environ.get("META_SYNC_DAYS_BACK", "5"))
        self.timeout_seconds = float(os.environ.get("META_TIMEOUT_SECONDS", "60"))
        self.gcs_project = os.environ.get("GCS_PROJECT", "")
        self.gcs_bucket = os.environ.get("GCS_BUCKET", "")

    @property
    def base_url(self) -> str:
        return f"https://graph.facebook.com/{self.api_version}"


config = Config()


class MetaApiError(RuntimeError):
    pass


# =============================================================================
# Helpers
# =============================================================================

def today() -> str:
    return datetime.now(BSAS_TZ).date().isoformat()


def rolling_window(days_back: int) -> tuple[str, str]:
    """(desde, hasta) de los ultimos `days_back` dias, excluyendo hoy."""
    hoy = datetime.now(BSAS_TZ).date()
    return (hoy - timedelta(days=days_back)).isoformat(), (hoy - timedelta(days=1)).isoformat()


def month_chunks(since: str, until: str) -> list[tuple[str, str]]:
    """
    Parte un rango en tramos de a lo sumo un mes calendario. Insights con
    time_increment=1 sobre rangos largos (backfill) puede dar timeout del
    lado de Meta ("Please reduce the amount of data"); por mes anda bien.
    """
    start, end = date.fromisoformat(since), date.fromisoformat(until)
    chunks = []
    while start <= end:
        next_month = (start.replace(day=1) + timedelta(days=32)).replace(day=1)
        chunk_end = min(end, next_month - timedelta(days=1))
        chunks.append((start.isoformat(), chunk_end.isoformat()))
        start = chunk_end + timedelta(days=1)
    return chunks


def normalize_account_id(account_id: str) -> str:
    account_id = account_id.strip()
    return account_id if account_id.startswith("act_") else f"act_{account_id}"


def number_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def first_action_value(actions: list[dict] | None, action_types: tuple[str, ...]) -> float:
    by_type = {a.get("action_type"): a.get("value") for a in actions or []}
    for action_type in action_types:
        if action_type in by_type:
            return number_value(by_type[action_type])
    return 0.0


# =============================================================================
# Cliente Graph API
# =============================================================================

def graph_get(client: httpx.Client, url: str, params: dict | None = None, max_retries: int = 5) -> dict:
    """
    GET a la Graph API con reintentos ante rate limit (codigos de
    RATE_LIMIT_CODES) y errores 5xx. `url` puede ser un path relativo
    ("act_123/insights") o la URL absoluta de `paging.next` (que ya trae
    todos los parametros, incluido el token).
    """
    if not config.access_token:
        raise RuntimeError("Falta META_ACCESS_TOKEN (variable de entorno o meta-ads/.env).")

    if not url.startswith("http"):
        url = f"{config.base_url}/{url}"
        params = {**(params or {}), "access_token": config.access_token}

    for attempt in range(1, max_retries + 1):
        response = client.get(url, params=params)
        try:
            body = response.json()
        except ValueError:
            body = {}

        if response.status_code == 200 and "error" not in body:
            return body

        error = body.get("error", {})
        retriable = error.get("code") in RATE_LIMIT_CODES or response.status_code >= 500
        if retriable and attempt < max_retries:
            wait = 30 * attempt
            logger.warning("Rate limit / error transitorio de Meta (%s), reintento %d en %ds",
                           error.get("message") or response.status_code, attempt, wait)
            time.sleep(wait)
            continue

        raise MetaApiError(
            f"HTTP {response.status_code} -- {error.get('message', response.text[:300])} "
            f"(code={error.get('code')}, subcode={error.get('error_subcode')}, "
            f"fbtrace_id={error.get('fbtrace_id')})"
        )

    raise MetaApiError("Se agotaron los reintentos")


def graph_get_all(client: httpx.Client, path: str, params: dict) -> list[dict]:
    """Recorre la paginacion por cursor (`paging.next`) y junta todo `data`."""
    rows: list[dict] = []
    body = graph_get(client, path, params)
    while True:
        rows.extend(body.get("data", []))
        next_url = body.get("paging", {}).get("next")
        if not next_url:
            return rows
        body = graph_get(client, next_url)


# =============================================================================
# Cuentas
# =============================================================================

def resolve_ad_accounts(client: httpx.Client) -> list[dict]:
    """
    Devuelve [{id, name, currency, account_status}] de las cuentas a
    consultar: las de META_AD_ACCOUNT_IDS si esta seteada, si no las del
    grupo de activos META_ASSET_GROUP_ID.
    """
    fields = "id,name,currency,account_status"

    if config.ad_account_ids_raw.strip():
        accounts = []
        for raw_id in config.ad_account_ids_raw.split(","):
            if raw_id.strip():
                accounts.append(graph_get(client, normalize_account_id(raw_id), {"fields": fields}))
        return accounts

    return graph_get_all(
        client, f"{config.asset_group_id}/contained_adaccounts", {"fields": fields, "limit": 100}
    )


# =============================================================================
# Insights
# =============================================================================

def fetch_account_insights(client: httpx.Client, account_id: str, since: str, until: str) -> list[dict]:
    """Una fila por campana y dia de una cuenta, para el rango [since, until]."""
    rows: list[dict] = []
    for chunk_since, chunk_until in month_chunks(since, until):
        rows.extend(graph_get_all(client, f"{account_id}/insights", {
            "level": "campaign",
            "time_increment": 1,
            "time_range": json.dumps({"since": chunk_since, "until": chunk_until}),
            "fields": INSIGHTS_FIELDS,
            "limit": 500,
        }))
    return rows


def build_row(raw: dict, fecha_carga: str) -> dict:
    """Aplana una fila de Insights al formato que se sube a GCS."""
    return {
        "date": raw.get("date_start"),
        "platform": "META",
        "account_id": f"act_{raw.get('account_id')}",
        "account_name": raw.get("account_name"),
        "currency": raw.get("account_currency"),
        "campaign_id": raw.get("campaign_id"),
        "campaign_name": raw.get("campaign_name"),
        "objective": raw.get("objective"),
        "spend": number_value(raw.get("spend")),
        "impressions": int(number_value(raw.get("impressions"))),
        "reach": int(number_value(raw.get("reach"))),
        "frequency": number_value(raw.get("frequency")),
        "clicks": int(number_value(raw.get("clicks"))),
        "inline_link_clicks": int(number_value(raw.get("inline_link_clicks"))),
        "cpm": number_value(raw.get("cpm")),
        "cpc": number_value(raw.get("cpc")),
        "ctr": number_value(raw.get("ctr")),
        "purchases": first_action_value(raw.get("actions"), PURCHASE_ACTION_TYPES),
        "purchase_value": first_action_value(raw.get("action_values"), PURCHASE_ACTION_TYPES),
        "actions_json": json.dumps(raw.get("actions") or [], ensure_ascii=False),
        "action_values_json": json.dumps(raw.get("action_values") or [], ensure_ascii=False),
        "fecha_carga": fecha_carga,
    }


def fetch_meta_insights(since: str, until: str) -> tuple[list[dict], list[str]]:
    """
    Trae insights de todas las cuentas resueltas. Si falla UNA cuenta se
    sigue con las demas y se devuelve la falla aparte (mismo criterio que el
    fix #1 de la revision 30/9/2026 de Mercado Libre: no tirar los datos que
    si se pudieron traer).
    """
    fecha_carga = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    failures: list[str] = []

    with httpx.Client(timeout=config.timeout_seconds) as client:
        accounts = resolve_ad_accounts(client)
        logger.info("%d cuenta(s) a consultar, rango %s -> %s", len(accounts), since, until)

        for account in accounts:
            try:
                raw_rows = fetch_account_insights(client, account["id"], since, until)
            except MetaApiError as e:
                failures.append(f"{account['id']} ({account.get('name')}): {e}")
                logger.error("Fallo %s: %s", account["id"], e)
                continue
            rows.extend(build_row(r, fecha_carga) for r in raw_rows)
            logger.info("  %s (%s): %d fila(s)", account["id"], account.get("name"), len(raw_rows))

    return rows, failures


# =============================================================================
# Entry point / CLI
# =============================================================================

def _run_list_accounts() -> int:
    with httpx.Client(timeout=config.timeout_seconds) as client:
        accounts = resolve_ad_accounts(client)
    print(f"\n{len(accounts)} cuenta(s):")
    for a in accounts:
        print(f"  {a['id']}  {a.get('name')}  ({a.get('currency')}, status={a.get('account_status')})")
    return 0


def _run_extract(since: str, until: str) -> int:
    rows, failures = fetch_meta_insights(since, until)
    print(f"\n[OK] {len(rows)} filas obtenidas ({since} -> {until}).")

    if rows:
        df = pd.DataFrame(rows)
        os.makedirs("output", exist_ok=True)
        csv_path = os.path.join("output", f"meta_ads_{since}_{until}.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[OK] Exportado a: {csv_path}")
        resumen = df.groupby(["account_name", "currency"], as_index=False)["spend"].sum()
        print(resumen.to_string(index=False))

    for f in failures:
        print(f"[ADVERTENCIA] {f}")
    return 1 if failures else 0


def _run_daily_sync() -> int:
    """
    Modo del Cloud Run Job (Cloud Scheduler, una vez por dia). Igual que en
    mercado-libre/: sube lo que se pudo traer y devuelve exit code 1 si hubo
    fallos parciales, para que la ejecucion quede marcada como fallida.
    """
    from _common.gcs_utils import escribir_a_gcs

    since, until = rolling_window(config.sync_days_back)
    rows, failures = fetch_meta_insights(since, until)
    print(f"\n[OK] {len(rows)} filas obtenidas (ventana {since} -> {until}).")

    if rows:
        path = escribir_a_gcs(
            pd.DataFrame(rows), fecha_ejecucion=today(), fuente=GCS_FUENTE, entidad=GCS_ENTIDAD,
            gcs_project=config.gcs_project or None, gcs_bucket=config.gcs_bucket or None,
        )
        print(f"[OK] Subido a gs://{config.gcs_bucket}/{path}")
    else:
        print("[AVISO] 0 filas -- no se subio nada a GCS esta corrida.")

    if failures:
        print(f"\n[ADVERTENCIA] {len(failures)} fallo(s) parcial(es) durante la corrida:")
        for f in failures:
            print(f"  - {f}")
        return 1
    return 0


def _run_backfill(since: str, until: str) -> int:
    """
    Carga historica a GCS: trae [since, until] (por mes, ver month_chunks) y
    escribe UN archivo por dia de dato -- raw/inversiones/meta/{dia}.json
    -- igual que quedo el historico de Magento (raw/magento/ordenes/2025-01-01.json, ...).

    Se corre una vez a mano antes de prender el daily-sync. Si se re-corre
    sobre dias que el daily-sync ya escribio, pisa esos archivos con datos
    mas nuevos -- no se pierde nada, cada fila lleva su fecha_carga.
    """
    from _common.gcs_utils import escribir_a_gcs

    rows, failures = fetch_meta_insights(since, until)
    print(f"\n[OK] {len(rows)} filas obtenidas ({since} -> {until}).")

    if rows:
        df = pd.DataFrame(rows)
        for dia, df_dia in df.groupby("date"):
            path = escribir_a_gcs(
                df_dia, fecha_ejecucion=dia, fuente=GCS_FUENTE, entidad=GCS_ENTIDAD,
                gcs_project=config.gcs_project or None, gcs_bucket=config.gcs_bucket or None,
            )
            logger.info("  %d fila(s) -> gs://%s/%s", len(df_dia), config.gcs_bucket, path)
        print(f"[OK] {df['date'].nunique()} archivo(s) diario(s) subidos a gs://{config.gcs_bucket}/raw/{GCS_FUENTE}/{GCS_ENTIDAD}/")

    for f in failures:
        print(f"[ADVERTENCIA] {f}")
    return 1 if failures else 0


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["list-accounts", "extract", "backfill", "daily-sync"], default="extract")
    parser.add_argument("--since", help="Fecha desde (YYYY-MM-DD) para --mode extract/backfill. Default: ventana rodante.")
    parser.add_argument("--until", help="Fecha hasta (YYYY-MM-DD) para --mode extract/backfill. Default: ayer.")
    args = parser.parse_args()

    if args.mode == "list-accounts":
        sys.exit(_run_list_accounts())

    if args.mode == "daily-sync":
        sys.exit(_run_daily_sync())

    if args.mode == "backfill":
        if not args.since:
            parser.error("--mode backfill necesita --since (ej. --since 2025-01-01)")
        sys.exit(_run_backfill(args.since, args.until or rolling_window(1)[1]))

    default_since, default_until = rolling_window(config.sync_days_back)
    sys.exit(_run_extract(args.since or default_since, args.until or default_until))


if __name__ == "__main__":
    main()
