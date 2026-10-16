"""
mercado_libre_metrics.py
=========================
Transcripcion a Python (async, con httpx) del servicio de sync de metricas
de Mercado Ads (Product Ads + Display) que compartiste en TypeScript/NestJS.

TRANSPARENCIA IMPORTANTE -- lee esto antes de usarlo:
Segui fielmente la logica y los endpoints de tu codigo TS. Pero hay piezas
que estaban REFERENCIADAS pero no vinieron en lo que pegaste, asi que las
tuve que inferir. Cada una esta marcada con "[INFERIDO]" en su docstring.
Confirmalas contra tu implementacion real antes de usar esto en produccion:

1. fetch_mercado_libre_campaign_metrics() -- el metodo que trae las metricas
   DIARIAS por campana de Product Ads (fetchMercadoLibreCampaignMetrics en tu
   TS). Se llama desde fetchMercadoLibreProductAdsDailyMetrics pero su cuerpo
   no vino en el pegado. Ojo: en tu codigo esa funcion recibe solo campaignId
   (no advertiser_id) -- yo arme el endpoint simetrico al de Display Ads (que
   SI lleva advertiser_id en el path) porque es el patron mas consistente con
   el resto de tu API, pero si tu endpoint real no lleva advertiser_id, hay
   que sacarlo del path.
2. add_mercado_libre_spend() -- como se acumula el spend en el diccionario
   (adaptacion del Map de tu TS). Arme una version que agrupa por fecha +
   advertiser; si tu logica real agrupa distinto (ej. tambien por campaign_id)
   avisame.
3. extract_mercado_libre_list() -- asumo el patron mas comun: buscar la
   primera clave del JSON de respuesta que sea una lista (results/campaigns/
   data/items/advertisers).
4. is_mercado_libre_token_expiring() -- asumo un margen de 5 minutos antes
   del expires_at guardado.
5. resolve_brand_mapping() -- tu brandMappingService.resolve(referencia) es
   un servicio interno tuyo que no vino en el codigo compartido. Dejo un stub
   que devuelve la referencia tal cual como marca -- reemplazalo por tu
   logica real (diccionario, consulta a DB, lo que uses).

Todo lo demas -- los endpoints, el header "api-version: 2", "cost" como
campo de costo en Product Ads, "consumed_budget" en Display, el flujo de
retry-on-401, la paginacion offset/limit con tope en 10000 -- esta tomado
tal cual de tu codigo TS.

-----------------------------------------------------------------------------
REVISION 30/9/2026 -- 2 fixes + deploy a Cloud Run Job (mismo patron que
magento/ en el repo worldsport-extractors)
-----------------------------------------------------------------------------
Fixes de codigo (ver detalle en world-sport-meli-extractor-review-v0.1.md,
proyecto Claude "Mauro"):
1. Antes, si fallaba UN advertiser, se descartaban TODOS los datos del
   periodo (incluidos los advertisers que si funcionaron). Ahora devuelve
   los datos parciales y loguea advertencia de cuales fallaron.
2. Antes, PADS + DSP juntos ("MERCADO_LIBRE_PRODUCTS=PADS,DSP") mezclaban el
   spend en un solo total sin poder separarlo despues. Ahora cada fila queda
   etiquetada por plataforma real (MELI-PADS / MELI-DSP).

Agregados para correr como Cloud Run Job diario (igual que las ordenes de
Magento y el stock de SAP -> GCS, capa raw):
3. `escribir_a_gcs()` de `_common/gcs_utils.py` (mismo modulo que ya usa
   magento/) -- sube el resultado a
   gs://GCS_BUCKET/raw/mercado-libre/inversion/{fecha_ejecucion}.json como
   JSON Lines, en vez de (o ademas de) el CSV local.
4. Nuevo modo de CLI `--mode daily-sync`: pensado para correr sin
   intervencion humana desde Cloud Scheduler. Trae una VENTANA RODANTE de
   los ultimos `MERCADO_LIBRE_SYNC_DAYS_BACK` dias (default 5, mismo
   criterio que `VENTANA_DIAS=5` en el extractor de Magento) en `scope=daily`
   dia por dia (no "monthly" agregado), EXCLUYENDO el dia de hoy porque el
   gasto de un dia en curso todavia no esta asentado -- supuesto declarado,
   igual que se documento la ventana rodante de Magento; ajustar si Mercado
   Ads confirma que el dato del dia corriente ya sale cerrado.
5. `HOY` ahora sale de America/Argentina/Buenos_Aires (zoneinfo), no de
   `date.today()` a secas -- mismo motivo que el fix #4 de Magento: un
   container de Cloud Run corre en UTC, y sin esto la ventana se desalinea
   del dia de negocio real.
6. Estado OAuth (access_token/refresh_token) con backend configurable:
   `MERCADO_LIBRE_OAUTH_BACKEND=file` (default, para correr `--mode
   authorize` a mano en tu maquina) o `=secretmanager` (para el Cloud Run
   Job). Un archivo JSON local no sobrevive entre ejecuciones de un Cloud
   Run Job (filesystem efimero) -- sin esto, el job se rompe solo la primera
   vez que Mercado Libre rota el refresh_token. Mismo problema, mismo tipo
   de fix, que ya resolvimos en GCS MCP con Firestore (ahi se eligio
   Firestore porque FastMCP lo pedia asi; aca se eligio Secret Manager por
   ser un unico blob JSON chico, mas simple para este caso).

-----------------------------------------------------------------------------
REVISION 1/10/2026 -- datos a nivel campana + set completo de metricas
(a pedido del usuario: "si, quiero traer toda la informacion a nivel campana")
-----------------------------------------------------------------------------
Hasta esta revision, cada fila era un TOTAL por cuenta+fecha+plataforma (un
id de campana sintetico), y solo se pedia "cost" a la API -- por eso
impressions/clicks/conversions/revenue siempre salian en 0. Cambios:

1. Cada fila ahora es UNA CAMPANA REAL (campaign_id/campaign_name vienen de
   la API de Mercado Ads, ya no son sinteticos/agregados). Se elimino la
   estructura de agregacion por dict (`add_mercado_libre_spend`) -- ya no
   hace falta, cada campana ya es su propia fila.
2. `fetch_mercado_libre_campaigns()` ahora pide el set completo de metricas
   de Product Ads confirmado contra documentacion oficial (parametro
   `metrics`, ver PADS_METRICS_DEFAULT / MERCADO_LIBRE_PADS_METRICS mas
   abajo), no solo "cost". Fuente:
   https://developers.mercadolivre.com.br/en_us/product-ads-us-read
3. `DailyMetric` suma los campos ctr, cpc, acos, cvr, roas, direct_amount,
   indirect_amount, total_amount, direct_units_quantity,
   indirect_units_quantity (nombres tal cual la API, para no perder
   precision) -- ver el docstring de la clase para el mapeo de los 4 campos
   "genericos" (impressions/clicks/conversions/revenue) a sus equivalentes
   reales de la API.
4. Funciones eliminadas por quedar obsoletas con este cambio:
   `add_mercado_libre_spend()` (agregador por dict, ya no aplica),
   `fetch_mercado_libre_campaign_metrics()` (el endpoint [INFERIDO] que
   habia quedado marcado como no confirmado en la seccion de transparencia
   original -- se reemplazo por el `metrics` embebido en la respuesta de
   fetch_mercado_libre_campaigns, que si esta confirmado), y
   `fetch_mercado_libre_product_ads_daily_metrics()` (la funcion que
   colapsaba a nivel cuenta). Reemplazadas por `_build_pads_metric()` /
   `_build_display_metric()`, que arman una fila por campana.
5. Display Ads (DSP) sigue [INFERIDO] -- no se encontro documentacion
   oficial equivalente a la de Product Ads para los nombres de campo de
   metricas de Display, asi que `_build_display_metric()` solo llena
   spend/impressions/clicks de forma oportunista (ver su docstring).
6. Investigado pero NO adoptado: la documentacion menciona un parametro
   `aggregation_type=DAILY` (y `metrics_summary=true`) que en teoria daria
   el desglose diario real de todo un mes en un solo request -- pero se vio
   en una pagina cuyo endpoint base difiere del que usamos (confirmado
   funcionando) aca. Queda como investigacion futura, no implementado.

Como validar: correr `--mode extract --scope daily --months 1` (mismo
comando que ya corriste con exito) y comparar unas pocas filas contra el
dashboard de Mercado Ads antes de confiar en el pipeline para reportes.

-----------------------------------------------------------------------------
INSTALACION
-----------------------------------------------------------------------------
pip install -r requirements.txt --break-system-packages

-----------------------------------------------------------------------------
CONFIG (variables de entorno / .env)
-----------------------------------------------------------------------------
MERCADO_LIBRE_CLIENT_ID
MERCADO_LIBRE_CLIENT_SECRET
MERCADO_LIBRE_ACCESS_TOKEN
MERCADO_LIBRE_REFRESH_TOKEN        (opcional -- si no esta, no hay auto-renovacion)
MERCADO_LIBRE_ADVERTISER_IDS       (opcional -- si esta vacio, se descubren via API)
MERCADO_LIBRE_PRODUCTS             (opcional -- ej "PADS,DSP"; vacio = solo PADS)
MERCADO_LIBRE_PADS_METRICS         (opcional -- lista de metricas de Product Ads a pedir a la API,
                                     separadas por coma; default = set completo confirmado, ver
                                     PADS_METRICS_DEFAULT arriba. Recortar aca si algun combo da 400.)
MERCADO_LIBRE_API_BASE_URL         (default: https://api.mercadolibre.com)
MERCADO_LIBRE_SYNC_TIMEOUT_SECONDS (default: 30)
MERCADO_LIBRE_OAUTH_STATE_PATH     (default: mercadolibre_oauth_state.json -- solo si OAUTH_BACKEND=file)
MERCADO_LIBRE_OAUTH_BACKEND        (default: file; "secretmanager" para Cloud Run Job)
MERCADO_LIBRE_OAUTH_SECRET_NAME    (obligatoria si OAUTH_BACKEND=secretmanager -- nombre completo del secret, ej:
                                     projects/worldsport-503720/secrets/meli-oauth-state)
MERCADO_LIBRE_SYNC_DAYS_BACK       (default: 5 -- ventana rodante de --mode daily-sync)
GCS_PROJECT / GCS_BUCKET           (obligatorias para --mode daily-sync -- mismas que usa el extractor de Magento)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo
import pandas as pd

import httpx

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Raiz del repo = dos niveles arriba de este archivo (mercado-libre/src/... ->
# repo root) -- mismo mecanismo que usa magento/src/extraccion_magento.py
# para poder hacer `from _common.gcs_utils import escribir_a_gcs` tanto
# corriendo local (parado en la raiz del repo) como dentro del container
# (el Dockerfile copia _common/ y mercado-libre/src/ preservando esta misma
# estructura relativa -- ver mercado-libre/Dockerfile). Si este archivo se
# corre suelto (fuera del repo worldsport-extractors), el import de
# _common falla solo si se usa --mode daily-sync -- authorize/extract no lo
# necesitan.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

logger = logging.getLogger("mercado_libre_metrics")
logging.basicConfig(level=logging.INFO)

BSAS_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

# Metricas de Product Ads confirmadas contra documentacion oficial (1/10/2026):
# https://developers.mercadolivre.com.br/en_us/product-ads-us-read -- el
# parametro "metrics" acepta una lista separada por comas de estos nombres.
# Se pide el set completo por default porque el usuario pidio "toda la
# informacion a nivel campana"; si algun combo especifico da 400 (algunas
# metricas podrian ser incompatibles entre si o requerir un rango de fechas
# distinto -- no confirmado), recortar via MERCADO_LIBRE_PADS_METRICS.
PADS_METRICS_DEFAULT = (
    "clicks,prints,ctr,cost,cpc,acos,cvr,roas,"
    "direct_amount,indirect_amount,total_amount,"
    "direct_units_quantity,indirect_units_quantity,units_quantity"
)


# =============================================================================
# Config -- equivalente a this.configService.* en tu TS
# =============================================================================

class Config:
    def __init__(self):
        self.client_id = os.environ.get("MERCADO_LIBRE_CLIENT_ID", "")
        self.client_secret = os.environ.get("MERCADO_LIBRE_CLIENT_SECRET", "")
        self.access_token = os.environ.get("MERCADO_LIBRE_ACCESS_TOKEN", "")
        self.refresh_token = os.environ.get("MERCADO_LIBRE_REFRESH_TOKEN", "")
        self.advertiser_ids_raw = os.environ.get("MERCADO_LIBRE_ADVERTISER_IDS", "")
        self.products_raw = os.environ.get("MERCADO_LIBRE_PRODUCTS", "")
        self.api_base_url = os.environ.get("MERCADO_LIBRE_API_BASE_URL", "https://api.mercadolibre.com")
        # Dominio del "Advertiser Hub" -- probablemente una API/generacion
        # distinta para endpoints de Ads. Cargada pero TODAVIA NO USADA por
        # las funciones de este archivo hasta confirmar la estructura real
        # de sus paths (ver nota en la respuesta del chat). No cambiar los
        # endpoints existentes a este base_url sin antes verificar los paths
        # reales (ej. inspeccionando la pestaña Network del navegador contra
        # https://ads.mercadolibre.com.ar/advertiser-hub).
        self.ads_api_base_url = os.environ.get("MERCADO_LIBRE_ADS_API_BASE_URL", self.api_base_url)
        self.sync_timeout_seconds = float(os.environ.get("MERCADO_LIBRE_SYNC_TIMEOUT_SECONDS", "30"))
        self.oauth_state_path = os.environ.get("MERCADO_LIBRE_OAUTH_STATE_PATH", "mercadolibre_oauth_state.json")
        # "file" (default, para correr --mode authorize a mano) o
        # "secretmanager" (para el Cloud Run Job -- ver nota de la revision
        # 30/9/2026 en la cabecera del archivo sobre por que un archivo local
        # no alcanza en un container efimero).
        self.oauth_backend = os.environ.get("MERCADO_LIBRE_OAUTH_BACKEND", "file").lower()
        self.oauth_secret_name = os.environ.get("MERCADO_LIBRE_OAUTH_SECRET_NAME", "")
        self.sync_days_back = int(os.environ.get("MERCADO_LIBRE_SYNC_DAYS_BACK", "5"))
        self.gcs_project = os.environ.get("GCS_PROJECT", "")
        self.gcs_bucket = os.environ.get("GCS_BUCKET", "")
        self.pads_metrics = os.environ.get("MERCADO_LIBRE_PADS_METRICS", PADS_METRICS_DEFAULT)

    def parse_advertisers(self) -> list["AdvertiserConfig"]:
        # Cada entrada viene como "advertiser_id:account_name:referencia"
        # (ej. "1044417:DC Shoes 3P:DC Shoes"), separadas por coma entre
        # anunciantes. Si falta account_name/referencia, caen en fallback
        # al id / account_name respectivamente.
        entries = [a.strip() for a in self.advertiser_ids_raw.split(",") if a.strip()]
        result = []
        for entry in entries:
            parts = [p.strip() for p in entry.split(":")]
            adv_id = parts[0]
            account_name = parts[1] if len(parts) > 1 and parts[1] else adv_id
            referencia = parts[2] if len(parts) > 2 and parts[2] else account_name
            result.append(AdvertiserConfig(id=adv_id, account_name=account_name, referencia=referencia))
        return result

    def parse_products(self) -> set:
        return {p.strip().upper() for p in self.products_raw.split(",") if p.strip()}


config = Config()


# =============================================================================
# Tipos -- equivalentes a tus interfaces TS
# =============================================================================

@dataclass
class AdvertiserConfig:
    id: str
    account_name: str
    referencia: str


@dataclass
class DailyMetric:
    """
    REVISION 1/10/2026 -- a pedido del usuario ("traer toda la informacion a
    nivel campana"), esta fila ahora representa UNA CAMPANA real (campaign_id
    y campaign_name vienen de la API, ya no son un id sintetico agregado a
    nivel cuenta) y trae todas las metricas que confirma la documentacion
    oficial de Mercado Ads para Product Ads (metrics query param):
    https://developers.mercadolivre.com.br/en_us/product-ads-us-read

    Mapeo de los campos "genericos" (compatibles con como ya se veian antes,
    para no romper si algo rio abajo los espera con estos nombres):
      impressions  <- "prints"
      clicks       <- "clicks"           (mismo nombre en la API)
      conversions  <- "units_quantity"   (unidades vendidas atribuidas al ad,
                                           directas+indirectas -- la metrica
                                           mas parecida a "conversiones" que
                                           expone la API; no hay un campo
                                           literal "conversions")
      revenue      <- "total_amount"     (definicion oficial: "Sum of the
                                           value of sales obtained from your
                                           Product Ad" -- directas+indirectas)
      spend        <- "cost"             (sin cambios)

    Los campos de abajo (ctr, cpc, acos, cvr, roas, direct_amount,
    indirect_amount, total_amount, direct_units_quantity,
    indirect_units_quantity) son ADICIONALES a esos 4 genericos -- se dejan
    con el nombre tal cual lo devuelve la API para no perder precision.
    Display Ads (DSP) por ahora solo llena spend/impressions/clicks si la
    respuesta los trae -- el resto queda en 0 porque los nombres de campo
    de Display siguen sin confirmar contra documentacion oficial (a
    diferencia de Product Ads, que si se confirmo).
    """
    date: str
    cliente: str
    marca: str
    referencia: str
    account_id: str
    account_name: str
    plataforma: str
    campaign_id: str
    campaign_name: str
    granularity: str
    coverage_end_date: Optional[str]
    spend: float
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    revenue: float = 0.0
    ctr: float = 0.0
    cpc: float = 0.0
    acos: float = 0.0
    cvr: float = 0.0
    roas: float = 0.0
    direct_amount: float = 0.0
    indirect_amount: float = 0.0
    total_amount: float = 0.0
    direct_units_quantity: float = 0.0
    indirect_units_quantity: float = 0.0


@dataclass
class OAuthState:
    access_token: str = ""
    refresh_token: str = ""
    expires_at: str = ""
    updated_at: str = ""


# =============================================================================
# Helpers chicos -- equivalentes a numberValue / round2 / today / monthStart
# =============================================================================

def _raise_oauth_error(response: "httpx.Response", context: str) -> None:
    """
    REVISION 1/10/2026 -- antes usabamos response.raise_for_status() a secas,
    que arma un mensaje generico ("Client error '400 Bad Request' for url
    ...") SIN el cuerpo de la respuesta -- y el cuerpo es justamente donde
    Mercado Libre manda el motivo real (ej. {"error": "invalid_grant",
    "message": "..."}: refresh_token ya usado/invalido, o
    {"error": "invalid_client", ...}: client_id/client_secret no coinciden).
    Sin esto, un 400 en el refresh es indistinguible de cualquier otro 400 --
    hay que adivinar. Esta funcion no cambia ningun comportamiento en el
    camino exitoso (2xx), solo enriquece el mensaje de error.
    """
    if response.status_code < 400:
        return
    try:
        body = response.json()
    except Exception:
        body = response.text[:500]
    raise RuntimeError(
        f"Mercado Libre OAuth error ({context}): HTTP {response.status_code} -- {body}"
    )


def number_value(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def round2(value: float) -> float:
    return round(value, 2)


def today() -> str:
    """
    FIX (revision 30/9/2026): antes usaba date.today() a secas -- correcto
    en una laptop, pero en un container de Cloud Run (reloj en UTC) desalinea
    el "hoy" del dia de negocio real de Argentina en el tramo ~21hs-24hs
    (ya séria el dia siguiente en UTC). Mismo fix que ya se aplico en el
    extractor de Magento (HOY timezone-aware).
    """
    return datetime.now(BSAS_TZ).date().isoformat()


def rolling_window_days(days_back: int, exclude_today: bool = True) -> list[str]:
    """
    Devuelve una lista de fechas ISO (mas vieja a mas nueva) para la ventana
    rodante de --mode daily-sync. `exclude_today=True` (default) deja afuera
    el dia en curso -- SUPUESTO DECLARADO: el gasto de Mercado Ads del dia de
    hoy todavia puede no estar asentado/cerrado. Si se confirma que la API
    ya devuelve el dato del dia corriente como definitivo, se puede correr
    con exclude_today=False (o sumar 1 a MERCADO_LIBRE_SYNC_DAYS_BACK).
    """
    hoy = datetime.now(BSAS_TZ).date()
    ultimo_dia = hoy - timedelta(days=1) if exclude_today else hoy
    primer_dia = ultimo_dia - timedelta(days=days_back - 1)
    return [
        (primer_dia + timedelta(days=i)).isoformat()
        for i in range((ultimo_dia - primer_dia).days + 1)
    ]


def month_start(iso_date: str) -> str:
    d = date.fromisoformat(iso_date)
    return d.replace(day=1).isoformat()


def month_bounds(year: int, month: int) -> tuple[str, str]:
    """Primer y ultimo dia calendario de un mes, como strings ISO."""
    import calendar
    start = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end = date(year, month, last_day)
    return start.isoformat(), end.isoformat()


def last_n_months(n: int, reference_date: Optional[date] = None) -> list[tuple[str, str]]:
    """
    Devuelve una lista de (start_date, end_date) para los ultimos n meses
    calendario, del mas viejo al mas nuevo. El mes actual (si reference_date
    cae dentro de el) se corta en reference_date en vez de ir hasta fin de
    mes, para no pedirle a la API fechas futuras.
    """
    ref = reference_date or date.today()
    months: list[tuple[str, str]] = []
    y, m = ref.year, ref.month

    for _ in range(n):
        start, end = month_bounds(y, m)
        if y == ref.year and m == ref.month:
            end = ref.isoformat()
        months.append((start, end))

        m -= 1
        if m == 0:
            m = 12
            y -= 1

    return list(reversed(months))


def extract_mercado_libre_list(data: Any) -> list:
    """[INFERIDO] Ver nota (3) en la cabecera del archivo."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key in ("results", "campaigns", "data", "items", "advertisers"):
        value = data.get(key)
        if isinstance(value, list):
            return value
    return []


# =============================================================================
# Punto de entrada publico -- equivalente a fetchMercadoLibreMetrics
# =============================================================================

async def fetch_mercado_libre_metrics(scope: str, on_date: Optional[str] = None,
                                        range_start_date: Optional[str] = None,
                                        failures_out: Optional[list] = None) -> list[DailyMetric]:
    on_date = on_date or today()
    try:
        return await _fetch_mercado_libre_api_metrics(scope, on_date, range_start_date, failures_out)
    except Exception as error:
        logger.warning(f"Mercado Libre API {scope} sync returned no metrics: {error}")
        raise


async def fetch_mercado_libre_metrics_rolling_window(days_back: int, exclude_today: bool = True) -> tuple[list[DailyMetric], list[str]]:
    """
    Agregado para --mode daily-sync (revision 30/9/2026): a diferencia de
    fetch_mercado_libre_metrics_last_n_months (que trae UN total agregado
    por mes calendario), esto llama scope="daily" dia por dia sobre la
    ventana rodante de los ultimos `days_back` dias -- son datos diarios
    reales (no un promedio ni un prorrateo del total mensual).

    Devuelve (metricas, fallos) -- `fallos` junta los fallos de TODOS los
    dias/advertisers para que el caller (--mode daily-sync) pueda decidir el
    exit code del job (ver _main).
    """
    all_metrics: list[DailyMetric] = []
    all_failures: list[str] = []

    for day in rolling_window_days(days_back, exclude_today=exclude_today):
        day_failures: list[str] = []
        try:
            metrics = await fetch_mercado_libre_metrics(scope="daily", on_date=day, failures_out=day_failures)
            all_metrics.extend(metrics)
        except Exception as error:
            day_failures.append(f"{day}: sync completo del dia fallo ({error})")
            logger.warning(f"Se salteo el dia {day} por error: {error}")
        all_failures.extend(f"{day} -- {f}" for f in day_failures)

    return all_metrics, all_failures


async def fetch_mercado_libre_metrics_last_n_months(n: int) -> list[DailyMetric]:
    """
    Trae metricas mensuales de cada uno de los ultimos n meses calendario
    (uno por uno, cada llamada dentro del limite de 90 dias de la API), y
    devuelve todo concatenado. El mes actual sale parcial (hasta hoy).

    Si algun mes individual falla (ej. un 404/401 puntual), se loguea como
    warning y se sigue con los demas -- no se aborta todo el historico por
    un mes con problemas.
    """
    all_metrics: list[DailyMetric] = []

    for start_date, end_date in last_n_months(n):
        logger.info(f"Trayendo metricas de Mercado Ads: {start_date} a {end_date}...")
        try:
            metrics = await fetch_mercado_libre_metrics(
                scope="monthly", on_date=end_date, range_start_date=start_date
            )
            all_metrics.extend(metrics)
        except Exception as error:
            logger.warning(f"Se salteo el mes {start_date} a {end_date} por error: {error}")

    return all_metrics


# =============================================================================
# Orquestador principal -- equivalente a fetchMercadoLibreApiMetrics
# =============================================================================

async def _fetch_mercado_libre_api_metrics(scope: str, on_date: str,
                                              range_start_date: Optional[str],
                                              failures_out: Optional[list] = None) -> list[DailyMetric]:
    """
    REVISION 1/10/2026: a pedido del usuario ("traer toda la informacion a
    nivel campana"), esta funcion dejo de agregar todo a un total por
    cuenta -- ahora arma UNA FILA POR CAMPANA REAL (campaign_id/campaign_name
    de la API, no un id sintetico), con todas las metricas confirmadas de
    Product Ads (ver PADS_METRICS_DEFAULT / DailyMetric). Para "monthly" esto
    sigue siendo un total por campana para todo el mes (la API no da
    granularidad diaria real en un solo request -- ver nota en
    fetch_mercado_libre_campaigns); para "daily" (usado por --mode
    daily-sync) es el dato real de ese dia puntual, por campana.
    """
    configured_advertisers = config.parse_advertisers()
    access_token = await get_mercado_libre_access_token()

    if not access_token:
        raise RuntimeError("Mercado Libre access token/refresh token not configured")

    start_date = range_start_date or (month_start(on_date) if scope == "monthly" else on_date)
    end_date = on_date

    out: list[DailyMetric] = []

    try:
        products = config.parse_products()
        include_product_ads = len(products) == 0 or "PADS" in products
        include_display = "DSP" in products or "DISPLAY" in products

        advertisers = configured_advertisers or await fetch_mercado_libre_advertisers(access_token)

        product_ads_failures: list[str] = []
        display_failures: list[str] = []

        for advertiser in advertisers:
            mapping = await resolve_brand_mapping(advertiser.referencia)

            if include_product_ads:
                try:
                    campaigns = await fetch_mercado_libre_campaigns(access_token, advertiser.id, start_date, end_date)
                    logger.info(
                        f"Mercado Libre PADS {scope} advertiser {advertiser.id} returned {len(campaigns)} campana(s)"
                    )
                    for campaign in campaigns:
                        out.append(_build_pads_metric(scope, end_date, advertiser, mapping, campaign))
                except Exception as error:
                    detail = str(error)
                    product_ads_failures.append(f"{advertiser.id}: {detail}")
                    logger.warning(f"Mercado Libre PADS {scope} sync skipped for advertiser {advertiser.id}: {detail}")

            if include_display:
                try:
                    campaigns = await fetch_mercado_libre_display_campaigns(access_token, advertiser.id)
                    for campaign in campaigns:
                        metric_rows = await fetch_mercado_libre_display_campaign_metrics(
                            access_token, advertiser.id, str(campaign["id"]), start_date, end_date
                        )
                        out.append(_build_display_metric(scope, end_date, advertiser, mapping, campaign, metric_rows))
                except Exception as error:
                    detail = str(error)
                    display_failures.append(f"{advertiser.id}: {detail}")
                    logger.warning(f"Mercado Libre DSP {scope} sync skipped for advertiser {advertiser.id}: {detail}")

        # FIX (revision 30/9/2026): antes, si fallaba UN solo advertiser, se
        # descartaban TODOS los datos del período (incluidos los advertisers
        # que sí funcionaron) porque se relanzaba la excepción antes de armar
        # `out`. Ahora se devuelven los datos parciales y solo se deja
        # constancia en el log de qué advertiser(s) quedaron incompletos --
        # evita perder el mes entero por un timeout puntual de un anunciante.
        if include_product_ads and product_ads_failures:
            logger.warning(
                f"Mercado Libre PADS {scope} sync incompleto -- fallaron: "
                f"{'; '.join(product_ads_failures)}. Se devuelven los datos de "
                f"los demas advertisers; revisar estos antes de dar el numero por cerrado."
            )
        if include_display and display_failures:
            logger.warning(
                f"Mercado Libre DSP {scope} sync incompleto -- fallaron: "
                f"{'; '.join(display_failures)}. Se devuelven los datos de "
                f"los demas advertisers; revisar estos antes de dar el numero por cerrado."
            )

        if failures_out is not None:
            failures_out.extend(f"PADS {a}" for a in product_ads_failures)
            failures_out.extend(f"DSP {a}" for a in display_failures)

        out.sort(key=lambda m: (m.date, m.account_name or "", m.plataforma, m.campaign_name or ""))
        return out

    except Exception as error:
        raise RuntimeError(f"Mercado Libre API {scope} sync failed ({error})") from error


def _build_pads_metric(scope: str, end_date: str, advertiser: AdvertiserConfig,
                         mapping: dict, campaign: dict) -> DailyMetric:
    """Arma una fila por campana de Product Ads con todas las metricas
    confirmadas (ver nota en DailyMetric)."""
    m = campaign.get("metrics") or {}
    return DailyMetric(
        date=end_date,
        cliente=mapping["cliente"],
        marca=mapping["marca"],
        referencia=advertiser.referencia,
        account_id=advertiser.id,
        account_name=advertiser.account_name,
        plataforma="MELI-PADS",
        campaign_id=str(campaign.get("id") or ""),
        campaign_name=campaign.get("name") or "",
        granularity=scope,
        coverage_end_date=end_date if scope == "monthly" else None,
        spend=round2(number_value(m.get("cost"))),
        impressions=int(round(number_value(m.get("prints")))),
        clicks=int(round(number_value(m.get("clicks")))),
        conversions=round2(number_value(m.get("units_quantity"))),
        revenue=round2(number_value(m.get("total_amount"))),
        ctr=number_value(m.get("ctr")),
        cpc=number_value(m.get("cpc")),
        acos=number_value(m.get("acos")),
        cvr=number_value(m.get("cvr")),
        roas=number_value(m.get("roas")),
        direct_amount=round2(number_value(m.get("direct_amount"))),
        indirect_amount=round2(number_value(m.get("indirect_amount"))),
        total_amount=round2(number_value(m.get("total_amount"))),
        direct_units_quantity=number_value(m.get("direct_units_quantity")),
        indirect_units_quantity=number_value(m.get("indirect_units_quantity")),
    )


def _build_display_metric(scope: str, end_date: str, advertiser: AdvertiserConfig,
                            mapping: dict, campaign: dict, metric_rows: list[dict]) -> DailyMetric:
    """
    [INFERIDO -- Display Ads] a diferencia de Product Ads, los nombres de
    campo de la respuesta de metricas de Display siguen sin confirmar
    contra documentacion oficial (no se encontro una pagina equivalente a
    product-ads-us-read para Display). Se suma `consumed_budget` (unico
    campo que SI viene del codigo TS original) y se prueban oportunistamente
    "clicks"/"prints"/"impressions" si vienen -- si no, quedan en 0. Si vas a
    usar Display en serio, confirmar los nombres reales (ej. inspeccionando
    la respuesta cruda con un print()) y ajustar esta funcion.
    `metric_rows` puede traer mas de un registro (ej. por dia) para el mismo
    rango pedido -- se suman todos en una sola fila por campana, mismo
    criterio que Product Ads (un total por campana para el rango).
    """
    spend = sum(number_value(r.get("consumed_budget")) for r in metric_rows)
    clicks = sum(number_value(r.get("clicks")) for r in metric_rows)
    impressions = sum(number_value(r.get("prints") or r.get("impressions")) for r in metric_rows)
    return DailyMetric(
        date=end_date,
        cliente=mapping["cliente"],
        marca=mapping["marca"],
        referencia=advertiser.referencia,
        account_id=advertiser.id,
        account_name=advertiser.account_name,
        plataforma="MELI-DSP",
        campaign_id=str(campaign.get("id") or ""),
        campaign_name=campaign.get("name") or "",
        granularity=scope,
        coverage_end_date=end_date if scope == "monthly" else None,
        spend=round2(spend),
        impressions=int(round(impressions)),
        clicks=int(round(clicks)),
    )


async def resolve_brand_mapping(referencia: str) -> dict:
    """[FUERA DE ALCANCE] Ver nota (5) en la cabecera del archivo -- stub."""
    return {"cliente": "WorldSport", "marca": referencia}


# =============================================================================
# Descubrimiento de anunciantes -- equivalente a fetchMercadoLibreAdvertisers
# =============================================================================

async def fetch_mercado_libre_advertisers(access_token: str) -> list[AdvertiserConfig]:
    response = await mercado_libre_api_get(
        f"{config.api_base_url}/advertising/advertisers",
        access_token=access_token,
        params={"product_id": "PADS"},
    )
    data = response.json()
    advertisers = data.get("advertisers") or extract_mercado_libre_list(data)

    result = []
    for adv in advertisers:
        adv_id = str(adv.get("advertiser_id") or adv.get("id") or "")
        if not adv_id:
            continue
        account_name = str(adv.get("advertiser_name") or adv.get("name") or adv.get("account_name") or adv_id)
        result.append(AdvertiserConfig(id=adv_id, account_name=account_name, referencia=account_name))
    return result


# =============================================================================
# Campanas Product Ads (PADS) -- equivalente a fetchMercadoLibreCampaigns
# =============================================================================

async def fetch_mercado_libre_campaigns(access_token: str, advertiser_id: str,
                                           start_date: Optional[str] = None,
                                           end_date: Optional[str] = None,
                                           metrics: Optional[str] = None) -> list[dict]:
    """
    REVISION 1/10/2026: `metrics` ahora pide el set completo confirmado
    contra documentacion oficial (PADS_METRICS_DEFAULT / config.pads_metrics),
    no solo "cost" -- ver nota en DailyMetric. Cada campana devuelta trae el
    dict `metrics` completo tal cual lo da la API, para que el caller arme
    una fila POR CAMPANA (ya no se colapsa a un total por cuenta).
    """
    campaigns: list[dict] = []
    offset = 0
    limit = 50
    metrics = metrics if metrics is not None else config.pads_metrics

    while True:
        params = {"offset": offset, "limit": limit}
        if start_date and end_date:
            params.update({"date_from": start_date, "date_to": end_date, "metrics": metrics})

        response = await mercado_libre_api_get(
            f"{config.api_base_url}/advertising/MLA/advertisers/{advertiser_id}/product_ads/campaigns/search",
            access_token=access_token,
            headers={"api-version": "2"},
            params=params,
        )
        rows = extract_mercado_libre_list(response.json())

        for row in rows:
            campaign_id = str(row.get("id") or row.get("campaign_id") or row.get("campaignId") or "")
            if not campaign_id:
                continue
            campaigns.append({
                "id": campaign_id,
                "name": row.get("name") or row.get("campaign_name") or row.get("campaignName"),
                "metrics": row.get("metrics") or {},
            })

        if len(rows) < limit:
            break
        offset += limit
        if offset >= 10000:
            break

    return campaigns


# =============================================================================
# Campanas Display (DSP) -- equivalente a fetchMercadoLibreDisplayCampaigns / *Metrics
# =============================================================================

async def fetch_mercado_libre_display_campaigns(access_token: str, advertiser_id: str) -> list[dict]:
    campaigns: list[dict] = []
    offset = 0
    limit = 50

    while True:
        response = await mercado_libre_api_get(
            f"{config.api_base_url}/advertising/advertisers/{advertiser_id}/display/campaigns",
            access_token=access_token,
            params={"offset": offset, "limit": limit},
        )
        rows = extract_mercado_libre_list(response.json())

        for row in rows:
            campaign_id = str(row.get("id") or row.get("campaign_id") or row.get("campaignId") or "")
            if not campaign_id:
                continue
            campaigns.append({
                "id": campaign_id,
                "name": row.get("name") or row.get("campaign_name") or row.get("campaignName"),
                "goal": row.get("goal"),
            })

        if len(rows) < limit:
            break
        offset += limit
        if offset >= 10000:
            break

    return campaigns


async def fetch_mercado_libre_display_campaign_metrics(access_token: str, advertiser_id: str,
                                                           campaign_id: str, start_date: str,
                                                           end_date: str) -> list[dict]:
    response = await mercado_libre_api_get(
        f"{config.api_base_url}/advertising/advertisers/{advertiser_id}/display/campaigns/{campaign_id}/metrics",
        access_token=access_token,
        params={"date_from": start_date, "date_to": end_date},
    )
    data = response.json()
    rows = data.get("metrics")
    if isinstance(rows, list) and rows:
        return rows
    summary = data.get("summary")
    return [summary] if summary else []


# =============================================================================
# Wrapper HTTP con reintento en 401 -- equivalente a mercadoLibreApiGet
# =============================================================================

async def mercado_libre_api_get(url: str, access_token: Optional[str] = None,
                                   headers: Optional[dict] = None,
                                   params: Optional[dict] = None) -> httpx.Response:
    token = access_token or await get_mercado_libre_access_token()
    request_headers = {**(headers or {}), "Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=config.sync_timeout_seconds) as client:
        response = await client.get(url, headers=request_headers, params=params)

        if response.status_code == 401:
            refreshed_token = await refresh_mercado_libre_access_token(force=True)
            request_headers["Authorization"] = f"Bearer {refreshed_token}"
            response = await client.get(url, headers=request_headers, params=params)

        response.raise_for_status()
        return response


# =============================================================================
# OAuth: lectura/guardado de estado persistido
# -- equivalente a load/saveMercadoLibreOAuthState + getMercadoLibreAccessToken
#
# Dos backends (MERCADO_LIBRE_OAUTH_BACKEND, ver cabecera del archivo):
#   - "file" (default): archivo JSON local -- sirve para correr --mode
#     authorize/extract a mano en tu maquina.
#   - "secretmanager": Secret Manager de GCP -- OBLIGATORIO para --mode
#     daily-sync corriendo como Cloud Run Job, porque el filesystem de un
#     Job es efimero entre ejecuciones (un archivo local se pierde apenas
#     termina el container). Mismo problema que ya resolvimos en GCS MCP
#     con Firestore -- aca se eligio Secret Manager por ser un unico blob
#     JSON chico que cambia poco (cada ejecucion exitosa agrega una version
#     nueva del secret).
# =============================================================================

def _oauth_state_path() -> Path:
    return Path(config.oauth_state_path)


def _state_from_dict(parsed: dict) -> OAuthState:
    return OAuthState(
        access_token=str(parsed.get("accessToken", parsed.get("access_token", ""))),
        refresh_token=str(parsed.get("refreshToken", parsed.get("refresh_token", ""))),
        expires_at=str(parsed.get("expiresAt", parsed.get("expires_at", ""))),
        updated_at=str(parsed.get("updatedAt", parsed.get("updated_at", ""))),
    )


def _state_to_dict(state: OAuthState) -> dict:
    return {
        "accessToken": state.access_token,
        "refreshToken": state.refresh_token,
        "expiresAt": state.expires_at,
        "updatedAt": state.updated_at,
    }


def _load_state_from_file() -> OAuthState:
    path = _oauth_state_path()
    if path.exists():
        try:
            raw = path.read_text(encoding="utf-8-sig")
            return _state_from_dict(json.loads(raw))
        except Exception as error:
            logger.warning(f"No se pudo leer el estado de OAuth persistido en {path}: {error}")

    if config.access_token or config.refresh_token:
        return OAuthState(access_token=config.access_token, refresh_token=config.refresh_token)

    return OAuthState()


def _save_state_to_file(state: OAuthState) -> None:
    path = _oauth_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_state_to_dict(state), indent=2), encoding="utf-8")


def _secret_manager_client():
    try:
        from google.cloud import secretmanager
    except ImportError as error:
        raise RuntimeError(
            "MERCADO_LIBRE_OAUTH_BACKEND=secretmanager requiere el paquete "
            "google-cloud-secret-manager (ya esta en requirements.txt del "
            "Cloud Run Job; si corres esto local con este backend, "
            "'pip install google-cloud-secret-manager --break-system-packages')."
        ) from error
    return secretmanager.SecretManagerServiceClient()


def _load_state_from_secret_manager() -> OAuthState:
    if not config.oauth_secret_name:
        raise RuntimeError(
            "Falta MERCADO_LIBRE_OAUTH_SECRET_NAME (nombre completo del secret, ej. "
            "projects/worldsport-503720/secrets/meli-oauth-state) -- obligatoria con "
            "MERCADO_LIBRE_OAUTH_BACKEND=secretmanager."
        )
    client = _secret_manager_client()
    try:
        response = client.access_secret_version(request={"name": f"{config.oauth_secret_name}/versions/latest"})
    except Exception as error:
        logger.warning(f"No se pudo leer el secret {config.oauth_secret_name}: {error}")
        if config.access_token or config.refresh_token:
            return OAuthState(access_token=config.access_token, refresh_token=config.refresh_token)
        raise
    parsed = json.loads(response.payload.data.decode("utf-8"))
    return _state_from_dict(parsed)


def _save_state_to_secret_manager(state: OAuthState) -> None:
    if not config.oauth_secret_name:
        raise RuntimeError("Falta MERCADO_LIBRE_OAUTH_SECRET_NAME -- no se puede guardar el token renovado.")
    client = _secret_manager_client()
    payload = json.dumps(_state_to_dict(state)).encode("utf-8")
    # add_secret_version agrega una version nueva -- el secret en si
    # (projects/.../secrets/meli-oauth-state) tiene que existir de antes,
    # creado una sola vez en el paso de despliegue (ver guia de deploy).
    client.add_secret_version(request={"parent": config.oauth_secret_name, "payload": {"data": payload}})


def load_mercado_libre_oauth_state() -> OAuthState:
    if config.oauth_backend == "secretmanager":
        return _load_state_from_secret_manager()
    return _load_state_from_file()


def save_mercado_libre_oauth_state(state: OAuthState) -> None:
    if config.oauth_backend == "secretmanager":
        _save_state_to_secret_manager(state)
        return
    _save_state_to_file(state)


def is_mercado_libre_token_expiring(state: OAuthState, margin_seconds: int = 300) -> bool:
    """[INFERIDO] Ver nota (4) en la cabecera del archivo."""
    if not state.expires_at:
        return True
    try:
        expires_at = datetime.fromisoformat(state.expires_at)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return datetime.now(timezone.utc) >= expires_at - timedelta(seconds=margin_seconds)


async def get_mercado_libre_access_token() -> str:
    state = load_mercado_libre_oauth_state()

    if state.access_token and not is_mercado_libre_token_expiring(state):
        return state.access_token

    can_refresh = bool(state.refresh_token or config.refresh_token)
    if can_refresh:
        return await refresh_mercado_libre_access_token(force=False)

    return state.access_token or config.access_token


async def refresh_mercado_libre_access_token(force: bool) -> str:
    state = load_mercado_libre_oauth_state()
    refresh_token = state.refresh_token or config.refresh_token

    if not refresh_token:
        raise RuntimeError(
            "Mercado Libre refresh_token no configurado -- no se puede renovar el "
            "access_token automaticamente. Regenera uno manual (ver script anterior, "
            "--mode authorize) y setea MERCADO_LIBRE_ACCESS_TOKEN / MERCADO_LIBRE_REFRESH_TOKEN."
        )

    async with httpx.AsyncClient(timeout=config.sync_timeout_seconds) as client:
        response = await client.post(
            "https://api.mercadolibre.com/oauth/token",
            headers={"accept": "application/json", "content-type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "refresh_token",
                "client_id": config.client_id,
                "client_secret": config.client_secret,
                "refresh_token": refresh_token,
            },
        )
    _raise_oauth_error(response, context="refresh_token")
    tokens = response.json()

    expires_in = int(tokens.get("expires_in", 21600))
    now = datetime.now(timezone.utc)
    new_state = OAuthState(
        access_token=tokens.get("access_token", ""),
        refresh_token=tokens.get("refresh_token", refresh_token),
        expires_at=(now + timedelta(seconds=expires_in)).isoformat(),
        updated_at=now.isoformat(),
    )
    save_mercado_libre_oauth_state(new_state)
    return new_state.access_token


# =============================================================================
# Autorizacion inicial / regeneracion manual de tokens
# =============================================================================

async def authorize_flow() -> None:
    """Flujo interactivo -- correr una sola vez, o cuando el access_token y
    el refresh_token se hayan invalidado del todo (ej. revocaste el permiso
    desde Mercado Libre). Abre el login, pide el 'code' de la URL de
    redireccion, lo cambia por access_token + refresh_token, y los guarda en
    MERCADO_LIBRE_OAUTH_STATE_PATH. De ahi en mas, get_mercado_libre_access_token()
    se renueva solo usando ese refresh_token -- no hace falta volver a correr
    esto salvo que el refresh_token tambien se invalide."""
    import webbrowser

    redirect_uri = os.environ.get("MERCADO_LIBRE_REDIRECT_URI", "")
    if not config.client_id or not config.client_secret or not redirect_uri:
        raise RuntimeError(
            "Faltan MERCADO_LIBRE_CLIENT_ID / MERCADO_LIBRE_CLIENT_SECRET / "
            "MERCADO_LIBRE_REDIRECT_URI (esta ultima: cualquier URL que controles, "
            "ej. https://tudominio.com/callback)."
        )

    auth_url = (
        "https://auth.mercadolibre.com.ar/authorization"
        f"?response_type=code&client_id={config.client_id}&redirect_uri={redirect_uri}"
    )
    print("Abri esta URL, inicia sesion con la cuenta que administra Mercado Ads de WorldSport,")
    print("acepta los permisos, y copia el valor de 'code' de la URL a la que te redirige:\n")
    print(auth_url, "\n")
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    code = input("Pega aca el 'code' recibido: ").strip()

    async with httpx.AsyncClient(timeout=config.sync_timeout_seconds) as client:
        response = await client.post(
            "https://api.mercadolibre.com/oauth/token",
            headers={"accept": "application/json", "content-type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "authorization_code",
                "client_id": config.client_id,
                "client_secret": config.client_secret,
                "code": code,
                "redirect_uri": redirect_uri,
            },
        )
    _raise_oauth_error(response, context="authorization_code")
    tokens = response.json()

    expires_in = int(tokens.get("expires_in", 21600))
    now = datetime.now(timezone.utc)
    new_state = OAuthState(
        access_token=tokens.get("access_token", ""),
        refresh_token=tokens.get("refresh_token", ""),
        expires_at=(now + timedelta(seconds=expires_in)).isoformat(),
        updated_at=now.isoformat(),
    )
    save_mercado_libre_oauth_state(new_state)

    print(f"\n[OK] Tokens guardados en '{config.oauth_state_path}'.")
    print(f"     access_token nuevo (expira en {expires_in}s / ~{expires_in // 3600}hs).")
    if new_state.refresh_token:
        print("     refresh_token guardado -- de ahora en mas el script se renueva solo,")
        print("     no hace falta volver a correr --mode authorize salvo que se invalide.")
    else:
        print("     [AVISO] La respuesta no incluyo refresh_token -- vas a tener que repetir")
        print("     este paso manualmente cada vez que el access_token expire (6hs).")


# =============================================================================
# Entry point / CLI
# =============================================================================

async def _run_daily_sync() -> int:
    """
    Modo pensado para el Cloud Run Job (Cloud Scheduler lo dispara una vez
    por dia, sin intervencion humana -- ver mercado-libre/README.md y la
    guia de deploy en el proyecto Claude "Mauro"). A diferencia de
    --mode extract (pensado para uso manual/backfill puntual), este modo:
      - Usa scope=daily dia por dia sobre la ventana rodante
        MERCADO_LIBRE_SYNC_DAYS_BACK (no un agregado mensual).
      - Sube el resultado a GCS (raw/mercado-libre/inversion/) en vez de (o
        ademas de) un CSV local.
      - Devuelve un exit code distinto de 0 si hubo fallos parciales, para
        que la ejecucion del Cloud Run Job quede marcada como fallida y sea
        visible en `gcloud run jobs executions list` / alertas -- aunque los
        datos que SI se pudieron traer ya hayan quedado subidos a GCS (no se
        pierden por el fallo parcial, ver fix #1 de la revision 30/9/2026).
    """
    from _common.gcs_utils import escribir_a_gcs

    metrics, failures = await fetch_mercado_libre_metrics_rolling_window(config.sync_days_back)
    fecha_ejecucion = today()

    print(f"\n[OK] {len(metrics)} filas de metricas obtenidas (ventana de {config.sync_days_back} dias, hasta ayer).")

    if metrics:
        df_meli = pd.DataFrame([vars(m) for m in metrics])
        path = escribir_a_gcs(
            df_meli, fecha_ejecucion=fecha_ejecucion, fuente="mercado-libre", entidad="inversion",
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


async def _main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["authorize", "extract", "daily-sync"], default="extract")
    parser.add_argument("--scope", choices=["daily", "monthly"], default="monthly")
    parser.add_argument(
        "--months", type=int, default=10,
        help="Cantidad de meses hacia atras a traer (1 = solo el comportamiento normal de --scope; "
             "mayor a 1 = trae ese numero de meses calendario, uno por uno, y los junta todos). "
             "OJO: si --months > 1 (el default es 10), se ignora --scope -- fetch_mercado_libre_metrics_last_n_months "
             "siempre usa scope=monthly internamente. Para un dia puntual: --months 1 --scope daily."
    )
    parser.add_argument(
        "--split-by-month", action="store_true",
        help="Si se pasa, exporta un CSV separado por cada mes en vez de un unico CSV combinado."
    )
    args = parser.parse_args()

    if args.mode == "authorize":
        await authorize_flow()
        return

    if args.mode == "daily-sync":
        exit_code = await _run_daily_sync()
        sys.exit(exit_code)

    if args.months > 1:
        metrics = await fetch_mercado_libre_metrics_last_n_months(args.months)
    else:
        metrics = await fetch_mercado_libre_metrics(scope=args.scope)

    print(f"\n[OK] {len(metrics)} filas de metricas obtenidas.")

    if not metrics:
        return

    df_meli = pd.DataFrame([vars(m) for m in metrics])
    os.makedirs("output", exist_ok=True)

    if args.split_by_month:
        df_meli["_fecha"] = pd.to_datetime(df_meli["date"])
        df_meli["_mes"] = df_meli["_fecha"].dt.to_period("M").astype(str)

        for mes, df_mes in df_meli.groupby("_mes"):
            csv_path = os.path.join("output", f"mercadolibre_ads_{mes}.csv")
            df_mes.drop(columns=["_fecha", "_mes"]).to_csv(csv_path, index=False, encoding="utf-8-sig")
            print(f"[OK] {len(df_mes)} fila(s) -> {csv_path}")
    else:
        csv_path = os.path.join("output", f"mercadolibre_ads_{today()}.csv")
        df_meli.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[OK] Exportado a: {csv_path}")

    print(df_meli.head(20).to_string(index=False))


if __name__ == "__main__":
    asyncio.run(_main())