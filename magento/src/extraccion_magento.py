"""
Magento 2 -> Python (OAuth 1.0a)
Importa ordenes a nivel de linea de item (la composicion de cada orden).

- Auth: OAuth 1.0a (Consumer Key/Secret + Access Token/Secret).
- Trae ordenes ACTUALIZADAS entre 'desde' y 'hasta' (updated_at, no created_at
  -- ver el cambio marcado abajo, y por qué).
- Backoff ante 429/503 para no chocar con rate limits.
- Guarda el resultado en Parquet.

CAMBIOS respecto a la version anterior (ventana rodante):
1. El filtro de fecha ahora es sobre `updated_at`, no `created_at`. Con el
   analisis de las 943 ordenes de Ago 1-14 (mediana 14.3hs, p90 ~49hs, p99
   ~66.5hs entre creacion y ultima actualizacion), filtrar solo por
   created_at se perdia ordenes que tardan mas de un dia en asentarse --
   ~27% del volumen. Un order recien creado tambien cumple este filtro
   (su updated_at arranca igual al created_at), asi que no se pierde nada
   de lo que ya traia antes.
2. VENTANA_DIAS = 5: cubre el p99 (~2.8 dias) con margen. Quedan afuera a
   proposito los outliers de 4.8/8.6/22.8/24.9/32 dias detectados en el
   mismo analisis -- son 5 de 943 (0.5%), y agrandar la ventana para
   cubrirlos encarece muchisimo cada corrida para ganar casi nada. Revisar
   esas 5 ordenes puntuales en Magento antes de asumir que son el mismo
   fenomeno (podria ser una edicion no relacionada al estado de la orden,
   no una demora real de pago).
3. Se agrega `Fecha_Carga` (timestamp de esta corrida) a cada fila --:
   necesario para deduplicar en BigQuery (quedarse con la version mas
   reciente por Numero_Orden+SKU), porque con ventana rodante la MISMA
   orden va a aparecer en varias corridas mientras siga dentro de la
   ventana -- es esperado, no un bug, pero hay que resolverlo rio abajo.
4. `HOY` ahora es timezone-aware en America/Argentina/Buenos_Aires, no
   `datetime.now()` a secas -- si esto corre en un container (Cloud Run,
   Vercel), sin esto el "ahora" queda en UTC y desalinea la ventana contra
   el dia de negocio real.
5. PENDIENTE, no resuelto en esta version: el filtro `estatus="complete"`
   sigue trayendo solo ordenes que HOY estan completas. Una orden que
   estaba completa y se cancela/reembolsa despues deja de aparecer en
   corridas futuras -- la fila vieja en tu historico queda con el estado
   viejo para siempre. Esto no se soluciona con la ventana rodante; haria
   falta sacar el filtro de estado y decidir que cuenta como "venta
   valida" en una capa aparte (BigQuery), no en la extraccion.
6. Se agrega `escribir_a_gcs()`: sube df_ordenes a
   gs://GCS_BUCKET/raw/magento/ordenes/{fecha_ejecucion}.json como JSON
   Lines (formato directo para `bq load --source_format=NEWLINE_DELIMITED_JSON`).
   fecha_ejecucion es el dia de la corrida (no el rango desde/hasta de la
   ventana), para que una re-ejecucion el mismo dia pise el archivo en vez
   de acumular uno nuevo. Requiere GCS_PROJECT / GCS_BUCKET como variables
   de entorno (completar antes de correr). El .to_parquet() local se deja
   por ahora como respaldo/debug.
7. `escribir_a_gcs()` ahora vive en _common/gcs_utils.py, compartida con
   los demas extractores del repo (meta-ads, google-ads) en vez de estar
   copiada aca. Este archivo la importa agregando la raiz del repo a
   sys.path -- ver el bloque de imports.
8. NUEVO -- universo de SKUs para stock ya no es el catalogo completo de
   Magento (145.803 SKUs habilitados en una prueba, 730 paginas -- ver
   `obtener_catalogo_skus()`/`diagnosticar_catalogo()`, que quedan en el
   archivo para uso manual/auditoria pero ya no se llaman en el flujo
   principal). Se reemplaza por los SKUs "vistos" en GA4
   (`obtener_skus_vistos_ga4()`) en una ventana de GA4_MESES_VISTAS meses
   (default 6) -- pensado como insumo para la variable `stock_disponible`
   del MMM de World Sport (ver `world-sport-plan-trabajo-mmm.md`).
   Supuesto declarado y aceptado explicitamente (no verificado tecnicamente
   contra GTM/Magefan, porque Ciro ya no esta en el equipo para auditar el
   tag): el `item_id` que reporta GA4 es directamente compatible con el
   SKU de Magento (no se resuelve la duda de si Magefan tagea a nivel
   variante/simple o a nivel producto configurable/padre) -- si en algun
   momento se detectan muchos SKUs de `importar_stock()` que nunca
   matchean nada en GA4 (o viceversa), revisar este supuesto primero.
   `importar_stock()` no cambia: sigue devolviendo SIEMPRE una fila por
   SKU pedido, con Qty_Available=0 / En_Stock=False si Magento no
   devuelve nada para ese SKU.
9. REVERTIDO (2026-09) -- el cambio del punto 8 (universo de SKUs = vistos
   en GA4) queda sin usar en el flujo principal. Motivo: se confirmo con
   muestras reales de datos (no solo sospecha) que el `itemId` de GA4
   corresponde al SKU del producto CONFIGURABLE/padre, mientras que el
   stock real en Magento vive en los SKUs SIMPLE/hijos (variante
   talle-color) -- son universos de SKUs distintos, no mergeables sin
   antes resolver el mapeo padre->hijo (pendiente, no implementado aca;
   ver `configurable-products/{sku}/children` como candidato).
   `obtener_skus_vistos_ga4()` se deja definida por si se retoma ese
   mapeo mas adelante, pero ya NO se llama en `__main__`.
   En su lugar, el universo de SKUs para stock vuelve a ser el catalogo
   de Magento filtrado por `status=1` (habilitado) via
   `obtener_catalogo_skus(oauth, solo_habilitados=True)` -- ver el
   docstring de esa funcion y de `diagnosticar_catalogo()` para el
   detalle y las limitaciones de este filtro (no excluye productos
   `type_id=configurable`, que no cargan stock propio). Se agrega un
   flag `correr_diagnostico_catalogo` en `__main__` para correr el
   diagnostico barato de 7 llamadas antes de pagar el costo completo de
   traer y consultar stock para todo el catalogo habilitado.
10. RESUELTO (2026-09) -- el punto 9 volvia al catalogo completo porque el
    mergeo GA4 (padre) vs. stock (hijo) no se podia hacer directo. Ahora se
    resuelve con un paso intermedio: `resolver_hijos_configurable()`
    consulta `GET /V1/configurable-products/{sku}/children` en Magento
    para traer los SKUs hijo reales de cada SKU padre visto en GA4 (si un
    SKU visto no es configurable, se lo trata como su propio hijo -- ya es
    el nivel correcto). Como esto son ~13.355 SKUs padre (corrida de
    2026-09) y resolverlos todos de nuevo en cada corrida seria ~13.355
    llamadas extra a Magento solo para el mapeo, se cachea el resultado en
    GCS (`MAPEO_PADRE_HIJO_GCS_PATH`, path fijo, no fechado) via
    `actualizar_mapeo_padre_hijo()`: cada corrida solo resuelve contra
    Magento los SKUs padre NUEVOS que todavia no estan en el cache
    (altas de catalogo), no el universo completo.
    El universo de stock en `__main__` vuelve a ser
    `obtener_skus_vistos_ga4()` (no el catalogo completo del punto 9),
    resuelto a nivel hijo por `actualizar_mapeo_padre_hijo()` antes de
    llamar a `importar_stock()`.
    Supuesto sin verificar todavia: que el mapeo cacheado no queda
    desactualizado si Magento reasigna los hijos de un padre ya cacheado
    (ej. se agrega una variante de talle nueva a un producto existente)
    -- tal como esta, esto solo se detecta si el padre deja de estar
    cacheado (nunca, una vez que entro), no hay invalidacion por cambios
    en un padre ya resuelto. Si esto importa, hay que sumar una logica de
    refresco periodico completo (ej. mensual) ademas del incremental.
"""

import os
import sys
import time
import pandas as pd
import requests
from urllib.parse import quote
from dotenv import load_dotenv
from requests_oauthlib import OAuth1Session
import datetime
from zoneinfo import ZoneInfo
import json

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account

# Raiz del repo = dos niveles arriba de este archivo (magento/src/... -> repo root).
# Necesario para poder hacer `from _common.gcs_utils import escribir_a_gcs`
# tanto corriendo local (parado en la raiz del repo) como dentro del
# container (donde el Dockerfile copia _common/ y magento/src/ preservando
# esta misma estructura relativa -- ver magento/Dockerfile).
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from _common.gcs_utils import escribir_a_gcs, leer_jsonl_de_gcs, escribir_jsonl_a_gcs

load_dotenv()

MAGENTO_BASE_URL = os.getenv("MAGENTO_BASE_URL")  # sin barra al final, ej: https://www.worldsport.com.ar
CONSUMER_KEY = os.getenv("CONSUMER_KEY_MAGENTO")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET_MAGENTO")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN_MAGENTO")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET_MAGENTO")

# Supuesto: nombres de variables de entorno para el proyecto/bucket de GCS de
# World Sport. Completar en el .env / en las env vars del Cloud Run Job --
# no hardcodear el project id ni el nombre del bucket aca.
GCS_PROJECT = os.getenv("GCS_PROJECT")
GCS_BUCKET = os.getenv("GCS_BUCKET")

# GA4 (property unificada "World Sport - GA4", 5 streams, uno por marca --
# ver world-sport-incidente-ga4-agosto2026.md). GA4_PROPERTY_ID es el ID
# NUMERICO de la propiedad (no el Measurement ID G-XXXXXXX de ninguna marca
# puntual) -- completar en el .env / Cloud Run Job.
# Autenticacion: si GA4_CREDENTIALS_JSON apunta a un archivo de service
# account, se usa ese archivo explicitamente; si no esta seteada, se cae a
# Application Default Credentials (service account adjunta al Cloud Run
# Job, igual patron que ya se usa para BigQuery/GCS en LOOPI). En cualquier
# caso, esa cuenta de servicio necesita el rol de Viewer sobre la propiedad
# GA4 en Google Analytics Admin -- no alcanza con permisos de GCP.
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID",402391218)
GA4_CREDENTIALS_JSON = os.getenv("GA4_KEY_FILE")

# Ventana de "vistos" para definir el universo de SKUs a consultar stock --
# ver el punto 8 del docstring del archivo. Configurable por variable de
# entorno para poder probar otras ventanas sin tocar codigo.
GA4_MESES_VISTAS = int(os.getenv("GA4_MESES_VISTAS", "6"))

# Path (fijo, NO fechado) del mapeo padre->hijo cacheado en GCS -- ver el
# punto 10 del docstring del archivo y `actualizar_mapeo_padre_hijo()`.
# A diferencia de raw/{fuente}/{entidad}/{fecha}.json, este archivo se lee
# y se SOBREESCRIBE en el mismo path en cada corrida (via
# leer_jsonl_de_gcs/escribir_jsonl_a_gcs de _common/gcs_utils.py), porque
# es un cache que se actualiza incrementalmente, no un snapshot historico.
MAPEO_PADRE_HIJO_GCS_PATH = os.getenv(
    "MAPEO_PADRE_HIJO_GCS_PATH", "cache/magento/mapeo_padre_hijo.json"
)

PAGE_SIZE = 50
TIMEZONE_NEGOCIO = ZoneInfo("America/Argentina/Buenos_Aires")


def crear_sesion_oauth():
    """Crea la sesion autenticada con OAuth 1.0a para reutilizar en todas las llamadas."""
    faltantes = [
        nombre for nombre, valor in [
            ("MAGENTO_BASE_URL", MAGENTO_BASE_URL),
            ("CONSUMER_KEY_MAGENTO", CONSUMER_KEY),
            ("CONSUMER_SECRET_MAGENTO", CONSUMER_SECRET),
            ("ACCESS_TOKEN_MAGENTO", ACCESS_TOKEN),
            ("ACCESS_TOKEN_SECRET_MAGENTO", ACCESS_TOKEN_SECRET),
        ] if not valor
    ]
    if faltantes:
        raise EnvironmentError(
            "Faltan variables en el .env: " + ", ".join(faltantes)
        )

    return OAuth1Session(
        client_key=CONSUMER_KEY,
        client_secret=CONSUMER_SECRET,
        resource_owner_key=ACCESS_TOKEN,
        resource_owner_secret=ACCESS_TOKEN_SECRET,
        signature_method="HMAC-SHA256",
    )

def construir_claves_checkout_pro(n_pagos):
    claves = ["method_title", "date_of_expiration", "init_point", "id"]
    for i in range(n_pagos):
        claves += [
            f"payment_{i}_id", f"payment_{i}_type", f"payment_{i}_total_amount",
            f"payment_{i}_paid_amount", f"payment_{i}_refunded_amount",
            f"payment_{i}_card_number", f"payment_{i}_installments",
            f"mp_{i}_status", f"mp_{i}_status_detail", f"payment_{i}_expiration",
        ]
    claves += ["payment_index_list", "mp_status", "mp_status_detail"]
    return claves

def parsear_checkout_pro(payment):
    info = payment.get("additional_information", [])
    if not info:
        return {}
    n_pagos = round((len(info) - 7) / 10)
    claves_esperadas = construir_claves_checkout_pro(max(n_pagos, 1))
    if len(claves_esperadas) != len(info):
        return {}
    return dict(zip(claves_esperadas, info))

def extraer_detalle_pago(payment):
    """Normaliza el detalle de pago sea cual sea el metodo.
    Devuelve siempre las mismas claves; las que no aplican quedan en None."""
    metodo = payment.get("method", "")
    detalle = {
        "cuotas": None,
        "tipo_pago": None,
        "estado_pago": None,
        "id_transaccion": None,
    }

    if metodo in ("mercadopago_adbpayment_checkout_pro", "mercadopago_adbpayment_checkout_credits"):
        info = parsear_checkout_pro(payment)
        if info:
            detalle["cuotas"] = info.get("payment_0_installments")
            detalle["tipo_pago"] = info.get("payment_0_type")
            detalle["estado_pago"] = info.get("mp_status")
            detalle["id_transaccion"] = info.get("payment_0_id")

    elif metodo == "gocuotas":
        # GOcuotas no expone cuotas/estado especifico por orden, solo un texto fijo
        detalle["tipo_pago"] = "debito_sin_tarjeta"

    elif metodo == "talopay_transfer":
        info_lista = payment.get("additional_information", [])
        try:
            detalle_talopay = json.loads(info_lista[0]) if info_lista else {}
            detalle["estado_pago"] = "approved" if detalle_talopay.get("alreadyPaid") else detalle_talopay.get("payment_status")
            detalle["id_transaccion"] = detalle_talopay.get("id")
            detalle["tipo_pago"] = "transferencia_bancaria"
        except (json.JSONDecodeError, IndexError, TypeError):
            pass

    return detalle

def probar_conexion(oauth):
    """Prueba rapida: 1 orden, solo para confirmar que la autenticacion funciona."""
    url = f"{MAGENTO_BASE_URL}/rest/V1/orders?searchCriteria[pageSize]=1"
    resp = oauth.get(url)
    print(f"Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"Conexion OK. Total de ordenes en la tienda: {data.get('total_count')}")
    else:
        print(f"Error: {resp.text[:500]}")
    return resp.status_code == 200


def magento_get(oauth, url, intento=1, max_intentos=5):
    resp = oauth.get(url)
    code = resp.status_code

    if code == 200:
        return resp.json()

    if code in (429, 503) and intento <= max_intentos:
        retry_after = resp.headers.get("Retry-After")
        espera = int(retry_after) if retry_after else 2 ** intento
        print(f"  Rate limit ({code}). Esperando {espera}s (intento {intento}/{max_intentos})...")
        time.sleep(espera)
        return magento_get(oauth, url, intento + 1, max_intentos)

    raise Exception(f"Magento respondio {code}: {resp.text[:300]}")

# Cache para no repetir llamadas a la API por el mismo cliente
_customer_cache = {}

def obtener_edad_cliente(oauth, customer_id):
    """Edad del cliente a partir de su fecha de nacimiento (dob).
    Cachea por customer_id -- se consulta una sola vez por cliente."""
    if not customer_id:
        return None
    if customer_id in _customer_cache:
        return _customer_cache[customer_id]

    edad = None
    try:
        url = f"{MAGENTO_BASE_URL}/rest/V1/customers/{customer_id}"
        data = magento_get(oauth, url)
        dob = data.get("dob")  # formato esperado: "YYYY-MM-DD"
        if dob:
            # Bug corregido: era datetime.strptime/datetime.now (el modulo,
            # no la clase) -- con "import datetime" a secas esto rompia.
            nacimiento = datetime.datetime.strptime(dob[:10], "%Y-%m-%d")
            edad = (datetime.datetime.now() - nacimiento).days // 365
    except Exception as e:
        print(f"[AVISO] No se pudo obtener edad del customer_id {customer_id}: {e}")

    _customer_cache[customer_id] = edad
    return edad

def importar_ordenes(oauth, desde, hasta, estatus="complete", page_size=PAGE_SIZE):
    """
    Trae ordenes de Magento a nivel de linea de item, con updated_at entre
    'desde' y 'hasta'. Formato esperado de fechas: 'YYYY-MM-DD HH:MM:SS'

    Por que updated_at y no created_at: con ventana rodante, updated_at es
    lo que permite reconsultar ordenes viejas que recien ahora terminaron
    de asentarse (ver el analisis de tiempos creacion->actualizacion en la
    conversacion). Una orden recien creada tambien matchea este filtro
    (su updated_at arranca en el mismo valor que su created_at).

    estatus: valor interno de Magento a filtrar (ej. 'complete').
             Pasar None para traer todos los estatus sin filtrar.
             OJO: con este filtro, una orden que estaba "complete" y pasa a
             "canceled"/"refunded" simplemente deja de aparecer en corridas
             futuras -- no se borra ni se marca en tu historico, tu fila
             vieja queda desactualizada. Pendiente de resolver aparte.
    """
    filas = []
    pagina = 1
    total_paginas = 1
    fecha_carga = datetime.datetime.now(TIMEZONE_NEGOCIO).isoformat()

    desde_encoded = quote(desde)
    hasta_encoded = quote(hasta)

    while pagina <= total_paginas:
        url = (
            f"{MAGENTO_BASE_URL}/rest/V1/orders"
            f"?searchCriteria[filterGroups][0][filters][0][field]=updated_at"
            f"&searchCriteria[filterGroups][0][filters][0][value]={desde_encoded}"
            f"&searchCriteria[filterGroups][0][filters][0][conditionType]=gteq"
            f"&searchCriteria[filterGroups][1][filters][0][field]=updated_at"
            f"&searchCriteria[filterGroups][1][filters][0][value]={hasta_encoded}"
            f"&searchCriteria[filterGroups][1][filters][0][conditionType]=lteq"
        )

        if estatus:
            url += (
                f"&searchCriteria[filterGroups][2][filters][0][field]=status"
                f"&searchCriteria[filterGroups][2][filters][0][value]={quote(estatus)}"
                f"&searchCriteria[filterGroups][2][filters][0][conditionType]=eq"
            )

        url += (
            f"&searchCriteria[pageSize]={page_size}"
            f"&searchCriteria[currentPage]={pagina}"
        )

        data = magento_get(oauth, url)

        if pagina == 1:
            total_count = data.get("total_count", 0)
            total_paginas = max(1, -(-total_count // page_size))
            print(f"Total de ordenes a traer: {total_count} ({total_paginas} paginas)")

        for orden in data.get("items", []):
            items = orden.get("items", [])
            hijos_por_padre = {
                it["item_id"]: it for it in items if it.get("parent_item_id")
            }

            for item in items:
                if item.get("parent_item_id"):
                    continue

                hijo = hijos_por_padre.get(item["item_id"])
                sku_variante = hijo["sku"] if hijo else item["sku"]

                payment = orden.get("payment", {}) or {}
                metodo_pago = payment.get("method", "")

                billing = orden.get("billing_address", {}) or {}
                direccion = ", ".join(billing.get("street", [])) if billing.get("street") else ""
                localidad = billing.get("city", "")
                provincia = billing.get("region", "")
                # edad_cliente = obtener_edad_cliente(oauth, orden.get("customer_id"))

                detalle_pago = extraer_detalle_pago(payment)

                filas.append({
                    "Numero_Orden": orden["increment_id"],
                    "Fecha_de_compra": orden["created_at"],
                    "Fecha_Actualizacion": orden.get("updated_at"),
                    "Fecha_Carga": fecha_carga,  # nueva -- para deduplicar en BigQuery
                    "Estatus": orden["status"],
                    "Store_ID": orden.get("store_id"),
                    "Punto_de_compra": orden.get("store_name", "").replace("\n", " / "),
                    "Correo_electronico_del_cliente": orden.get("customer_email", ""),
                    "SKU": sku_variante,
                    "Nombre_Producto": item["name"],
                    "Cantidad": item["qty_ordered"],
                    "Precio": item["price"],
                    "Descuento": item.get("discount_amount", 0),
                    "Total_Linea": item["row_total"],
                    "Total_pagado": orden.get("total_paid", orden["grand_total"]),
                    "Moneda": orden.get("order_currency_code", ""),
                    "Metodo_de_Pago": metodo_pago,
                    "Direccion": direccion,
                    "Localidad": localidad,
                    "Provincia": provincia,
                    "Cupon": orden.get("coupon_code"),
                    "Descripcion_Descuento": orden.get("discount_description"),
                    "Reglas_Aplicadas": orden.get("applied_rule_ids"),
                    "Cuotas": detalle_pago["cuotas"],
                    "Tipo_de_Pago": detalle_pago["tipo_pago"],
                    "Estado_de_Pago": detalle_pago["estado_pago"],
                    "ID_Transaccion": detalle_pago["id_transaccion"],
                    # "Edad": edad_cliente,
                })

        print(f"Pagina {pagina}/{total_paginas} procesada - {len(filas)} lineas acumuladas")
        pagina += 1

    return pd.DataFrame(filas)


def _cliente_ga4():
    """
    Crea el cliente de la GA4 Data API. Ver comentario junto a
    GA4_CREDENTIALS_JSON/GA4_PROPERTY_ID (arriba en el archivo) para el
    detalle de que credencial se usa y por que.
    """
    if not GA4_PROPERTY_ID:
        raise EnvironmentError(
            "Falta GA4_PROPERTY_ID en el .env / variables de entorno."
        )

    if GA4_CREDENTIALS_JSON:
        credenciales = service_account.Credentials.from_service_account_file(
            GA4_CREDENTIALS_JSON
        )
        return BetaAnalyticsDataClient(credentials=credenciales)

    # Sin GA4_CREDENTIALS_JSON: Application Default Credentials (service
    # account adjunta al Cloud Run Job, o `gcloud auth application-default
    # login` en local).
    return BetaAnalyticsDataClient()


def obtener_skus_vistos_ga4(meses=GA4_MESES_VISTAS):
    """
    Trae la lista de SKUs "vistos" (evento view_item de GA4) en los ultimos
    `meses` meses, consultando la propiedad GA4 unificada de World Sport
    (5 marcas en la misma propiedad -- no se desagrega por marca aca, ver
    supuesto abajo).

    Supuesto CENTRAL, declarado y aceptado explicitamente (2026-09, con
    Ciro ya fuera del equipo para auditar el tag de Magefan/GTM que arma el
    objeto ecommerce): la dimension `itemId` que devuelve GA4 es
    directamente el SKU de Magento. No esta confirmado si Magefan tagea
    `item_id` a nivel de la variante/SKU simple (talle+color) o a nivel del
    producto configurable/padre -- si en algun momento la tasa de match
    contra `importar_stock()` sale sospechosamente baja, este es el primer
    lugar para revisar.

    No separa por marca: la propiedad es unica para las 5 marcas de World
    Sport, asi que un mismo SKU vendido bajo mas de una marca (si existiera)
    quedaria mezclado. Para el uso actual (definir el universo de SKUs a
    consultar stock) esto no es un problema -- si mas adelante se necesita
    `stock_disponible` desagregado por marca, hay que sumar la dimension
    de marca/stream a esta consulta (pendiente, no resuelto aca).

    Devuelve una lista de SKUs unicos (str). Puede devolver lista vacia si
    la propiedad no tiene datos de view_item en la ventana pedida -- no se
    trata como error, es responsabilidad de quien llama decidir que hacer
    con una lista vacia.
    """
    cliente = _cliente_ga4()

    hoy = datetime.datetime.now(TIMEZONE_NEGOCIO)
    desde = (hoy - datetime.timedelta(days=meses * 30)).strftime("%Y-%m-%d")
    hasta = hoy.strftime("%Y-%m-%d")

    print(f"\nConsultando GA4 (property {GA4_PROPERTY_ID}): SKUs vistos entre {desde} y {hasta}...")

    skus = []
    tamanio_pagina = 100_000  # maximo permitido por la GA4 Data API
    offset = 0
    total_filas = None

    while total_filas is None or offset < total_filas:
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            date_ranges=[DateRange(start_date=desde, end_date=hasta)],
            dimensions=[Dimension(name="itemId")],
            metrics=[Metric(name="itemsViewed")],
            limit=tamanio_pagina,
            offset=offset,
        )
        response = cliente.run_report(request)

        if total_filas is None:
            total_filas = response.row_count
            print(f"  GA4 reporta {total_filas} SKUs distintos con al menos 1 vista en la ventana.")

        for fila in response.rows:
            sku = fila.dimension_values[0].value
            if sku:
                skus.append(sku)

        offset += tamanio_pagina
        print(f"  GA4 SKUs vistos: {min(offset, total_filas or 0)}/{total_filas or 0} acumulados")

    skus_unicos = sorted(set(skus))
    print(f"Total SKUs vistos en GA4 (unicos, ultimos {meses} meses): {len(skus_unicos)}")
    return skus_unicos


def resolver_hijos_configurable(oauth, sku_padre, intento=1, max_intentos=5):
    """
    Devuelve la lista de SKUs hijo (simple, variante talle/color -- donde
    vive el stock real) de un producto configurable, consultando
    GET /V1/configurable-products/{sku}/children.

    Si `sku_padre` NO es un producto configurable, Magento responde con
    error (400/404) para este endpoint -- en ese caso se devuelve None, que
    la persona que llama debe interpretar como "este SKU YA es el nivel
    correcto" (ej. un accesorio simple sin variantes), no como una falla.

    Igual que `magento_get()`, reintenta con backoff ante 429/503; a
    diferencia de esa funcion, NO levanta excepcion ante 400/404 porque
    ese codigo es un resultado esperado (no-configurable), no un error.
    """
    url = f"{MAGENTO_BASE_URL}/rest/V1/configurable-products/{quote(sku_padre, safe='')}/children"
    resp = oauth.get(url)
    code = resp.status_code

    if code == 200:
        hijos = resp.json()
        return [h["sku"] for h in hijos] if hijos else None

    if code in (400, 404):
        return None

    if code in (429, 503) and intento <= max_intentos:
        retry_after = resp.headers.get("Retry-After")
        espera = int(retry_after) if retry_after else 2 ** intento
        print(f"  Rate limit ({code}) resolviendo hijos de {sku_padre}. Esperando {espera}s (intento {intento}/{max_intentos})...")
        time.sleep(espera)
        return resolver_hijos_configurable(oauth, sku_padre, intento + 1, max_intentos)

    raise Exception(f"Magento respondio {code} resolviendo hijos de {sku_padre}: {resp.text[:300]}")


def construir_mapeo_padre_hijo(oauth, skus_padre):
    """
    Resuelve, para cada SKU en `skus_padre`, su lista de SKUs hijo reales
    via `resolver_hijos_configurable()`. Si un SKU no es configurable, se
    mapea a si mismo (lista de un elemento).

    Pensada para llamarse SOLO con los SKUs padre que todavia NO estan en
    el mapeo cacheado (ver `actualizar_mapeo_padre_hijo()`) -- resolver el
    universo completo de nuevo en cada corrida es justo lo que el cache
    busca evitar (a ~13.355 SKUs padre vistos en la corrida de 2026-09,
    son ~13.355 llamadas a Magento solo para esto).

    Devuelve un dict {sku_padre: [sku_hijo, ...]}.
    """
    mapeo = {}
    for i, sku_padre in enumerate(skus_padre, start=1):
        hijos = resolver_hijos_configurable(oauth, sku_padre)
        mapeo[sku_padre] = hijos if hijos else [sku_padre]

        if i % 200 == 0:
            print(f"  Mapeo padre->hijo: {i}/{len(skus_padre)} SKUs padre nuevos resueltos")

    return mapeo


def cargar_mapeo_cacheado(gcs_project=None, gcs_bucket=None, path=None):
    """
    Lee el mapeo padre->hijo cacheado en GCS (path fijo, ver
    MAPEO_PADRE_HIJO_GCS_PATH). Devuelve un dict {sku_padre: [sku_hijo,
    ...]} -- vacio si el archivo todavia no existe (primera corrida, nada
    cacheado todavia).
    """
    path = path or MAPEO_PADRE_HIJO_GCS_PATH
    filas = leer_jsonl_de_gcs(path, gcs_project=gcs_project, gcs_bucket=gcs_bucket)

    mapeo = {}
    for fila in filas:
        mapeo.setdefault(fila["sku_padre"], []).append(fila["sku_hijo"])
    return mapeo


def guardar_mapeo_cacheado(mapeo, gcs_project=None, gcs_bucket=None, path=None):
    """
    Sobreescribe en GCS el mapeo padre->hijo cacheado con el dict COMPLETO
    recibido (no incremental a nivel archivo -- el merge incremental ya
    paso antes, en memoria, dentro de `actualizar_mapeo_padre_hijo()`; aca
    solo se aplana el dict a filas y se pisa el archivo entero).
    """
    path = path or MAPEO_PADRE_HIJO_GCS_PATH
    filas = [
        {"sku_padre": padre, "sku_hijo": hijo}
        for padre, hijos in mapeo.items()
        for hijo in hijos
    ]
    return escribir_jsonl_a_gcs(filas, path, gcs_project=gcs_project, gcs_bucket=gcs_bucket)


def actualizar_mapeo_padre_hijo(oauth, skus_padre_vistos, gcs_project=None, gcs_bucket=None, path=None):
    """
    Resuelve la lista de SKUs HIJO reales (donde vive el stock) para el
    universo de SKUs padre vistos en GA4, usando y actualizando el cache
    de GCS -- solo se resuelven contra Magento los SKUs padre que TODAVIA
    NO estan en el cache (altas nuevas de catalogo desde la ultima
    corrida), no el universo completo cada vez.

    Limitacion declarada, no resuelta aca (ver punto 10 del docstring del
    archivo): si un padre YA cacheado cambia sus hijos en Magento (ej. se
    agrega una variante de talle nueva), este mapeo no lo detecta -- solo
    se resuelven padres nuevos, nunca se re-resuelve uno ya cacheado. Si
    esto importa, hace falta sumar un refresco periodico completo aparte
    (ej. mensual), no solo este incremental.

    Devuelve la lista de SKUs hijo (unicos, ordenados) a consultar en
    `importar_stock()`.
    """
    mapeo = cargar_mapeo_cacheado(gcs_project=gcs_project, gcs_bucket=gcs_bucket, path=path)
    print(f"Mapeo padre->hijo cacheado: {len(mapeo)} SKUs padre ya resueltos previamente.")

    padres_nuevos = [p for p in skus_padre_vistos if p not in mapeo]

    if padres_nuevos:
        print(f"Resolviendo {len(padres_nuevos)} SKUs padre nuevos (no estaban en el cache) contra Magento...")
        mapeo_nuevo = construir_mapeo_padre_hijo(oauth, padres_nuevos)
        mapeo.update(mapeo_nuevo)
        guardar_mapeo_cacheado(mapeo, gcs_project=gcs_project, gcs_bucket=gcs_bucket, path=path)
        print(f"Cache actualizado y guardado en GCS: {len(mapeo)} SKUs padre en total.")
    else:
        print("Todos los SKUs padre vistos ya estaban en el cache -- no se llamo a Magento para resolver el mapeo.")

    skus_hijos = sorted({
        hijo
        for padre in skus_padre_vistos
        for hijo in mapeo.get(padre, [padre])
    })
    return skus_hijos


def diagnosticar_catalogo(oauth):
    """
    Chequeo barato (7 llamadas con searchCriteria[pageSize]=1 -- Magento
    igual devuelve el total_count real del filtro aplicado, sin traer los
    items) para caracterizar el catalogo ANTES de pagar el costo completo
    de `obtener_catalogo_skus()` + `importar_stock()` (~292 paginas + ~3650
    lotes de stock con page_size=500).

    Motivacion: una corrida de prueba (2026-09) dio 145.803 SKUs
    habilitados (status=1), un numero que a primera vista parece demasiado
    alto contra los ~802 SKUs unicos vendidos en una ventana de 5 dias.
    Esto no prueba nada por si solo (un catalogo de indumentaria con
    variantes de talle/color y varias marcas en el mismo Magento puede
    tener una cola larga real), pero antes de asumir que el numero esta
    bien -- o que hay un bug en el filtro -- conviene descomponerlo.

    Como leer el resultado:
    - Si "habilitados" y "total sin filtrar status" son casi iguales, el
      filtro status=1 no esta excluyendo casi nada -- puede ser normal
      (poco se da de baja) o puede ser que Magento este ignorando el
      filtro (bug de configuracion del lado del hosting/API).
    - "habilitados + type_id=simple" + "habilitados + type_id=configurable"
      (+ otros type_id que existan) deberian sumar aprox el total de
      "habilitados" -- si no suman ni cerca, hay un tercer type_id grande
      sin explicar (bundle, virtual, downloadable) que vale la pena mirar.
    - "habilitados sin tocar hace 2+ anios" alto sugiere catalogo legacy
      que nunca se deshabilita cuando un producto deja de venderse -- son
      SKUs reales, no un bug, pero explican por que el ratio contra
      ventas recientes es tan alto.

    No aborta ni decide nada solo -- imprime los numeros para que una
    persona confirme si el volumen tiene sentido antes de habilitar
    extraer_stock=True en una corrida real. Devuelve el dict ademas de
    imprimirlo, por si se quiere loguear estructurado mas adelante.
    """
    def _total_count(filtro_extra=""):
        url = (
            f"{MAGENTO_BASE_URL}/rest/V1/products?"
            f"{filtro_extra}"
            f"searchCriteria[pageSize]=1&searchCriteria[currentPage]=1"
        )
        return magento_get(oauth, url).get("total_count", 0)

    hace_2_anios = (
        datetime.datetime.now(TIMEZONE_NEGOCIO) - datetime.timedelta(days=730)
    ).strftime("%Y-%m-%d 00:00:00")

    conteos = {
        "habilitados (status=1)": _total_count(
            "searchCriteria[filterGroups][0][filters][0][field]=status"
            "&searchCriteria[filterGroups][0][filters][0][value]=1"
            "&searchCriteria[filterGroups][0][filters][0][conditionType]=eq&"
        ),
        "deshabilitados (status=2)": _total_count(
            "searchCriteria[filterGroups][0][filters][0][field]=status"
            "&searchCriteria[filterGroups][0][filters][0][value]=2"
            "&searchCriteria[filterGroups][0][filters][0][conditionType]=eq&"
        ),
        "total sin filtrar status": _total_count(""),
        "habilitados + type_id=simple": _total_count(
            "searchCriteria[filterGroups][0][filters][0][field]=status"
            "&searchCriteria[filterGroups][0][filters][0][value]=1"
            "&searchCriteria[filterGroups][0][filters][0][conditionType]=eq"
            "&searchCriteria[filterGroups][1][filters][0][field]=type_id"
            "&searchCriteria[filterGroups][1][filters][0][value]=simple"
            "&searchCriteria[filterGroups][1][filters][0][conditionType]=eq&"
        ),
        "habilitados + type_id=configurable": _total_count(
            "searchCriteria[filterGroups][0][filters][0][field]=status"
            "&searchCriteria[filterGroups][0][filters][0][value]=1"
            "&searchCriteria[filterGroups][0][filters][0][conditionType]=eq"
            "&searchCriteria[filterGroups][1][filters][0][field]=type_id"
            "&searchCriteria[filterGroups][1][filters][0][value]=configurable"
            "&searchCriteria[filterGroups][1][filters][0][conditionType]=eq&"
        ),
        "habilitados sin tocar hace 2+ anios (updated_at)": _total_count(
            "searchCriteria[filterGroups][0][filters][0][field]=status"
            "&searchCriteria[filterGroups][0][filters][0][value]=1"
            "&searchCriteria[filterGroups][0][filters][0][conditionType]=eq"
            "&searchCriteria[filterGroups][1][filters][0][field]=updated_at"
            f"&searchCriteria[filterGroups][1][filters][0][value]={quote(hace_2_anios)}"
            "&searchCriteria[filterGroups][1][filters][0][conditionType]=lteq&"
        ),
    }

    print("\n=== Diagnostico de catalogo (7 llamadas livianas, antes de traer todo) ===")
    for etiqueta, valor in conteos.items():
        print(f"  {etiqueta}: {valor}")
    print("==========================================================================\n")

    return conteos


def obtener_catalogo_skus(oauth, page_size=500, solo_habilitados=True):
    """
    Trae el listado completo de SKUs del catalogo (no solo los que aparecieron
    en ordenes recientes), consultando /rest/V1/products paginado.

    Supuesto: solo_habilitados=True filtra por status=1 (habilitado en
    Magento) -- no se incluyen productos deshabilitados/dados de baja, porque
    no tiene sentido reportar "sin stock" algo que ni siquiera esta activo
    para la venta. Si en realidad se necesita el catalogo completo sin
    filtrar, llamar con solo_habilitados=False.

    Nota sobre el volumen esperado: en una corrida de prueba (2026-09) este
    catalogo devolvio ~145.800 SKUs habilitados -- es alto pero plausible
    para un retailer multi-marca en Magento, porque cada combinacion
    talle/color se cuenta como un producto "simple" separado del
    "configurable" padre. Si en una corrida futura este numero cambia de
    orden de magnitud sin que haya un motivo de negocio (alta/baja masiva de
    productos), sospechar del filtro status=1 antes de asumir que esta bien.

    page_size subido de 200 a 500 (de 730 a ~292 paginas para ese volumen)
    para achicar el tiempo total de la corrida completa -- si el hosting de
    Magento lo permite, se puede probar subirlo mas (ej. 1000); si devuelve
    error o timeouts por pagina, bajarlo de nuevo.
    """
    skus = []
    pagina = 1
    total_paginas = 1

    while pagina <= total_paginas:
        url = f"{MAGENTO_BASE_URL}/rest/V1/products?"

        if solo_habilitados:
            url += (
                f"searchCriteria[filterGroups][0][filters][0][field]=status"
                f"&searchCriteria[filterGroups][0][filters][0][value]=1"
                f"&searchCriteria[filterGroups][0][filters][0][conditionType]=eq"
                f"&"
            )

        url += (
            f"searchCriteria[pageSize]={page_size}"
            f"&searchCriteria[currentPage]={pagina}"
        )

        data = magento_get(oauth, url)

        if pagina == 1:
            total_count = data.get("total_count", 0)
            total_paginas = max(1, -(-total_count // page_size))
            print(f"Total de productos en catalogo: {total_count} ({total_paginas} paginas)")

        for producto in data.get("items", []):
            skus.append(producto["sku"])

        print(f"Catalogo pagina {pagina}/{total_paginas} - {len(skus)} SKUs acumulados")
        pagina += 1

    return skus


def importar_stock(oauth, skus, skus_por_lote=40):
    """
    Consulta el stock MSI (multi-fuente) para una lista de SKUs.
    Devuelve un DataFrame con SKU, cantidad total y si esta en stock --
    con UNA fila por cada SKU de la lista recibida, siempre.

    Nota: este stock refleja el inventario ACTUAL (al momento de correr
    el script), no el stock historico en cada fecha de venta pasada.
    """
    acumulado = {}  # sku -> {"qty": total, "en_stock": bool}

    for i in range(0, len(skus), skus_por_lote):
        lote = skus[i:i + skus_por_lote]
        skus_str = ",".join(lote)

        url = (
            f"{MAGENTO_BASE_URL}/rest/V1/inventory/source-items"
            f"?searchCriteria[filterGroups][0][filters][0][field]=sku"
            f"&searchCriteria[filterGroups][0][filters][0][conditionType]=in"
            f"&searchCriteria[filterGroups][0][filters][0][value]={quote(skus_str)}"
            f"&searchCriteria[pageSize]={skus_por_lote * 5}"
        )

        data = magento_get(oauth, url)

        for it in data.get("items", []):
            sku = it["sku"]
            if sku not in acumulado:
                acumulado[sku] = {"qty": 0, "en_stock": False}
            acumulado[sku]["qty"] += float(it.get("quantity", 0) or 0)
            if it.get("status") == 1:
                acumulado[sku]["en_stock"] = True

        print(f"  Lote {i // skus_por_lote + 1}: {len(lote)} SKUs consultados")

    # Corregido: si un SKU pedido no aparece en NINGUN source-item (Magento
    # no devuelve nada para el), antes quedaba afuera del resultado en
    # silencio -- ahora se agrega explicitamente como sin stock (0, False),
    # para poder responder "cuales SKUs no estan en stock" sobre el
    # catalogo completo, no solo sobre lo que Magento eligio devolver.
    for sku in skus:
        if sku not in acumulado:
            acumulado[sku] = {"qty": 0, "en_stock": False}

    filas = [
        {"SKU": sku, "Qty_Available": v["qty"], "En_Stock": v["en_stock"]}
        for sku, v in acumulado.items()
    ]
    return pd.DataFrame(filas)


def probar_endpoint_producto(oauth, sku):
    """
    Prueba puntual: consulta /rest/V1/products/{sku} para un SKU especifico.
    Sirve para confirmar si el permiso 'Products' (distinto al de 'Categories')
    ya esta habilitado, y para ver si el producto trae algun atributo
    de categoria en 'custom_attributes' (ej. category_ids) sin necesidad
    del recurso Magento_Catalog::categories.
    """
    url = f"{MAGENTO_BASE_URL}/rest/V1/products/{quote(sku, safe='')}"
    resp = oauth.get(url)

    print(f"SKU: {sku} -> Status: {resp.status_code}")

    if resp.status_code != 200:
        print(f"  Error: {resp.text[:300]}")
        return None

    data = resp.json()
    custom_attrs = data.get("custom_attributes", [])

    print("  custom_attributes disponibles:")
    for attr in custom_attrs:
        print(f"    {attr.get('attribute_code')}: {attr.get('value')}")

    return data


def obtener_opciones_atributo(oauth, attribute_code):
    """
    Trae el diccionario id -> label de un atributo tipo dropdown de Magento
    (ej. 'size', 'color'). Se pide UNA SOLA VEZ por atributo -- no por
    producto -- porque el set de opciones es compartido por todo el catalogo.
    """
    url = f"{MAGENTO_BASE_URL}/rest/V1/products/attributes/{attribute_code}/options"
    opciones = magento_get(oauth, url)
    return {opt["value"]: opt["label"] for opt in opciones if opt.get("value")}


def importar_talle_color_genero(
    oauth, skus, talla_por_id, color_por_id  # ajustar al attribute_code real que encontraste
):
    """
    Trae Talle, Color y Genero para una lista de SKUs unicos, consultando
    /V1/products/{sku} una sola vez por SKU (no por linea de venta).
    Decodifica los ids de 'size'/'color'/<codigo_atributo_genero> contra los
    diccionarios id->label obtenidos con obtener_opciones_atributo.

    codigo_atributo_genero: el attribute_code real puede variar segun el catalogo
    (ej. "gender", "target_gender", "sexo") -- confirmalo antes de correr esto
    sobre todos los SKUs.

    Nota: no todos los SKUs tienen estos atributos cargados (ej. accesorios
    sin talle/genero) -- en esos casos queda en None, no es un error.
    """
    filas = []
    for i, sku in enumerate(skus, start=1):
        try:
            url = f"{MAGENTO_BASE_URL}/rest/V1/products/{quote(sku, safe='')}"
            producto = magento_get(oauth, url)
            atributos = {
                a["attribute_code"]: a["value"]
                for a in producto.get("custom_attributes", [])
            }

            talla_id = atributos.get("size")
            color_id = atributos.get("color")


            filas.append({
                "SKU": sku,
                "Talle": talla_por_id.get(talla_id),
                "Color": color_por_id.get(color_id),
            })
        except Exception as e:
            print(f"[AVISO] No se pudo obtener talle/color/genero del SKU {sku}: {e}")
            filas.append({"SKU": sku, "Talle": None, "Color": None, "Genero": None})

        if i % 50 == 0:
            print(f"  Atributos: {i}/{len(skus)} SKUs procesados")

    return pd.DataFrame(filas)


def extraer_categoria_edad_texto(nombre_producto):
    """
    Deteccion aproximada de la categoria 'Nino/Nina' basada en el texto
    del nombre del producto. Sirve como fallback cuando no hay permisos
    para consultar el endpoint real de categorias de Magento
    (Magento_Catalog::categories).

    Busca las palabras: niño, niños, niña, niñas, kid, kids
    (case-insensitive). "niñ" cubre las 4 variantes en español;
    "kid" cubre kid/kids en ingles.

    Limitacion conocida: solo detecta productos donde alguna de estas
    palabras aparece explicita en el nombre -- productos de esa
    categoria sin esa palabra (identificados solo por talle numerico,
    por ejemplo) no se van a marcar correctamente.
    """
    if pd.isna(nombre_producto):
        return False

    nombre_lower = nombre_producto.lower()
    palabras_clave = ["niñ", "kid"]

    return any(palabra in nombre_lower for palabra in palabras_clave)


if __name__ == "__main__":
    oauth = crear_sesion_oauth()

    extraer_ordenes = True
    extraer_stock = True
    extraer_atributos_producto = True
    # Corre el chequeo barato (7 llamadas pageSize=1) de diagnosticar_catalogo()
    # antes de traer stock -- ver el punto 9 del docstring del archivo. Sirve
    # para confirmar que el filtro status=1 esta excluyendo lo que se espera
    # (y no ignorandolo por algo del hosting) antes de pagar el costo de
    # traer y consultar stock para todo el catalogo habilitado.
    correr_diagnostico_catalogo = True

    if not probar_conexion(oauth):
        raise SystemExit("La conexion de prueba fallo. Revisa las credenciales antes de importar todo.")

    if correr_diagnostico_catalogo:
        diagnosticar_catalogo(oauth)

    # Ventana rodante, timezone-aware (America/Argentina/Buenos_Aires) --
    # ver el docstring del archivo para el porque de VENTANA_DIAS=5.
    # (Se elimino el override hardcodeado de "desde"/"hasta" que habia
    # quedado de una prueba puntual -- pisaba silenciosamente el calculo
    # dinamico de arriba en cada corrida.)
    HOY = datetime.datetime.now(TIMEZONE_NEGOCIO)
    VENTANA_DIAS = 5

    desde = (HOY - datetime.timedelta(days=VENTANA_DIAS)).strftime("%Y-%m-%d 00:00:00")
    hasta = HOY.strftime("%Y-%m-%d 23:59:59")

    desde_month = desde[:10]
    hasta_month = hasta[:10]

    # Fecha de esta corrida (no el rango desde/hasta de la ventana) -- es
    # la que se usa para nombrar el archivo en el bucket, ver escribir_a_gcs.
    fecha_ejecucion = HOY.strftime("%Y-%m-%d")

    if extraer_ordenes:
        df_ordenes = importar_ordenes(oauth, desde, hasta, estatus="complete")
        print(f"\nTotal de lineas importadas (solo 'complete'): {len(df_ordenes)}")

    skus_unicos = df_ordenes["SKU"].dropna().unique().tolist()

    os.makedirs("data/raw", exist_ok=True)

    if extraer_stock:
        # Universo de SKUs para consultar stock: SKUs vistos en GA4 en los
        # ultimos GA4_MESES_VISTAS meses, resueltos a su SKU hijo real
        # (donde vive el stock) via el mapeo padre->hijo cacheado -- ver
        # el punto 10 del docstring del archivo. Reemplaza el catalogo
        # completo del punto 9 (traia tambien configurables sin stock
        # propio, y no acotaba al trafico real).
        skus_vistos = obtener_skus_vistos_ga4()

        if not skus_vistos:
            print(
                "[AVISO] GA4 no devolvio SKUs vistos en la ventana configurada "
                f"({GA4_MESES_VISTAS} meses) -- revisar GA4_PROPERTY_ID/credenciales "
                "antes de asumir que el catalogo realmente no tuvo vistas."
            )

        # Resuelve padre->hijo usando el cache en GCS; solo llama a Magento
        # para los SKUs padre nuevos que todavia no esten cacheados.
        skus_core = actualizar_mapeo_padre_hijo(
            oauth, skus_vistos, gcs_project=GCS_PROJECT, gcs_bucket=GCS_BUCKET
        )
        print(f"SKUs padre vistos (GA4): {len(skus_vistos)} -> SKUs hijo reales a consultar: {len(skus_core)}")

        # Guardado de auditoria: que SKUs hijo entraron al universo de
        # stock esta corrida (distinto del mapeo cacheado en si, que vive
        # en su propio path fijo -- ver MAPEO_PADRE_HIJO_GCS_PATH).
        df_skus_core = pd.DataFrame({"SKU": skus_core})
        path_gcs_skus_core = escribir_a_gcs(
            df_skus_core, fecha_ejecucion,
            fuente="magento", entidad="skus_stock_resueltos",
            gcs_project=GCS_PROJECT, gcs_bucket=GCS_BUCKET,
        )
        print(f"{len(df_skus_core)} SKUs hijo escritos en gs://{GCS_BUCKET}/{path_gcs_skus_core}")

        print(f"\nConsultando stock para {len(skus_core)} SKUs hijo resueltos...")
        df_stock = importar_stock(oauth, skus_core)

        sin_stock = (~df_stock["En_Stock"]).sum()
        print(f"SKUs sin stock: {sin_stock} de {len(df_stock)}")

        df_stock.to_parquet(f"data/raw/stock_magento_{fecha_ejecucion}.parquet", index=False)
        print("Guardado local: stock_magento (respaldo/debug).")

        path_gcs_stock = escribir_a_gcs(
            df_stock, fecha_ejecucion,
            fuente="magento", entidad="stock",
            gcs_project=GCS_PROJECT, gcs_bucket=GCS_BUCKET,
        )
        print(f"{len(df_stock)} SKUs de stock escritos en gs://{GCS_BUCKET}/{path_gcs_stock}")

    if extraer_atributos_producto:
        print("\nTrayendo diccionarios de talle y color (una sola vez cada uno)...")
        talla_por_id = obtener_opciones_atributo(oauth, "size")
        color_por_id = obtener_opciones_atributo(oauth, "color")
        # genero_por_id = obtener_opciones_atributo(oauth, "gener")  # ajustar al attribute_code real que encontraste

        print(f"\nConsultando talle/color para {len(skus_unicos)} SKUs unicos...")
        df_atributos = importar_talle_color_genero(oauth, skus_unicos, talla_por_id, color_por_id)
        df_atributos.to_parquet(
            f"data/raw/atributos_producto_{desde_month}_{hasta_month}.parquet", index=False
        )
        print("Guardado: atributos_producto (talle/color).")

        df_ordenes = df_ordenes.merge(df_atributos, on="SKU", how="left")

    # Categoria Nino/Nina: deteccion por texto del nombre del producto
    # (no se consulta via API de categorias, ver extraer_categoria_edad_texto).
    df_ordenes["Es_Categoria_Nino"] = df_ordenes["Nombre_Producto"].apply(extraer_categoria_edad_texto)
    print(f"\nLineas de venta marcadas como categoria Nino: {df_ordenes['Es_Categoria_Nino'].sum()}")

    df_ordenes.to_parquet(f"data/raw/ordenes_magento_{desde_month}_{hasta_month}.parquet", index=False)
    print("Actualizado ordenes_magento_test.parquet con Cupon, Talle, Color, Fecha_Carga y Es_Categoria_Nino.")

    # Export a GCS (capa raw). Se mantiene tambien el .parquet local de
    # arriba por ahora, como respaldo/debug -- si ya no lo necesitas, se
    # puede sacar despues.
    path_gcs = escribir_a_gcs(
        df_ordenes, fecha_ejecucion,
        fuente="magento", entidad="ordenes",
        gcs_project=GCS_PROJECT, gcs_bucket=GCS_BUCKET,
    )
    print(f"\n{len(df_ordenes)} lineas de orden escritas en gs://{GCS_BUCKET}/{path_gcs}")