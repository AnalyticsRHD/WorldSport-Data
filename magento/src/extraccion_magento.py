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

# Raiz del repo = dos niveles arriba de este archivo (magento/src/... -> repo root).
# Necesario para poder hacer `from _common.gcs_utils import escribir_a_gcs`
# tanto corriendo local (parado en la raiz del repo) como dentro del
# container (donde el Dockerfile copia _common/ y magento/src/ preservando
# esta misma estructura relativa -- ver magento/Dockerfile).
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from _common.gcs_utils import escribir_a_gcs

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


def importar_stock(oauth, skus, skus_por_lote=40):
    """
    Consulta el stock MSI (multi-fuente) para una lista de SKUs.
    Devuelve un DataFrame con SKU, cantidad total y si esta en stock.

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

    if not probar_conexion(oauth):
        raise SystemExit("La conexion de prueba fallo. Revisa las credenciales antes de importar todo.")

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

    if False:
        print(f"\nConsultando stock para {len(skus_unicos)} SKUs unicos...")
        df_stock = importar_stock(oauth, skus_unicos)
        df_stock.to_parquet(f"data/raw/stock_magento_{desde_month}_{hasta_month}.parquet", index=False)
        print("Guardado: stock_magento_test.parquet")

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
