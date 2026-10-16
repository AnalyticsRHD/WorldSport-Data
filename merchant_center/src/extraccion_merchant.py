"""
Google Merchant Center -> GCS (raw/), catalogo de las 5 marcas de World Sport.

Trae el catalogo CRUDO (sin agregar) de las 5 cuentas de Merchant Center
(DC Shoes, Ala Moana, Quiksilver, Roxy, Rvca) via la Merchant API nueva
(no la vieja Content API for Shopping), metodo `products.list`.

NOTA (2026-09): la libreria usa `merchant_products_v1` (NO v1beta -- v1beta
fue discontinuada por Google el 28/02/2026, de ahi el error 409 "The v1beta
version of the Merchant API was discontinued..."). El cambio de v1beta a v1
tambien afecta el tipo de dos campos leidos mas abajo: `availability` y
`condition` pasaron de string plano a enum (se guardan como `.name`, ej.
"IN_STOCK"/"NEW", mismo criterio que ya se usaba para `severity`). Verificado
instalando la libreria e inspeccionando el schema real, no solo por doc.

- Auth: Application Default Credentials, via la service account
  ws-account@worldsport-503720.iam.gserviceaccount.com ya adjunta al Cloud
  Run Job (mismo criterio que el resto de los extractores de este repo para
  autenticarse contra GCS) -- sin archivo de credenciales ni OAuth
  interactivo. Requiere que esa service account este agregada como usuario
  (Configuracion de la cuenta -> Acceso) en CADA una de las 5 cuentas de
  Merchant Center -- son cuentas individuales, no una MCA/multi-cliente
  comun, asi que hay que agregarla 5 veces por separado.
- Por que `products.list` y no `reports.search`/`product_view`: esto ultimo
  da una vista ya aplanada y parcial (solo las columnas que pidas en el
  SELECT). Como esto va al bucket raw/, interesa guardar el dato tal cual
  lo procesa Google -- status por destino, issues por producto -- sin
  decidir de entrada que descartar.
- Las 5 marcas se consolidan en un solo DataFrame con columna `Marca`
  (no un archivo por marca) -- mismo criterio que ya se usa para inversion
  Meta/Google en el resto del pipeline de World Sport (ver
  `world-sport-plan-trabajo-mmm.md` en el proyecto de Claude).
- Se escribe a `raw/merchant-center/productos/{fecha_ejecucion}.json`
  (JSON Lines, formato directo para `bq load --source_format=NEWLINE_DELIMITED_JSON`)
  via `escribir_a_gcs()` de `_common/gcs_utils.py` -- no se reimplementa la
  subida a GCS aca. `fecha_ejecucion` es el dia de la corrida: una
  re-ejecucion el mismo dia pisa el archivo de ese dia en vez de acumular.

PENDIENTE, BLOQUEANTE antes de correr esto contra datos reales:
1. Completar `MERCHANT_CENTER_IDS` de abajo -- estan los 5 valores en
   `None` a proposito (placeholder). El merchantId de cada cuenta se ve en
   Merchant Center -> Configuracion de la cuenta, arriba a la derecha.
   Una marca con `None` se salta (se loguea, no rompe el resto del Job).
2. Agregar la service account como usuario en cada una de las 5 cuentas de
   Merchant Center (ver arriba) -- sin esto, esa marca puntual va a fallar
   con un error de permisos (tambien se loguea y se salta, no corta el Job).
3. Registrar el proyecto de GCP (worldsport-503720) contra Merchant Center
   -- v1 exige un `registerGcp` UNA SOLA VEZ por proyecto de GCP antes de
   que cualquier llamada a la API funcione (sin esto, las 5 cuentas van a
   fallar con 401 UNAUTHENTICATED, no con el 409 de v1beta). Segun la guia
   oficial de Google (Merchant API FAQ), una agencia debe registrar el
   proyecto contra UNA cuenta "primaria" centralizada -- no contra cada
   cuenta de cliente por separado -- y despues de eso el mismo proyecto
   queda habilitado para operar sobre cualquier otra cuenta de Merchant
   Center a la que tenga acceso (las 5 marcas, en este caso). Falta
   definir cual de las cuentas actua como esa cuenta "primaria" de la
   agencia (idealmente una cuenta propia de Red Hook Data / World Sport a
   nivel agencia, no una de las 5 marcas puntuales) -- este registro se
   hace UNA vez, requiere acceso de Administrador en esa cuenta y un
   `developer_email` humano, via
   `DeveloperRegistrationServiceClient.register_gcp()`
   (`google.shopping.merchant_accounts_v1`). No esta implementado en este
   archivo porque es un paso de configuracion manual, no parte de la
   corrida periodica del Job.
"""

import os
import sys
import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from google.shopping import merchant_products_v1 as merchant_products

# Raiz del repo = dos niveles arriba de este archivo (merchant_center/src/...
# -> repo root). Necesario para `from _common.gcs_utils import escribir_a_gcs`
# tanto corriendo local (parado en la raiz del repo) como dentro del
# container (donde el Dockerfile copia _common/ y merchant_center/src/
# preservando esta misma estructura relativa -- ver merchant_center/Dockerfile).
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from _common.gcs_utils import escribir_a_gcs

load_dotenv()

GCS_PROJECT = os.getenv("GCS_PROJECT")
GCS_BUCKET = os.getenv("GCS_BUCKET")

TIMEZONE_NEGOCIO = ZoneInfo("America/Argentina/Buenos_Aires")

# PLACEHOLDER -- completar con el merchantId real de cada cuenta (Merchant
# Center -> Configuracion de la cuenta, arriba a la derecha). Una marca en
# None se salta en importar_productos_merchant_center() (se loguea, no
# rompe el resto del Job).
MERCHANT_CENTER_IDS = {
    "DC Shoes": 556839041,
    "Ala Moana": 556838720,
    "Quiksilver": 556857802,
    "Roxy": 556857802,
    "Rvca": 5529234663,
}

PAGE_SIZE = 250


def importar_productos_merchant_center(merchant_ids=None):
    """
    Loopea `merchant_ids` (por defecto MERCHANT_CENTER_IDS) y trae el
    catalogo completo de cada cuenta via `products.list`.

    Devuelve un DataFrame con una fila por producto y columna `Marca` --
    si ninguna marca tiene merchantId configurado o todas fallan, devuelve
    un DataFrame vacio (no None), para que escribir_a_gcs() de mas abajo
    no se rompa por eso.
    """
    merchant_ids = merchant_ids if merchant_ids is not None else MERCHANT_CENTER_IDS

    client = merchant_products.ProductsServiceClient()

    filas = []
    for marca, merchant_id in merchant_ids.items():
        if not merchant_id:
            print(f"[Merchant Center] {marca}: sin merchantId configurado todavia (placeholder) -- se salta.")
            continue

        request = merchant_products.ListProductsRequest(
            parent=f"accounts/{merchant_id}",
            page_size=PAGE_SIZE,
        )

        try:
            productos_marca = 0
            for producto in client.list_products(request=request):
                atributos = producto.product_attributes
                estado = producto.product_status

                issues = [
                    {
                        "code": issue.code,
                        "severity": issue.severity.name if issue.severity is not None else None,
                        "attribute": issue.attribute,
                    }
                    for issue in getattr(estado, "item_level_issues", [])
                ]

                filas.append({
                    "Marca": marca,
                    "Merchant_Id": merchant_id,
                    "Offer_Id": producto.offer_id,
                    "Product_Resource_Name": producto.name,
                    "Titulo": atributos.title if atributos else None,
                    # En v1, availability/condition pasaron de string a enum
                    # (antes en v1beta eran texto plano) -- se guarda el
                    # nombre del enum (ej. "IN_STOCK", "NEW") via .name,
                    # mismo criterio que ya se usaba para severity/reporting_context.
                    "Disponibilidad": atributos.availability.name if atributos else None,
                    "Condicion": atributos.condition.name if atributos else None,
                    "Estados_Destino": [
                        d.reporting_context.name
                        for d in getattr(estado, "destination_statuses", [])
                    ],
                    "Cantidad_Issues": len(issues),
                    "Item_Level_Issues": issues,
                })
                productos_marca += 1

            print(f"[Merchant Center] {marca} (merchantId={merchant_id}): {productos_marca} productos.")

        except Exception as exc:
            # No se corta la corrida completa por una cuenta que falle (ej.
            # service account todavia no agregada como usuario en esa
            # cuenta puntual, o merchantId mal cargado) -- se registra el
            # error y se sigue con las demas marcas.
            print(f"[Merchant Center] {marca} (merchantId={merchant_id}) fallo: {exc}")

    return pd.DataFrame(filas)


if __name__ == "__main__":
    HOY = datetime.datetime.now(TIMEZONE_NEGOCIO)
    fecha_ejecucion = HOY.strftime("%Y-%m-%d")

    print("Consultando catalogo en Google Merchant Center (5 cuentas)...")
    df_productos = importar_productos_merchant_center()
    print(f"Merchant Center devolvio {len(df_productos)} productos en total (5 marcas).")

    path_gcs = escribir_a_gcs(
        df_productos, fecha_ejecucion,
        fuente="merchant-center", entidad="productos",
        gcs_project=GCS_PROJECT, gcs_bucket=GCS_BUCKET,
    )
    print(f"{len(df_productos)} productos de Merchant Center escritos en gs://{GCS_BUCKET}/{path_gcs}")