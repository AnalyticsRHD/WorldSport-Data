"""
Registro UNICO del proyecto de GCP contra Merchant Center (`registerGcp`).

QUE ES ESTO Y POR QUE EXISTE COMO ARCHIVO APARTE
-------------------------------------------------
La Merchant API v1 exige que el proyecto de GCP (worldsport-503720) este
"registrado" contra una cuenta de Merchant Center antes de que CUALQUIER
llamada a la API funcione (sin esto, `extraccion_merchant_center.py` falla
con 401 UNAUTHENTICATED en las 5 marcas). Este registro se hace UNA SOLA VEZ
por proyecto de GCP -- no una vez por marca -- y despues de hecho, el mismo
proyecto queda habilitado para operar sobre cualquier otra cuenta de
Merchant Center a la que tenga acceso (segun la guia oficial de Google:
"Third party developers and agencies should maintain their own primary
Merchant Center account and register all of their Google Cloud projects to
that single, centralized account. Don't register your Google Cloud project
with each individual merchant's account.").

Cuenta "primaria" elegida (2026-09): DC Shoes (merchantId 556839041).
Una vez corrido este script contra DC Shoes, las otras 4 marcas (Ala Moana,
Quiksilver, Roxy, Rvca) quedan habilitadas tambien -- NO hace falta repetir
el registro para cada una.

Por eso este script vive SUELTO en `merchant_center/` (no en `src/`): no es
parte de la corrida periodica del Cloud Run Job (el Dockerfile solo copia
`merchant_center/src/`, asi que esto nunca entra a la imagen), es un paso de
configuracion manual que se corre una vez desde una notebook/terminal local
y despues no se vuelve a tocar.

REQUISITOS ANTES DE CORRERLO
-----------------------------
1. Acceso de Administrador en la cuenta de Merchant Center de DC Shoes
   (merchantId 556839041) -- Configuracion de la cuenta -> Acceso -> tu
   usuario debe figurar como Administrador (no alcanza con "Estandar").
2. Autenticarse localmente con ESE usuario administrador (no con la service
   account) corriendo, antes de este script:
       gcloud auth application-default login
   e iniciando sesion con la cuenta de Google que tiene ese acceso Admin.
   (El registro queda asociado al proyecto de GCP, no a la identidad que lo
   ejecuta -- pero la llamada en si requiere que quien la hace tenga permiso
   de Administrador sobre la cuenta de Merchant Center indicada.)
3. Completar `DEVELOPER_EMAIL` mas abajo con un email de contacto real (el
   que Google va a usar para notificaciones sobre el acceso a la API de esta
   cuenta) -- no afecta el funcionamiento tecnico del extractor, es solo el
   contacto administrativo del registro. Se dejo `analytics@redhookdata.com`
   como default; cambialo si preferis otro contacto (ej. Ciro Derito, que es
   quien lleva Tech de forma transversal).

COMO CORRERLO
--------------
    cd merchant_center
    pip install google-shopping-merchant-accounts  # no esta en requirements.txt
                                                     # a proposito: ese archivo
                                                     # es para la imagen del Job
                                                     # recurrente, y este script
                                                     # nunca corre ahi.
    python setup_registrar_gcp.py

CORRECCION (2026-09): `register_gcp()` NO es idempotente -- si el proyecto
ya esta registrado (contra la cuenta que sea), tira 409 ALREADY_EXISTS en
vez de no hacer nada. El script de abajo lo maneja: si ese error aparece,
en vez de cortar, llama a `get_account_for_gcp_registration()` para mostrar
contra que cuenta quedo registrado el proyecto -- eso confirma que el paso
ya esta hecho (aunque haya sido registrado contra otra cuenta que no sea DC
Shoes, no importa: el registro es por PROYECTO de GCP, sirve igual para las
5 marcas).
"""

from google.api_core.exceptions import AlreadyExists
from google.shopping import merchant_accounts_v1 as merchant_accounts

# Cuenta "primaria" elegida para el registro -- ver docstring de arriba.
# NO cambiar por el merchantId de otra marca: el registro es UNICO para las
# 5 cuentas, se hace una sola vez contra esta.
PRIMARY_MERCHANT_ID = "556839041"  # DC Shoes

# Email de contacto administrativo para el registro (no es una credencial).
DEVELOPER_EMAIL = "analytics@redhookdata.com"


def cuenta_registrada_actualmente():
    """
    Devuelve el `name` (ej. "accounts/556839041") de la cuenta contra la que
    el proyecto de GCP actual ya esta registrado, o None si todavia no se
    registro contra ninguna.
    """
    client = merchant_accounts.DeveloperRegistrationServiceClient()
    respuesta = client.get_account_for_gcp_registration(request={})
    return respuesta.name


def registrar_gcp(merchant_id=PRIMARY_MERCHANT_ID, developer_email=DEVELOPER_EMAIL):
    client = merchant_accounts.DeveloperRegistrationServiceClient()
    name = client.developer_registration_path(merchant_id)

    request = merchant_accounts.RegisterGcpRequest(
        name=name,
        developer_email=developer_email,
    )

    print(f"Registrando el proyecto de GCP contra {name} (contacto: {developer_email})...")
    try:
        registration = client.register_gcp(request=request)
        print("Registro OK:", registration)
        return name
    except AlreadyExists:
        # El proyecto ya estaba registrado (posiblemente de un intento
        # anterior) -- no es un error real, solo confirmamos contra que
        # cuenta quedo y seguimos. El registro es por proyecto de GCP, asi
        # que sirve igual para las 5 marcas independientemente de contra
        # cual haya quedado -- por eso consultamos la cuenta REAL en vez de
        # asumir que fue justo contra PRIMARY_MERCHANT_ID.
        cuenta_actual = cuenta_registrada_actualmente()
        print(f"El proyecto ya estaba registrado (contra {cuenta_actual}) -- no hace falta repetirlo.")
        return f"{cuenta_actual}/developerRegistration"


def consultar_estado(developer_registration_name):
    client = merchant_accounts.DeveloperRegistrationServiceClient()
    estado = client.get_developer_registration(name=developer_registration_name)
    print("Estado actual del registro:", estado)
    return estado


if __name__ == "__main__":
    nombre_registro = registrar_gcp()
    print()
    consultar_estado(nombre_registro)