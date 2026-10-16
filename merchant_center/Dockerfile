# Imagen del Cloud Run Job del extractor de Merchant Center (World Sport).
#
# IMPORTANTE: este Dockerfile espera correr con el CONTEXTO en la raiz del
# repo (worldsport-extractors/), no parado dentro de merchant_center/ -- por
# eso copia "merchant_center/..." y "_common/" con ruta completa. Ver
# cloudbuild.yaml en esta misma carpeta para el comando de build correcto, y
# el README.md de la raiz del repo para la explicacion completa.
#
# Es un batch job (corre y termina), no un servicio HTTP -- no necesita
# exponer ningun puerto.

FROM python:3.11-slim

WORKDIR /app

COPY merchant_center/requirements.txt merchant_center/requirements.txt
RUN pip install --no-cache-dir -r merchant_center/requirements.txt

COPY _common/ ./_common/
COPY merchant_center/src/ ./merchant_center/src/

CMD ["python", "merchant_center/src/extraccion_merchant_center.py"]