# Imagen del Cloud Run Job del extractor de Magento (World Sport).
#
# IMPORTANTE: este Dockerfile espera correr con el CONTEXTO en la raiz del
# repo (worldsport-extractors/), no parado dentro de magento/ -- por eso
# copia "magento/..." y "_common/" con ruta completa. Ver cloudbuild.yaml
# en esta misma carpeta para el comando de build correcto, y el README.md
# de la raiz del repo para la explicacion completa.
#
# Es un batch job (corre y termina), no un servicio HTTP -- no necesita
# exponer ningun puerto.

FROM python:3.11-slim

WORKDIR /app

COPY magento/requirements.txt magento/requirements.txt
RUN pip install --no-cache-dir -r magento/requirements.txt

COPY _common/ ./_common/
COPY magento/src/ ./magento/src/

CMD ["python", "magento/src/extraccion_magento.py"]
