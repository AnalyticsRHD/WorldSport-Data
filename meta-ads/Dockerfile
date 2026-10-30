# Imagen del Cloud Run Job del extractor de Meta Ads (World Sport).
#
# IMPORTANTE: este Dockerfile espera correr con el CONTEXTO en la raiz del
# repo (worldsport-extractors/), no parado dentro de meta-ads/ -- por eso
# copia "meta-ads/..." y "_common/" con ruta completa. Ver cloudbuild.yaml
# en esta misma carpeta para el comando de build correcto, y el README.md
# de la raiz del repo para la explicacion completa (mismo patron que
# magento/Dockerfile).
#
# Es un batch job (corre y termina, --mode daily-sync), no un servicio HTTP
# -- no necesita exponer ningun puerto.

FROM python:3.11-slim

WORKDIR /app

COPY meta-ads/requirements.txt meta-ads/requirements.txt
RUN pip install --no-cache-dir -r meta-ads/requirements.txt

COPY _common/ ./_common/
COPY meta-ads/src/ ./meta-ads/src/

CMD ["python", "meta-ads/src/extraccion_meta.py", "--mode", "daily-sync"]
