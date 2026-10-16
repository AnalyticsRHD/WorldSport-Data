# Imagen del Cloud Run Job del extractor de Mercado Ads (World Sport).
#
# IMPORTANTE: este Dockerfile espera correr con el CONTEXTO en la raiz del
# repo (worldsport-extractors/), no parado dentro de mercado-libre/ -- por
# eso copia "mercado-libre/..." y "_common/" con ruta completa. Ver
# cloudbuild.yaml en esta misma carpeta para el comando de build correcto, y
# el README.md de la raiz del repo para la explicacion completa (mismo
# patron que magento/Dockerfile).
#
# Es un batch job (corre y termina, --mode daily-sync), no un servicio HTTP
# -- no necesita exponer ningun puerto.

FROM python:3.11-slim

WORKDIR /app

COPY mercado-libre/requirements.txt mercado-libre/requirements.txt
RUN pip install --no-cache-dir -r mercado-libre/requirements.txt

COPY _common/ ./_common/
COPY mercado-libre/src/ ./mercado-libre/src/

CMD ["python", "mercado-libre/src/extraccion_meli.py", "--mode", "daily-sync"]
