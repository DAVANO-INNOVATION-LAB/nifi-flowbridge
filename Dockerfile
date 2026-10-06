FROM python:3.12-slim
LABEL org.opencontainers.image.source="https://github.com/DAVANO-INNOVATION-LAB/nifi-flowbridge"
LABEL org.opencontainers.image.licenses="Apache-2.0"
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements-live.txt /app/requirements-live.txt
RUN pip install --no-cache-dir -r requirements-live.txt && mkdir /data && chown 65532:0 /data && chmod 2770 /data
COPY flowbridge /app/flowbridge
COPY web /app/web
COPY templates /app/templates
COPY LICENSE /app/LICENSE
USER 65532:0
EXPOSE 8790
CMD ["python", "-m", "flowbridge", "serve", "--host", "0.0.0.0"]
