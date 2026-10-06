FROM python:3.12-slim
LABEL org.opencontainers.image.source="https://github.com/DAVANO-INNOVATION-LAB/nifi-flowbridge"
LABEL org.opencontainers.image.licenses="Apache-2.0"
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements-live.txt /app/requirements-live.txt
COPY requirements-media.txt /app/requirements-media.txt
RUN pip install --no-cache-dir -r requirements-live.txt && mkdir /data && chown 65532:0 /data && chmod 2770 /data
COPY flowbridge /app/flowbridge
COPY web /app/web
COPY examples /app/examples
COPY docs/media-demo-evidence.json /app/docs/media-demo-evidence.json
COPY docs/nifi-native-import-evidence.json /app/docs/nifi-native-import-evidence.json
COPY templates /app/templates
COPY LICENSE /app/LICENSE
COPY third_party /app/third_party
USER 65532:0
EXPOSE 8790
CMD ["python", "-m", "flowbridge", "serve", "--host", "0.0.0.0"]

COPY docs/media-target-import-evidence.json /app/docs/media-target-import-evidence.json
