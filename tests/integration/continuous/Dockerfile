FROM flowbridge-media-smoke:local
COPY flowbridge /app/flowbridge
COPY requirements-media.txt /app/requirements-media.txt
COPY tests/integration/media/fixtures /demo/fixtures
COPY tests/integration/continuous /continuous
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "/continuous/smoke.py"]
