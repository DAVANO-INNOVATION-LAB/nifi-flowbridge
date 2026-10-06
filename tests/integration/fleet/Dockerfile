FROM flowbridge-media-smoke:local
COPY flowbridge /app/flowbridge
COPY tests /app/tests
ENV PYTHONPATH=/app:/app/tests
ENTRYPOINT ["python", "/app/tests/integration/fleet/smoke.py"]
