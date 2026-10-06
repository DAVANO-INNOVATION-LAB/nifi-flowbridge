# Preload the media test image and dependencies before entering the offline test.
FROM flowbridge-media-smoke:local
COPY flowbridge /app/flowbridge
COPY tests/integration/media/fixtures /demo/fixtures
COPY tests/integration/airgap /airgap
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "/airgap/smoke.py"]
