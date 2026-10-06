FROM nifi-flowbridge-flowbridge:latest
USER root
RUN pip install --no-cache-dir boto3==1.42.49
COPY flowbridge /app/flowbridge
COPY tests/integration/media /demo
ENV PYTHONPATH=/app
USER 65532:0
ENTRYPOINT ["python", "/demo/smoke.py"]
