FROM flowbridge-media-smoke:local
COPY flowbridge /app/flowbridge
COPY examples/nifi-continuous-media.json /stress/native.json
COPY tests/integration/stress/run.py /stress/run.py
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "/stress/run.py"]
