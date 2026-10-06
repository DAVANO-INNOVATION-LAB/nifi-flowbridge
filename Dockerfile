FROM python:3.12-slim
WORKDIR /app
COPY flowbridge /app/flowbridge
COPY web /app/web
COPY templates /app/templates
USER 65532:65532
EXPOSE 8790
CMD ["python", "-m", "flowbridge", "serve", "--host", "0.0.0.0"]
