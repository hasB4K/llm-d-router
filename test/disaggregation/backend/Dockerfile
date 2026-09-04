FROM python:3.12-slim

RUN pip install --no-cache-dir fastapi==0.116.1 uvicorn==0.35.0

COPY server.py /server.py

EXPOSE 8080

CMD ["python", "/server.py"]
