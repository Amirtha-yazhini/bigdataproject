FROM python:3.11-slim
WORKDIR /data
RUN pip install --no-cache-dir requests==2.32.3
ENV PYTHONUNBUFFERED=1
CMD ["python", "/data/bootstrap.py"]
