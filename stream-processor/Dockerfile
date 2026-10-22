FROM python:3.11.10-slim-bookworm

WORKDIR /app

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py config.py cdc_utils.py /app/

CMD ["python", "-u", "app.py"]
