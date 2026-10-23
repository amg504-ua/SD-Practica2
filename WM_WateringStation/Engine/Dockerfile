FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Common/ ./Common/
COPY WM_WateringStation/Engine/ ./WM_WateringStation/Engine/

CMD ["python", "-u", "WM_WateringStation/Engine/WM_WS_E.py"]