FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Common/ ./Common/
COPY WM_WateringStation/Monitor/ ./WM_WateringStation/Monitor/

CMD ["python", "-u", "WM_WateringStation/Monitor/WM_WS_M.py"]