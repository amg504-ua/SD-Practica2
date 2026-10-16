
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Common/ ./Common/
COPY WM_Central/ ./WM_Central/

CMD ["python", "-u", "WM_Central/WM_Central.py"]
