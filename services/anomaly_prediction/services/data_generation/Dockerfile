FROM python:3.10-slim

WORKDIR /app

COPY services/data_generation/ ./services/data_generation/
COPY shared_lib/ ./shared_lib/
COPY services/data_generation/requirements.txt .

ENV PYTHONPATH=/app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "services.data_generation.producer"]