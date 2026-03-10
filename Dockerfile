FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN adduser --disabled-password --gecos "" appuser
USER appuser

# Expose Prometheus metrics port
EXPOSE 9100

ENTRYPOINT ["python", "-m", "processor.main"]
CMD ["start"]
