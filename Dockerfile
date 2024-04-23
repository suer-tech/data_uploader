FROM python:3.11-slim

RUN apt-get update && apt-get install -y postgresql-client

WORKDIR /app

COPY pyproject.toml .
COPY poetry.lock .

RUN pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi

COPY . .
COPY prestart.sh .
RUN ls -la

RUN chmod +x prestart.sh
RUN ls -la
ENTRYPOINT  ["bash", "./prestart.sh"]

CMD ["python", "uploader/main.py"]
