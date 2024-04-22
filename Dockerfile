FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY poetry.lock .
RUN pip install poetry && poetry config virtualenvs.create false && poetry install --no-dev --no-interaction --no-ansi
COPY . .

RUN chmod +x prestart.sh

ENTRYPOINT ["./prestart.sh"]

CMD ["python", "main.py"]
