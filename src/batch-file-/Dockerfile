# File: Dockerfile
FROM python:3.11-slim

# Install system dependencies if needed
RUN apt-get update && apt-get install -y build-essential

# Copy project files
WORKDIR /app
COPY pyproject.toml poetry.lock ./

# Install Poetry
RUN pip install poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi

# Copy the rest of the code
COPY ppfl_python_worker/ ppfl_python_worker/

# Use Celery worker as the entry point
CMD ["celery", "-A", "ppfl_python_worker.csw.celery_app", "worker", "--loglevel=debug", "--without-heartbeat", "--without-mingle", "-E"]
