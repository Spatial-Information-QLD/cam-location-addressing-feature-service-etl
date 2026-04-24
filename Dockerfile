ARG PYTHON_VERSION=3.13
ARG UV_VERSION=0.9.28

#
# Build
#
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim AS python-build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_CACHE_DIR=/tmp/uv-cache

COPY --from=uv /uv /uvx /bin/

WORKDIR /app
COPY . .

RUN uv sync --frozen --no-dev

#
# Final
#
FROM python:${PYTHON_VERSION}-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Create a non-root user
RUN useradd --system --create-home --home-dir /app appuser

# Set virtual environment path
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --from=python-build /app /app
WORKDIR /app

# Change ownership of the app directory to the new user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

CMD ["python", "main_pls.py"]
