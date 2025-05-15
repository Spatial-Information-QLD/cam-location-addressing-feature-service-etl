ARG PYTHON_VERSION=3.13

#
# Build
#
FROM python:${PYTHON_VERSION}-alpine AS python-build

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app
COPY . .

RUN uv sync

#
# Final
#
FROM python:${PYTHON_VERSION}-alpine

# Create a non-root user
RUN adduser -D -h /app appuser

# Set virtual environment path
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --from=python-build /app /app
WORKDIR /app

# Change ownership of the app directory to the new user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

CMD ["python", "main.py"]
