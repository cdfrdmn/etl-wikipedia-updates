FROM ghcr.io/astral-sh/uv:python3.12-alpine

WORKDIR /app

# Enable bytecode compilation (.pyc) for faster app launch
ENV UV_COMPILE_BYTECODE=1

# Copy lockfiles separately from the rest of the project. This
#   allows docker to update dependencies ONLY if they've changed.
COPY uv.lock pyproject.toml /app/

# Install dependencies
RUN uv sync --frozen --no-install-project --no-dev

# Copy the rest of the application
COPY . /app

# Place uv binaries on the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Streamlit default port is 8501
EXPOSE 8501