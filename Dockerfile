FROM prefecthq/prefect:2-python3.12

ENV PYTHONFAULTHANDLER=True \
    PYTHONUNBUFFERED=True \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    # Poetry's configuration:
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/pypoetry' \
    POETRY_HOME='/usr/local' \
    POETRY_VERSION=1.8.1

# Install setuptools for distutils
RUN pip install setuptools

# Install pipx
RUN python3 -m pip install pipx && python3 -m pipx ensurepath
ENV PATH="/root/.local/bin:$PATH"

# Install poetry for dependency management
RUN python3 -m pipx install poetry && python3 -m pipx upgrade poetry
RUN poetry config cache-dir ${WORKSPACE_DIR}/.cache && \
    poetry config virtualenvs.in-project true

# Copy only requirements to cache them in docker layer
WORKDIR /code
COPY poetry.lock pyproject.toml /code/

# Project initialization
RUN poetry install --no-root --no-interaction --no-ansi

ARG PREFECT_API_KEY
ENV PREFECT_API_KEY=$PREFECT_API_KEY

ARG PREFECT_API_URL
ENV PREFECT_API_URL=$PREFECT_API_URL

COPY prefect/flows/ /opt/prefect/flows/

ENTRYPOINT ["prefect", "agent", "start", "-q", "default"]