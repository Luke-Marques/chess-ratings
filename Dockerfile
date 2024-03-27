FROM python:3.12

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
    POETRY_VERSION=1.7.1

# Install setuptools for distutils
RUN pip install setuptools

# System deps:
RUN curl -sSL https://install.python-poetry.org | python3 -

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

ENTRYPOINT [ "poetry", "run", "prefect", "agent", "start", "-q", "chess-ratings-dev" ]