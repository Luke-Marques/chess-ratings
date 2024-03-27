FROM python:3.12

ENV POETRY_NO_INTERACTION=true \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=1.7.1

# Install setuptools for distutils
RUN pip install setuptools

# Install setuptools for distutils
RUN pip install setuptools

# Install pipx
RUN python3 -m pip install pipx && python3 -m pipx ensurepath
ENV PATH="/root/.local/bin:$PATH"

# Install poetry for dependency management
RUN python3 -m pipx install poetry && python3 -m pipx upgrade poetry

# Copy only requirements to cache them in docker layer
WORKDIR /opt
COPY poetry.lock pyproject.toml /opt/

# Project initialization
RUN poetry install --no-root --no-interaction --no-ansi

ARG PREFECT_API_KEY
ENV PREFECT_API_KEY=$PREFECT_API_KEY
 
ARG PREFECT_API_URL
ENV PREFECT_API_URL=$PREFECT_API_URL

COPY prefect/flows/ /opt/prefect/flows/

ENTRYPOINT [ "poetry", "run", "prefect", "agent", "start", "-q", "chess-ratings-dev" ]