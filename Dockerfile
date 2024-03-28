# This image (built via Dockerfile.prefect) is used for the Prefect backend.
FROM prefecthq/prefect:2-python3.12 as python-base

# Install poetry
RUN pip install --upgrade pip
RUN pip install poetry==1.7.1

# Copy only requirements to cache them in docker layer
COPY prefect/flows/ /opt/chess-ratings/prefect/flows/
COPY poetry.lock pyproject.toml /opt/chess-ratings/
WORKDIR /opt/chess-ratings/

# Install packages in the system Python
RUN poetry config virtualenvs.create false
RUN poetry config virtualenvs.prefer-active-python true

# Install required packages
RUN poetry install --no-interaction --no-ansi

# Set Prefect environment variables
ARG PREFECT_API_KEY
ENV PREFECT_API_KEY=$PREFECT_API_KEY
ARG PREFECT_API_URL
ENV PREFECT_API_URL=$PREFECT_API_URL

# Set entrypoint to start prefect agent/worker 
ENTRYPOINT [ "prefect", "agent", "start", "-q", "chess-ratings-dev" ]