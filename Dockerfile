ARG JAVA_VERSION=11

FROM openjdk:${JAVA_VERSION}-jdk-slim AS base

ARG PYTHON_VERSION=3.10.12

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    libffi-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install pyenv
ENV PYENV_ROOT /root/.pyenv
ENV PATH $PYENV_ROOT/bin:$PATH
RUN curl https://pyenv.run | bash

# Install Python using pyenv
RUN pyenv install ${PYTHON_VERSION} && pyenv global ${PYTHON_VERSION}

# Ensure pyenv shims are in the PATH
ENV PATH $PYENV_ROOT/shims:$PATH

# Install pip for the specific Python version
# Extract major and minor version for pip installation
RUN PYTHON_MAJOR_MINOR=$(echo ${PYTHON_VERSION} | cut -d. -f1,2) \
    && curl -sS https://bootstrap.pypa.io/get-pip.py | python${PYTHON_MAJOR_MINOR}


# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python${PYTHON_MAJOR_MINOR} \
    && ln -s /root/.local/bin/poetry /usr/local/bin/poetry

FROM base AS dev-image

# Install dependencies
COPY pyproject.toml .
# Ensure Poetry does not create a virtual environment
RUN poetry config virtualenvs.create false
RUN poetry install --no-root --with=test
RUN poetry config virtualenvs.create false


RUN mkdir -p /app/output

# Set up the working directory
WORKDIR /app

# Copy the application code to the container
COPY . /app
