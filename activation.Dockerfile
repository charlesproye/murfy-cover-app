# Use the latest Miniconda3 image
FROM continuumio/miniconda3:latest

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    PYTHONIOENCODING=utf-8 \
    PYTHONOPTIMIZE=2 \
    PYTHONHASHSEED=random \
    PYTHONPATH=/app/src

# Set working directory
WORKDIR /app

# Copy only environment spec first (cacheable)
COPY activation.conda-env.yaml .

# Installer les dépendances
RUN conda update -n base -c defaults conda
RUN conda env update --prune -n base -f activation.conda-env.yaml

# Copy source code (this is what changes frequently)
COPY src/core src/core
COPY src/activation src/activation

# Entrypoint
CMD ["python", "src/activation/main.py"]

