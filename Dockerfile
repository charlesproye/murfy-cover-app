# Utiliser la dernière image Miniconda3
FROM continuumio/miniconda3:latest

# # Définir quelques variables d'environnement pour optimiser la mémoire et la performance
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    PYTHONMALLOC=debug \
    PYTHONFAULTHANDLER=1 \
    PYTHONASYNCIODEBUG=0 \
    PYTHONIOENCODING=utf-8 \
    PYTHONOPTIMIZE=2 \
    PYTHONHASHSEED=random \
    PYTHONPATH=/app/src

# Copier votre application dans le conteneur
WORKDIR /app

COPY src src/
COPY conda-env.yaml conda-env.yaml

# Installer les dépendances
RUN conda update -n base -c defaults conda
RUN conda env update --prune -n base -f conda-env.yaml
