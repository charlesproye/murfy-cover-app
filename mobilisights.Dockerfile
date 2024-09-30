# Utiliser la dernière image Miniconda3
FROM continuumio/miniconda3:latest

# Définir quelques variables d'environnement
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

# Copier votre application dans le conteneur
COPY . /app/
WORKDIR /app

# Installer les dépendances
RUN conda update -n base -c defaults conda
RUN conda env update --prune -n base -f ingestion.conda-env.yaml
RUN chmod +x ./start_tesla.sh

# Définir le shell par défaut pour utiliser conda
SHELL ["/bin/bash", "-c"]

# Démarrer l'application
CMD ["./start_mobilisights.sh"]
