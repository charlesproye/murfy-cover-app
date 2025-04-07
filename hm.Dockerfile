# Utiliser la dernière image Miniconda3
FROM continuumio/miniconda3:latest

# Définir quelques variables d'environnement pour optimiser la mémoire et la performance
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
    PYTHONHASHSEED=random

# Copier votre application dans le conteneur
COPY . /app/
WORKDIR /app

# Installer les dépendances
RUN conda update -n base -c defaults conda
RUN conda env update --prune -n base -f ingestion.conda-env.yaml
# Installer des outils supplémentaires pour la gestion de la mémoire et le diagnostic
RUN pip install memory-profiler psutil
RUN chmod +x ./start_hm.sh

# Définir le shell par défaut pour utiliser conda
SHELL ["/bin/bash", "-c"]

# Démarrer l'application avec des limites de mémoire pour Python
CMD ["./start_hm.sh"] 
#, "--max_workers", "4", "--compress_threaded"]
