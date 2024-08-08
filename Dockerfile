# Use the latest Miniconda3 image
FROM continuumio/miniconda3:latest

# Set some environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

# Copy your app into the container
COPY . /app/
WORKDIR /app

# Install dependencies
RUN conda update -n base -c defaults conda
RUN conda env update --prune -n base -f conda-env.yaml
RUN chmod +x ./start.sh

# Start the application
CMD ["./start.sh"]
