# Use the latest micromamba image (lighter than miniconda)
FROM mambaorg/micromamba:latest as conda

# Create environment
COPY --chown=$MAMBA_USER:$MAMBA_USER ingestion.conda-lock.yaml /tmp/conda-lock.yaml
RUN micromamba install -y -n base -f /tmp/conda-lock.yaml && \
    micromamba clean --all --yes

COPY . .

ARG MAMBA_DOCKERFILE_ACTIVATE=1  # (otherwise python will not be found)
# Start the application
CMD ["./start.sh"]

