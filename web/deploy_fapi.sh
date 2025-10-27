# Authenticate with Scaleway CLI (first time only)
# scw init

# create image
docker compose -f ../docker-compose.yaml build ingestion-web-app

# login first time only
# docker login rg.fr-par.scw.cloud/data-engineering -u nologin --password-stdin <<< "$SCW_SECRET_KEY"

# tag to the good format
docker tag ingestion-web-app:latest rg.fr-par.scw.cloud/data-engineering/ingestion-web-app:latest

# push image to registry
docker push rg.fr-par.scw.cloud/data-engineering/ingestion-web-app:latest

# then go to the console and redeploy the container

