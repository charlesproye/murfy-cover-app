# Authenticate with Scaleway CLI (first time only)
# scw init

# create image 
docker compose -f web/docker-compose.yml build    

# login first time only 
# docker login rg.fr-par.scw.cloud/bib-images -u nologin --password-stdin <<< "$SCW_SECRET_KEY"

# tag to the good format 
docker tag ingestion-web-app:latest rg.fr-par.scw.cloud/bib-images/ingestion-web-app:latest

# push image to registry
docker push rg.fr-par.scw.cloud/bib-images/ingestion-web-app:latest

# then go to the console and redeploy the container
