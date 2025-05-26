# Authenticate with Scaleway CLI (first time only)
# scw init

# create image 
docker build -t my-app . --platform=linux/amd64            

# login first time only 
# docker login rg.fr-par.scw.cloud/bib-images -u nologin --password-stdin <<< "$SCW_SECRET_KEY"

# tag to the good format 
docker tag my-app:latest rg.fr-par.scw.cloud/bib-images/my-app:latest

# push image to registry
docker push rg.fr-par.scw.cloud/bib-images/my-app:latest

# then go to the console and redeploy the container
