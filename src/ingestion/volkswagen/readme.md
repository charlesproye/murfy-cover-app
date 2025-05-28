# Volkswagen Ingester

In this directory we implement the client for the volkswagen api. 
You just need to use it to activate vehicles and thats all the data will then be send to our server by them. 

# Activate a vehicle
store the vins you want to activate in a txt file and run this command
```bash
python -m src.ingestion.volkswagen.activate_vehicle path_to_your_vin_file
```
