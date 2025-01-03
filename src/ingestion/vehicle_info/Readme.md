Do be able to have all the information about the vehicles we need to store all the static data about the vehicles.

You'll find here some information : https://www.notion.so/bib-batteries/Pipeline-1292de3b75c780328d18eba362868645?pvs=4#1602de3b75c7801289a1e078d9f8aa65

Depeding on the brand we will base are dat either on the data from our customer or directly on the data from the OEMs
At the moment we have the following brands : 
- Tesla : tesla.py

For all the others brands we are basing our data on the one given by the leasing company
- other.py

The process is the following : 
1. We list all the VIN and their ownership : fleet_info.py
2. Depending on the brand we run the right script to get the model information 
    a. We get the list of the VIN for the particluar brand 
    b. We do all the API call to get the information of the model 
    c. We add the model that still don't exist into the database vehicle_model
    d. We load the vehicle with the right ownership and vehicle_model
3. We load the date in the database with the right ownership



