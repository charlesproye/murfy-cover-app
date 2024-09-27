# Ingestion High Mobility 

Adding a new brand to process
To enable processing of a new brand, simply create a file with the brand name in src/ingestion/high_mobility/schema/brands. The brand name should be one of bmw, citroen, ds, mercedes-benz, mini, opel, peugeot, vauxhall, jeep, fiat, alfaromeo, ford, renault, toyota, lexus, porsche, maserati, kia, tesla, volvo-cars, sandbox, replacing any - with _.
In this file, you need to define the class corresponding to the data format returned by the API, and the class corresponding to the compressed data format. To do this, take examples from those already present in the folder. Then, you'll need to decorate the class corresponding to the API data with @register_brand(rate_limit=<rate_limit>), and the class corresponding to the compressed data with @register_merged. These decorators will make the classes available, as well as a function for decoding the API data.

