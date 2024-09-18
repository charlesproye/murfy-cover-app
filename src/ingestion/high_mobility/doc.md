# Ingestion High Mobility 

## Ajouter une marque à traiter

Pour permettre de traiter une nouvelle marque, il suffit de créer un fichier avec le nom de la marque dans `src/ingestion/high_mobility/schema/brands`. Le nom de la marque doit être un parmi ` bmw, citroen, ds, mercedes-benz, mini, opel, peugeot, vauxhall, jeep, fiat, alfaromeo, ford, renault, toyota, lexus, porsche, maserati, kia, tesla, volvo-cars, sandbox`, en remplaçant les `-` par des `_`.

Il faut ensuite dans ce fichier définir la classe correspondant au format de données renvoyé par l'API, et la classe correspondant au format des données compressées. Pour cela prendre exemple sur celles déjà présentes dans le dossier. Il suffira ensuite de décorer la classe correspondant au données API avec `@register_brand(rate_limit=<rate_limit>)`, et la classe correspondant aux données compressées avec `@register_merged`. Ces décorateurs vont rendre disponibles les classes, ainsi qu'une fonction de décodage des données API.

