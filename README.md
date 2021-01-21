# Brisbane_city_bike
L'objectif de ce projet est de proposer un clustering (kmeans) de brisbane-city-bike en utilisant l'emplacement des vélos.
# Instructions:
:arrow_forward: Cloner le dépôt git sur votre ordinateur avec la l'instruction "git clone https://github.com/ibsagno95/Brisbane_city_bike"  
Le dépôt contient 4 repertoires et un fichier properties.conf.  
1. **Data:** Contient le fichier de donnée Brisbane-city-bike.json  
2. **script:** Contient un notebook et script python **script_brisbane.py**  
3. **exported:** Contient la base de données exportée contenant la longitude, la lattitude et les clusters
4. **output:** Contient un une capture de la carte obtenue  
Et enfin un fichier **properties.conf** contenant les *paths*.

:arrow_forward: Se placer dans le repertoire cloné et ouvrir une console et taper  l'instruction:  
"spark-submit run.py"

Les réponses attendues appaîtront progressivement. Voici un exemple de sorties.
## Une entête du data set 
![](https://github.com/ibsagno95/Brisbane_city_bike/blob/main/output/ent%C3%AAte%20du%20dataset.png)  

## Latitude et longitude moyenne par cluster
![](https://github.com/ibsagno95/Brisbane_city_bike/blob/main/output/Longitude%20et%20latitude%20moyenne%20par%20cluster.png)  

## La carte des clusters sur Brisbane
![](https://github.com/ibsagno95/Brisbane_city_bike/blob/main/output/Map_brisbane.png)  

**NB:** La carte n'apparaîttra pas dans la console mais le code l'exportera au format **html** dans votre repertoire courant.
