#Importer les packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import leaflet
import folium

#Instancier le client spark
spark=SparkSession.builder\
                .master("local[*]")\
                .appName("brisbane_city")\
                .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Fichier de configuration pour recuper les paths ( il faut créer un fichier properties.conf avant en dur)
import configparser
config = configparser.ConfigParser()
config.read('properties.conf')
path_to_input_data= config['Brisbane-City-bike']['Input_data']
path_to_output_data= config['Brisbane-City-bike']['Output_data']
num_partition_kmeans = config.getint('Brisbane-City-bike','Kmeans_level')

#Importation du fichier json
brisbane=spark.read.json(path=path_to_input_data)

#On affchiche les 3 premières lignes 
print("********************Les 3 premières lignes du dataset sont*******************************")
brisbane.show(3)


#Schema des données
print("******* Le schema est *****************")
brisbane.printSchema()


#Dimensions du Dataframe
print("******************************** Les dimensions sont",brisbane.count(),len(brisbane.columns))

#Création de la base Kmeans_df
kmeans_df=brisbane.select(col("longitude"),col("latitude"))


print("*************************Les 5 premières lignes **********************")
kmeans_df.show(5)

#Algorithme de Kmeans (3 clusters crées)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
features = ("longitude","latitude")
kmeans = KMeans().setK(num_partition_kmeans).setSeed(1)
assembler = VectorAssembler(inputCols=features,outputCol="features")
dataset=assembler.transform(kmeans_df)
model = kmeans.fit(dataset)
fitted = model.transform(dataset)

#On vérifie les colonnes
fitted.columns

#Calcule de la longitude et de la latitude moyenne par cluster (DSL)
print("******************************* la longitude et la latitude moyenne est********************************")
fitted.groupby(col("prediction"))\
    .agg(mean(col("longitude")).alias("Moyenne_longitude"),mean(col("latitude")).alias("Moyenne_latitude"))\
    .orderBy(col("prediction"))\
    .show()
print("***************************************************************************************************************")
#Calcule de la longitude et de la latitude moyenne par cluster (DSL)
fitted.createOrReplaceTempView("fitted_sql")
spark.sql(""" select prediction,avg(longitude) as Moyenne_logitude, avg(latitude) as Moyenne_latitude
                    from fitted_sql 
                     group by prediction order by prediction""").show()

#Set des coordonnées géographiques de Brisbane
latitude_brisbane=-27.4710107
longitude_brisbane=153.0234489
brisbane_map = folium.Map(location = [latitude_brisbane, longitude_brisbane], zoom_start = 12.6)

#on recupère la logitude, la lattitude, les clusters et les noms dans des listes distinctes
lat=list(fitted.select(col('latitude')).toPandas()['latitude'])
long=list(fitted.select(col('longitude')).toPandas()['longitude'])
pred=list(fitted.select(col('prediction')).toPandas()['prediction'])
name=list(brisbane.select(col('name')).toPandas()['name'])

#Ajout des markers sur chaque points
liste=[]
for i in range(0,len(lat)):
    if pred[i]==0:
        col="blue"
    elif pred[i]==1:
                col="red"
    else:
        col="darkgreen"
    folium.Marker([lat[i],long[i]],popup=name[i],icon=folium.Icon(color=col)).add_to(brisbane_map)
brisbane_map

#on exporte la map 
brisbane_map.save('Map_brisbane.html')

#Suppression de la colonne "features"
fitted_export=fitted.drop("features")

#On exporte la base au format csv
fitted_export.toPandas().to_csv(path_to_output_data+"clusters_brisbane.csv")
