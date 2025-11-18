import pandas as pd
from pyspark import SparkContext

sc= SparkContext('local[*]', 'arbres_paris')
df = pd.read_csv("data/arbresremarquablesparis.csv", sep=";")
df.head()

rdd = sc.parallelize(df.to_dict(orient="records"))

# Display the first 5 rows of the RDD
rdd.take(5)

#oldest tree in paris
oldest = (
    rdd.map(lambda x: (x["Année de plantation"], x["espèce"]))
       .filter(lambda x: x[0] != "") 
       .sortByKey()
       .first()
)
print("Oldest tree in Paris:", oldest)


#highest tree in paris
highest = (
    rdd.map(lambda x: (x['hauteur en m'], x['espèce']))
         .filter(lambda x: x[0] != "")
         .sortByKey()
         .first()
)

print("Highest tree in Paris:", highest)

#tree with largest circumference in paris
circumference = rdd.map(lambda x: (float(x['circonference en cm']) if x['circonference en cm'] != '' else 0, x['espèce'])).sortByKey(ascending=False).first()
print("Tree with largest circumference in Paris:", circumference)

#height mean of trees in paris
height_mean = rdd.map(lambda x: float(x['hauteur en m']) if x['hauteur en m'] != '' else 0).mean()
print("Mean height of trees in Paris:", height_mean)

#number of trees by area in paris
trees_by_area = (
    rdd.map(lambda x: (x["Arrondissement"], 1))
       .reduceByKey(lambda a, b: a + b)
       .sortByKey()
)

print("Number of trees by area in Paris:", trees_by_area.collect())

# number of trees in pere-lachaise cemetery
# Cimetière du Père Lachaise = Paris 20e arrondissement


n_pere_lachaise_trees = rdd.filter(lambda x: x["Site"] == "Cimetière du Père Lachaise").count()


print("Number of trees at Cimetière du Père Lachaise :", n_pere_lachaise_trees)


rdd.collect()

sc.stop()