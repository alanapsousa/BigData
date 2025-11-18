from pyspark import SparkContext


sc = SparkContext(master="local[*]", appName="TestRDD")

l = [1, 2, 3, 4, 5] * 1000000

rdd = sc.parallelize(l, numSlices=8)

#print('Partictions:',rdd.glom().collect())

result = rdd.map(lambda x: x * x).collect()
#print('l^2:',result)
print(sc.master)
rdd.collect()

sc.stop()