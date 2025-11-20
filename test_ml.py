from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from sklearn.datasets import load_diabetes

#from sklearn.preprocessing import StandardScaler
import numpy as np

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# init spark
sc = SparkContext("spark://51.91.85.87:7077", "regression_rdd")
sc = SparkContext.getOrCreate()

# dataset Boston Housing
data = load_diabetes()
X = data.data
y = data.target

# normalizar features
#scaler = StandardScaler()
#X = scaler.fit_transform(X)

# criar RDD de LabeledPoint
rdd = sc.parallelize([
    LabeledPoint(float(y[i]), X[i].tolist())
    for i in range(len(y))
])

# dividir treino/teste
train_rdd, test_rdd = rdd.randomSplit([0.8, 0.2], seed=42)

# trainng model
model = LinearRegressionWithSGD.train(
    train_rdd,
    iterations=200,
    step=0.01
)

# predictions on test set
predictions = test_rdd.map(lambda p: (p.label, model.predict(p.features)))

# MSE
mse = predictions.map(lambda x: (x[0] - x[1])**2).mean()

print("Fisrt 10 previsions (label, prediction):")
print(predictions.take(10))

print("\nMSE:", mse)

sc.stop()

