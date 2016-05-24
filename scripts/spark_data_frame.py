%history
from pyspark.sql.types import *
from pyspark.sql import Row
rdd = sc.textFile('data/realEstate.csv')
rdd = rdd.map(lambda line: line.split(","))
header = rdd.first()
rdd = rdd.filter(lambda line:line != header)
df = rdd.map(lambda line: Row(street = line[0], city = line[1], zip=line[2], beds=line[4], baths=line[5], sqft=line[6], price=line[9])).toDF()
import pandas
df.toPandas()
favorite_zip = df[df.zip == 95815]
favorite_zip.show(5)
favorite_zip.show()
df.select('city','beds').show(10)
df.groupBy("beds").count().show()
df.describe(['baths', 'beds','price','sqft']).show()
df.describe(['baths', 'beds','price','sqft']).show()
# regression with mllib
import pyspark.mllib
import pyspark.mllib.regression
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import *
df = df.select('price','baths','beds','sqft')
df = df[df.baths > 0]
df = df[df.beds > 0]
df = df[df.sqft > 0]
df.describe(['baths','beds','price','sqft']).show()
temp = df.map(lambda line:LabeledPoint(line[0],[line[1:]]))
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import StandardScaler
%history -f spark_data_frame.py
