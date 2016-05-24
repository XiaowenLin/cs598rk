metadata = sqlContext.read.json('/home/cs598rk/cs598rk/data/meta_musical.json')
metadata.printSchema()
metadata.groupBy('brand').count().show()
metadata.groupBy('brand').count().sort("count", ascending=False).show() #To get partial list
metadata.groupBy('brand').count().sort("count", ascending=False).collect() #To get data as an array


metadata.groupBy('categories').count().sort("count", ascending=False).show() #Doesnt work for categories as they are an array
#So we will use an explode function
df = metadata.select(explode(metadata.categories)) #Explode 1st level
df.collect() #Process all the data
newdf = df.select(explode(df._c0)).collect(); #Explode 2nd level
newdf.collect() #Process all the data
newdf.distinct().count()
