from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc=SparkContext()
sc.setLogLevel("ERROR")
ssc=StreamingContext(sc,10)
lines=ssc.socketTextStream("localhost",5687)
words=lines.flatMap(lambda x:x.split(" "))
words_map=words.map(lambda x:(x,1))
finalrdd=words_map.reduceByKey(lambda x,y:x+y)
finalrdd.pprint()

ssc.start()
ssc.awaitTermination()
