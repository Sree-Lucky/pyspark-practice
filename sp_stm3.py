from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc= SparkContext("local[2]","sliding")
sc.setLogLevel("ERROR")
ssc=StreamingContext(sc,10)
ssc.checkpoint(".")
lines=ssc.socketTextStream("localhost",5687)

words_dstream=lines.flatMap(lambda x:x.split())
map_dstream=words_dstream.map(lambda x:(x,1))
final_dstream=map_dstream.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,20,10)
final_dstream.pprint()

ssc.start()
ssc.awaitTermination()
