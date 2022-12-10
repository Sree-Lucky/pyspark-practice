from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc=SparkContext("local[2]","stateful")
ssc=StreamingContext(sc,10)
lines=ssc.socketTextStream("localhost",5687)
ssc.checkpoint(".")

sum_dstream=lines.reduceByWindow(lambda x,y:int(x)+int(y),lambda x,y:int(y),20,10)
count_dstream=lines.countByWindow(20,10)

sum_dstream.pprint()
count_dstream.pprint()

ssc.start()
ssc.awaitTermination()
