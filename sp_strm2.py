from pyspark import  SparkContext
from pyspark.streaming import StreamingContext

sc=SparkContext("local[2]","stateful")
sc.setLogLevel("ERROR")
ssc=StreamingContext(sc,10)
ssc.checkpoint(".")
lines=ssc.socketTextStream("localhost",5687)

def updateFunc(new_values,previous_state):
    if previous_state is None:
        previous_state=0
    return sum(new_values,previous_state)

words_rdd=lines.flatMap(lambda x:x.split())
filter_dstream=words_rdd.filter(lambda x:x.startswith("big"))
map_rdd=filter_dstream.map(lambda x:(x,1))
final_rdd=map_rdd.updateStateByKey(updateFunc)
final_rdd.pprint()

ssc.start()
ssc.awaitTermination()