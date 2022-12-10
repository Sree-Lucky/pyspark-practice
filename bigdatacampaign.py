from pyspark import SparkContext

if __name__=='__main__':
    def parseValues(line):
        arr=line.split(",")
        sentence=arr[0]
        amount = float(arr[10])
        return (sentence,amount)
    def loadBoringWords():
        boring_words=set(line.strip() for line in open("C:/SreeLucky/Bigdata/content/Week10/dataset/boringwords.txt"))
        return boring_words
    sc=SparkContext("local[*]", "bigdatacampaign")
    sc.setLogLevel("WARN")
    file=sc.textFile("C:/SreeLucky/Bigdata/content/Week10/dataset/bigdata.csv")
    broadcast_var=sc.broadcast(loadBoringWords())
    rdd1=file.map(parseValues)
    rdd2=rdd1.map(lambda x:(x[1],x[0]))
    rdd3=rdd2.flatMapValues(lambda x:x.split(" "))
    rdd4 = rdd3.map(lambda x: (x[1].lower(), x[0]))
    rdd5=rdd4.reduceByKey(lambda x,y:x+y)
    rdd6 = rdd5.sortBy(lambda x:x[1],False)
    final_rdd=rdd6.filter(lambda x:x[0] not in broadcast_var.value)
    result = final_rdd.take(10)
   # result=final_rdd.foreach(print)

    for a in result:
        print(a)
    #print(broadcast_var.value)