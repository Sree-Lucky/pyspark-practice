from pyspark import SparkContext
import sys


if __name__ == '__main__':
    sc =SparkContext("local[*]","count_chapters")
    sc.setLogLevel("ERROR")
    rdd1=sc.textFile("C:/SreeLucky/Bigdata/content/Week10/assignment/chapters.csv")
    rdd2=rdd1.map(lambda x:(int(x.split(",")[1]),1))
    rdd3=rdd2.reduceByKey(lambda x,y:x+y)
    rdd4=rdd3.sortBy(lambda x:x[0],True)
    print(rdd4.collect())
    sys.stdin.readline()