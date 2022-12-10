# from sys import stdin

from pyspark import SparkContext

if __name__ == "__main__":
    # sc = SparkContext("local[*]", "wordcount")
    # sc.setLogLevel("WARN")
    # finalresult = sc.textFile("C:\\SreeLucky\\text.txt").\
    #   flatMap(lambda x: x.split(" ")).\
    #  countByValue()
    # .map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
    # for a in finalresult:
    #  print('(',a,',',finalresult[a],')')

    sc=SparkContext("local[*]", "bigdatacampaign")
    sc1=SparkContext("local[*]", "bigdatacampaignf")
    file = sc.textFile("C:/SreeLucky/Bigdata/content/Week10/dataset/bigdata.csv")
    file1 = sc1.textFile("C:/SreeLucky/Bigdata/content/Week10/dataset/boringwords.txt")
    x = (9, 1.12364546554645)
    y = 1.233435456
    print(':'.join(map(str, x)))
