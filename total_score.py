from pyspark import SparkContext
from sys import stdin
if __name__=='__main__':
    def parse_to_scores(percentage):
        if percentage>=0.9:
            return 10
        elif 0.5 <= percentage < 0.9:
            return 4
        elif percentage>=0.25 and percentage<0.5:
            return 2
        else:
            return 0
    sc=SparkContext("local[*]","total_score")
    sc.setLogLevel("ERROR")
    titlesRDD = sc.textFile("C:/SreeLucky/Bigdata/content/Week10/assignment/titles.csv",1) \
        .map(lambda x: (int(x.split(",")[0]), x.split(",")[1]))

    #calculating total chapters per course
    rdd111 = sc.textFile("C:/SreeLucky/Bigdata/content/Week10/assignment/chapters.csv",1) \
    .map(lambda x:(int(x.split(",")[0]), int(x.split(",")[1])))
    rdd211 = rdd111.map(lambda x: (x[1], 1))
    chapterCountRDD = rdd211.reduceByKey(lambda x, y: x + y)

    #calculating total score
    rdd1=sc.textFile("C:/SreeLucky/Bigdata/content/Week10/assignment/views1.csv",1)
    rdd2 = sc.textFile("C:/SreeLucky/Bigdata/content/Week10/assignment/views2.csv",1)
    rdd3 = sc.textFile("C:/SreeLucky/Bigdata/content/Week10/assignment/views3.csv",1)
    raw_input_rdd=rdd1 + rdd2 + rdd3
    chapteridUseridRDD=raw_input_rdd.map(lambda x:(int(x.split(",")[1]),int(x.split(",")[0])))
    distinctChapteridUseridRDD=chapteridUseridRDD.distinct()
    viewsRDD=distinctChapteridUseridRDD.join(rdd111)
    totalviewsRDD=viewsRDD.map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y)
    courseViewsRDD=totalviewsRDD.map(lambda x:(x[0][1],x[1]))
    totalCourseViewsRDD=courseViewsRDD.join(chapterCountRDD)
    viewsPercentageRDD=totalCourseViewsRDD.mapValues(lambda x:x[0]/x[1])
    formattedPercentageRDD=viewsPercentageRDD.mapValues(lambda x:float("{0:.3f}".format(x)))
    courseScoreRDD=formattedPercentageRDD.mapValues(lambda x:parse_to_scores(x))
    totalCourseScoreRDD=courseScoreRDD.reduceByKey(lambda x,y:x+y)
    finaltitlesRDD=totalCourseScoreRDD.join(titlesRDD).map(lambda x:(x[1][0],x[1][1])).sortByKey(False)
    finalFormattedRDD=finaltitlesRDD.map(lambda x:":".join(map(str,x)))
    finalFormattedRDD.saveAsTextFile("C:/SreeLucky/Bigdata/content/Week10/assignment/output/best_course.csv")

    #result=finaltitlesRDD.collect()
    #for a in result:
     # print(str(a[1]),',',a[0])
    stdin.readline()