from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.window import Window
from pyspark.sql.functions import *

if __name__=='__main__':
    myconfig=SparkConf()
    myconfig.set("spark.app.name","No_one_batsman")
    myconfig.set("spark.master","local[*]")

    spark=SparkSession.builder.config(conf=myconfig).getOrCreate()
    file_A_DF=spark.read \
        .format("csv") \
        .option("header",True) \
        .option("delimiter"," ") \
        .option("inferSchema",True) \
        .option("path","C:/SreeLucky/Bigdata/content/Week12/assignment/filea.csv").load()
    file_B_DF = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("path", "C:/SreeLucky/Bigdata/content/Week12/assignment/fileb.csv").load()
    joinDF=file_A_DF.join(file_B_DF,file_A_DF.Batsman==file_B_DF.Batsman,"inner")
    formDF=joinDF.drop(file_A_DF.Batsman).drop(file_A_DF.Team)
    """mywin=Window.partitionBy("Batsman").rowsBetween(Window.unboundedPreceding,Window.currentRow)
    formDF.withColumn("avg", avg("RunsScored").over(mywin)).show()"""
    result=formDF.groupby("Batsman")\
        .agg(
             avg("RunsScored").alias("avg") \
            ,sum("RunsScored").alias("totalRuns") \
            ) \
        .orderBy("avg",ascending=False)
    #result.show(1)
    result.write\
        .partitionBy("Batsman","avg")\
        .mode(saveMode="overWrite")\
        .csv("C:/SreeLucky/Bigdata/content/Week12/assignment/output")