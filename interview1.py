from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("testing").getOrCreate()

def main():
    businessDF=spark.read.\
        format('csv')\
        .option('path',"C:/SreeLucky/interview1.txt")\
        .option('header',True)\
        .option('inferSchema',True)\
        .load()
    print(spark.conf.get("spark.app.name"))
    #businessDF.withColumn("avg_occq",avg("occurence").over(Window.partitionBy("event_type"))).show()
    avgdf=businessDF.groupby("event_type")\
        .agg(avg("occurence").alias("avg_occ"))
    result=businessDF.join(broadcast(avgdf),avgdf["event_type"]==businessDF["event_type"],"inner")\
        .filter(businessDF["occurence"]>avgdf["avg_occ"])\
        .withColumn("valid",count(businessDF.event_type).over(Window.partitionBy("business_id")))\
        .filter(col("valid")>1).select("business_id").dropDuplicates()
    result.show()
if __name__== '__main__':

    main()
    spark.stop()