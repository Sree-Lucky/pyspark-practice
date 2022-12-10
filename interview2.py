from pyspark.sql import SparkSession
import logging

from pyspark.sql.functions import *
from pyspark.sql.types import *


def main():
    log=logging.getLogger('org')
    log.setLevel("WARN")
    spark=SparkSession.builder.appName("interview2").master("local[*]").getOrCreate()
    log.warning("hello {0}".format(spark.sparkContext.getConf().get("spark.app.name")))



    schema=StructType([StructField("article_id",IntegerType(),False),
                        StructField("author_id",IntegerType(),False),
                        StructField("viewer_id", IntegerType(), False),
                        StructField("view_date", DateType(), False)
                       ])
    schema2 = "article_id:int, author_id:int, viewer_id:int, view_date:date"
    #above not working please check
    views_df=spark.read.format("csv").\
        schema(schema).\
        load("C:/SreeLucky/data/interview2.csv")
    views_df.show()
    views_df.groupby("viewer_id","view_date").\
        agg(countDistinct("article_id").alias("cnt")).\
        filter(col("cnt")>1).select("viewer_id").show()





if __name__=='__main__':
    main()


