

from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging

if __name__=='__main__':
    myconfig=SparkConf()
    myconfig.set("spark.master","local[*]")
    myconfig.set("spark.app.name", "employee_count")
    spark=SparkSession.builder.config(conf=myconfig).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log=logging.getLogger()
    log.error(myconfig.get("spark.app.name").upper()+" Application started")

    emp_df=spark.read.format("json") \
        .option("path","C:/SreeLucky/Bigdata/content/Week12/assignment/employee.json") \
        .load()
    dept_df=spark.read.format("json") \
        .option("path","C:/SreeLucky/Bigdata/content/Week12/assignment/dept.json") \
        .load()

    joinDF=emp_df.join(dept_df,emp_df.deptid==dept_df.deptid,"inner").drop(emp_df.deptid)
    joinDF.groupby("deptName","deptid")\
        .count().withColumnRenamed("count","empcount").show()
