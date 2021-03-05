package com.spark.cos

import com.ibm.ibmos2spark.CloudObjectStorage
import org.apache.spark.sql.SparkSession

object SparkIBM extends App {

  val spark = SparkSession
    .builder()
    .appName("Connect IBM COS")
    .master("local")
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
  spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
  spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")

  var credentials = scala.collection.mutable.HashMap[String, String](
    "endPoint" -> "s3.us.cloud-object-storage.appdomain.cloud",
    "accessKey" -> "0aba66146f3b450cacebaa908046d17e",
    "secretKey" -> "27b804de3b329a680dbf148fd76da208f33e8a5aaaea4cbd")
  var bucketName = "candidate-exercise"
  var objectname = "emp-data.csv"

  var configurationName = "softlayer_cos"

  var cos = new CloudObjectStorage(spark.sparkContext, credentials, configurationName)

  var df = spark.
    read.format("csv").
    option("header", "true").
    option("inferSchema", "true").
    load(cos.url(bucketName, objectname))

  df.printSchema()
  df.show()

  df.registerTempTable("emp")

  val ratio_df = spark.sql(""" select DEPTNO, 
    sum(case when gender = 'MALE' then 1 else 0 end)/count(*) male_ratio, 
    sum(case when gender = 'FEMALE' then 1 else 0 end)/count(*) fem_ratio 
    from emp group by DEPTNO order by count(*) desc """)

  val avg_df = spark.sql(" select deptno, avg(sal) average_salary from emp group by deptno ")

  val salary_gap_df = spark.sql(""" select DEPTNO, 
    round(avg(case when gender='MALE' then sal end),0) avg_m_salary, 
    round(avg(case when gender='FEMALE' then sal end),0) avg_f_salary, 
    (round(avg(case when gender='MALE' then sal end),0) - round(avg(case when gender='FEMALE' then sal end),0) ) diff_in_avg 
    from emp group by DEPTNO order by DEPTNO """)

  ratio_df.coalesce(1).write.format("parquet").save(cos.url(bucketName, "ratio.parquet"))
  avg_df.coalesce(1).write.format("parquet").save(cos.url(bucketName, "avg.parquet"))
  salary_gap_df.coalesce(1).write.format("parquet").save(cos.url(bucketName, "salary_gap_df.parquet"))

}