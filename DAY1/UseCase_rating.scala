// Databricks notebook source
// Databricks notebook source
//File location path ->  /FileStore/tables/ratings-2.csv

//creating rdd from external dataset

val data= sc.textFile("/FileStore/tables/ratings-2.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

val ratingsData = data.map(x => x.split(",")(2))

ratingsData.collect()

// COMMAND ----------

ratingsData.count()

// COMMAND ----------

ratingsData.countByValue()

// COMMAND ----------


