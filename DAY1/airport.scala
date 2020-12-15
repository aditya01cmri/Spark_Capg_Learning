// Databricks notebook source
val airportrdd=sc.textFile("/FileStore/tables/airports.text")

val USairport=airportrdd.filter(line => line.split(",")(3) == "\"United States\"")

USairport.collect()

def splitInput(line:String) = {
  
  val datasplit=line.split(",")
  
  val airportId = datasplit(1)
  val cityName = datasplit(2)
  
  (airportId,cityName)
}

USairport.map( splitInput ).take(3)

USairport.map(line => {
  val splitData= line.split(",")
  splitData(1)+" "+splitData(2) }).collect()

val filterCountry= airportrdd.filter(x=>(x.split(",")(7)).toDouble>40 || x.split(",")(3)=="\"Iceland\"")

filterCountry.take(20)

filterCountry.saveAsTextFile("CountryIceland.csv")

val pacific= airportrdd.filter(x=>(x.split(",")(8)).toDouble%2==0).map(x=>x.split(",")(11))

pacific.countByValue

// COMMAND ----------


