package com.cgomezfl.testSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext


import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._


object SimpleSparkSqlExample {
  
   def main(args: Array[String]) = {
    val conf = new SparkConf()
       .setAppName("SimpleSparkSqlExample")
       .setMaster("local[2]")
       //.set("spark.executor., value)
     
    val sc = new SparkContext(conf)
    
    val sqlc = new org.apache.spark.sql.SQLContext(sc)
    
    val p = sc.textFile("src/test/resources/Person")
    val pmap = p.map { p => p.split(",") }    
    val PersonRdd = pmap.map { p => Person(p(0),p(1),p(2).toInt) }
    val path1 = args
   
    val file1 = sc.textFile(path1.apply(0))
    
    file1.collect()
    
    
    file1.flatMap { line => line.split(" ") }
    .map { word => (word,1) }
    .reduceByKey(_ + _)
    .saveAsTextFile(path1.apply(1))
    
  }
}