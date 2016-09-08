package com.cgomezfl.testSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object JoinSpark {
   def main(args: Array[String]) = {
    val conf = new SparkConf()
       .setAppName("WordCount")
       .setMaster("local")
     
    val sc = new SparkContext(conf)
    
    val path1 = args
   
    val file1 = sc.textFile(path1.apply(0))
    
    file1.collect()
    
    
    file1.flatMap { line => line.split(" ") }
    .map { word => (word,1) }
    .reduceByKey(_ + _)
    .saveAsTextFile(path1.apply(1))
    
  }
}