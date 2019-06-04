package com.baoanh.LearnSPARK
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait Context {
    // set configuration
    lazy val conf = new SparkConf()
          .setAppName("First App")
          .setMaster("local[*]")
    //  create a spark session
    lazy val sparkSession = SparkSession
          .builder()
          .config(conf)
          .getOrCreate()
    lazy val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
}