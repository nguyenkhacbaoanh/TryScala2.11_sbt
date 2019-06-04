package com.baoanh.LearnSPARK

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NOOAdata {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Try SQL Spark").master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val path: String = System.getProperty("user.dir")
    //    declare schema for DataFrame
    //    tschema for data2019
    val tschema = StructType(Array(
      StructField("sid", StringType),
      StructField("date", DateType),
      StructField("mtype", StringType),
      StructField("value", DoubleType)
    ))
    //    sschema for station data
    val sschema = StructType(Array(
      StructField("sid", StringType),
      StructField("lat", DoubleType),
      StructField("lon", DoubleType),
      StructField("name", StringType)
    ))
    //    read data2019 from csv
    val data2019 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv(path + "/data/2019.csv").cache()
    //    data2019.show()
    //    data2019.schema.printTreeString()

    //    query in DF to get tmax and tmin
    val tmax2019 = data2019.filter(data2019("mtype") === "TMAX").limit(1000000).drop("mtype").withColumnRenamed("value", "tmax")
    val tmin2019 = data2019.filter(data2019("mtype") === "TMIN").limit(1000000).drop("mtype").withColumnRenamed("value", "tmin")
    //    tmax2019.show()
    //    tmin2019.show()

    //    join DataFrame

    //    val joinMinMax2019 = tmax2019.join(tmin2019, tmax2019("sid") === tmin2019("sid") && tmax2019("date") === tmin2019("date"))
    val joinMinMax2019 = tmax2019.join(tmin2019, Seq("sid", "date"))
    //    joinMinMax2019.show()

    //    Calculate average Temperature by Select

    val averageTemp2019 = joinMinMax2019.select(joinMinMax2019("sid"), joinMinMax2019("date"), (joinMinMax2019("tmax") + joinMinMax2019("tmin")) / 2 as "tave")
    //    averageTemp2019.show()

    //    read stations data from csv
    /*
     * read by RDD to switch to DataFrame
     */

    val stationsRDD = spark.sparkContext.textFile(path + "/data/ghcnd-stations.txt").map { line =>
      val id = line.substring(0, 11)
      val lat = line.substring(12, 20).toDouble
      val lon = line.substring(21, 30).toDouble
      val name = line.substring(41, 71)
      Row(id, lat, lon, name)
    }
    // create DF from RDD and schema
    val stations = spark.createDataFrame(stationsRDD, sschema).cache()
    //    stations.show()

    // groupby SQL and aggregate SQL
    val avgTemp2019GroupBy = averageTemp2019.groupBy('sid).agg(avg('tave) as "tave")

    // join data
    val joinData2019 = avgTemp2019GroupBy.join(stations, "sid")
    //    joinData2019.show()

    //    val plot = Vegas("Country Pop").
    //      withData(
    //        Seq(
    //          Map("country" -> "USA", "population" -> 314),
    //          Map("country" -> "UK", "population" -> 64),
    //          Map("country" -> "DK", "population" -> 80)
    //        )
    //      ).
    //      encodeX("country", Nom).
    //      encodeY("population", Quant).
    //      mark(Bar)
    //
    //    plot.show
    spark.stop()

  }
}