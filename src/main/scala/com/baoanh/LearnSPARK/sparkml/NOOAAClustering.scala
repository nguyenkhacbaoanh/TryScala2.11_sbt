package com.baoanh.LearnSPARK.sparkml

import com.baoanh.LearnSPARK.Context
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import scalafx.application.JFXApp
import swiftvis2.plotting._
import swiftvis2.plotting
import swiftvis2.plotting.renderer.FXRenderer

case class Station(id: String, lat: Double, lon: Double, elev: String, name: String)

object NOOAAClustering extends JFXApp with Context{
  import sparkSession.implicits._

  sparkSession.sparkContext.setLogLevel("WARN")

  val stations = sparkSession.read.textFile("data/ghcnd-stations.txt").map { line =>
    val id = line.substring(0,11)
    val lat = line.substring(12,20).toDouble
    val lon = line.substring(21,30).toDouble
    val elev = line.substring(31,37)
    val name = line.substring(41,71)
    Station(id, lat, lon, elev, name)
  }.cache()

  /*
  in this scripts, we try to use KMean algorithm in Spark MLlib
   */

  // in KMean: it require data like a Vector, so base on station dataset, we need to transform to Vector

  val stationsVA = new VectorAssembler().setInputCols(Array("lat", "lon")).setOutputCol("location")
  val stationsWithLoc = stationsVA.transform(stations)
//  stationsWithLoc.show()

  // declare KMean model
  val kMeans = new KMeans().setK(2000).setFeaturesCol("location")
  val stationClusterModel = kMeans.fit(stationsWithLoc)

  val stationWithClusters = stationClusterModel.transform(stationsWithLoc)
//  stationWithClusters.show()

  val x = stationWithClusters.select('lon).as[Double].collect()
  val y = stationWithClusters.select('lat).as[Double].collect()
  val predict = stationWithClusters.select('prediction).as[Double].collect()
  val cg = ColorGradient(0.0 -> BlueARGB, 1000.0 -> RedARGB, 2000.0 -> GreenARGB)
  val plot = Plot.scatterPlot(x,y,"Stations", "Longitude", "Latitude", 3.toDouble, predict.map(cg))

  FXRenderer(plot, 1000, 650)
}






