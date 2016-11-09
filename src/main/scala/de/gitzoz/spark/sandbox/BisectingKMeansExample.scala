package de.gitzoz.spark.sandbox

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.linalg.Vector

object BisectingKMeansExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BisectingKMeansExample")
      .getOrCreate()

    val dataset = spark.read.format("libsvm").load("sample_kmeans_data.txt")

    val result = (1 to 10).foldLeft(Map[Int, (Double, Array[Vector])]()) { (acc, idx) =>
      val bkm = new BisectingKMeans().setK(idx).setSeed(1)
      val model = bkm.fit(dataset)

      val cost = model.computeCost(dataset)
      println(s"Within Set Sum of Squared Errors = $cost")

      val centers = model.clusterCenters

      acc ++ Map(idx -> (cost, centers))
    }

    result.foreach {
      case (idx, (cost, centers)) =>
        println(s"For $idx Clusters, sum of Squared Errors = $cost")
        centers.foreach(println)
    }
  }
}