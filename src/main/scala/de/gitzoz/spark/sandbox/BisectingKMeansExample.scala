/*
 * Copyright 2016 Stefan Roehrbein
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.gitzoz.spark.sandbox

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.linalg.Vector
import java.io.File
import org.apache.spark.ml.clustering.KMeans

object BisectingKMeansExample {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder.appName("BisectingKMeansExample").getOrCreate()

    val dataset = spark.read.format("libsvm").load("sample_kmeans_data.txt")
    constructDataForElbowMethod(dataset, spark)
    ouputPredictionsForLabels(3, dataset, spark)
  }

  def constructDataForElbowMethod(dataset: DataFrame, spark: SparkSession) = {
    val result =
      (2 to 10).foldLeft(Map[Int, (Double, Array[Vector])]()) { (acc, idx) =>
        val bkm   = new BisectingKMeans().setK(idx).setSeed(1)
        val model = bkm.fit(dataset)

        val cost = model.computeCost(dataset)
        println(s"Within Set Sum of Squared Errors = $cost")

        val centers = model.clusterCenters

        acc ++ Map(idx -> (cost, centers))
      }
    writeCostCsv(result)
  }

  def ouputPredictionsForLabels(clusters: Int,
                                dataset: DataFrame,
                                spark: SparkSession) = {
    val bkm   = new BisectingKMeans().setK(clusters).setSeed(1)
    val model = bkm.fit(dataset)

    val predictions = model.transform(dataset)
    writePredictionCsv(predictions)
  }

  def writeCostCsv(modelResult: Map[Int, (Double, Array[Vector])]) = {
    val WSSSEString = modelResult.toSeq.sortBy(_._1).map {
      case (idx, (wssse, _)) =>
        s"$idx;$wssse\n".replace(".", ",")
    }
    val stringBuilder = new StringBuilder()
    WSSSEString.foreach { stringBuilder.append(_) }
    writeFile(stringBuilder.toString, "wssse.csv")
  }
  def writePredictionCsv(predictions: DataFrame) = {
    val predictionStrings =
      predictions.select("label", "prediction").sort("label").collect.map {
        row =>
          val label      = row.getAs[Double]("label")
          val prediction = row.getAs[Int]("prediction")
          s"$label;$prediction\n".replace(".", ",")
      }
    val stringBuilder = new StringBuilder()
    predictionStrings.foreach { stringBuilder.append(_) }
    writeFile(stringBuilder.toString, "predictions.csv")
  }

  def writeFile(content: String, filename: String) = {
    import java.nio.file.{ Paths, Files }
    import java.nio.charset.StandardCharsets
    Files
      .write(Paths.get(s"$filename"), content.getBytes(StandardCharsets.UTF_8))
  }
}
