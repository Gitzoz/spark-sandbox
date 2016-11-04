package de.gitzoz.spark.sandbox

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DecisionTreeClassificationExample {
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.
    val data = spark.read.format("libsvm").load("spot_nur_halbstreuwinkel_libsvm.txt")
    
  }
  
  
}