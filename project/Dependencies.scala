import sbt._

object Version {
  final val Scala     = "2.11.8"
  final val ScalaTest = "3.0.0"
  final val Spark = "2.0.2"
  
}

object Library {
  val scalaTest = "org.scalatest" %% "scalatest" % Version.ScalaTest
  val spark = "org.apache.spark" %% "spark-core" % Version.Spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % Version.Spark
  val sparkMlLib = "org.apache.spark" %% "spark-mllib" % Version.Spark
}
