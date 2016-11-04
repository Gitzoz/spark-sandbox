lazy val `spark-sandbox` =
  project.in(file(".")).enablePlugins(AutomateHeaderPlugin, GitVersioning)

libraryDependencies ++= Vector(
  Library.scalaTest % "test",
  Library.spark,
  Library.sparkSql,
  Library.sparkMlLib
)

initialCommands := """|import de.gitzoz.spark.sandbox._
                      |""".stripMargin
