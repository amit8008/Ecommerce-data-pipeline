ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Stream_data_pipelines",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0" % "provided"
    ),
//    assembly / assemblyJarName := "spark-streamingdata-pipeline-1.0.jar",
  )
