ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Stream_data_pipelines",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0" % "provided",
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.9.1",
      "org.postgresql" % "postgresql" % "42.2.23" % "provided",
      "com.typesafe" % "config" % "1.4.2"
    ),
    assembly / assemblyJarName := "spark-streamingdata-pipeline-1.0.jar",
  )
